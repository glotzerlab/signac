import logging
logger = logging.getLogger('compdb.job_queue')

KEY_CALLABLE_NAME = 'name'
KEY_CALLABLE_MODULE = 'module'
KEY_CALLABLE_SOURCE_HASH = 'source_hash'
KEY_CALLABLE_MODULE_HASH = 'module_hash'
KEY_CALLABLE_CHECKSUM = 'checksum'

KEY_ITEM = 'item'
KEY_RESULT_RESULT = 'result'
KEY_RESULT_ERROR = 'error'

MPI_ROOT = 0
STOP_ITEM = "STOP".encode()

def hash_module(c):
    import inspect, hashlib
    module = inspect.getmodule(c)
    src_file = inspect.getsourcefile(module)
    m = hashlib.md5()
    with open(src_file, 'rb') as file:
        m.update(file.read())
    return m.hexdigest()

def hash_source(c):
    import inspect, hashlib
    m = hashlib.md5()
    m.update(inspect.getsource(c).encode())
    return m.hexdigest()

def callable_name(c):
    try:
        return c.__name__
    except AttributeError:
        return c.name()

def callable_spec(c):
    import inspect
    assert callable(c)
    try:
        spec = {
            KEY_CALLABLE_NAME: callable_name(c),
            KEY_CALLABLE_SOURCE_HASH: hash_source(type(c)),
        }
    except TypeError:
        spec = {
            KEY_CALLABLE_NAME: callable_name(c),
            KEY_CALLABLE_MODULE: c.__module__,
            KEY_CALLABLE_MODULE_HASH: hash_module(c),
        }
    return spec

def encode_callable(fn, args, kwargs):
    import jsonpickle as pickle
    #import pickle
    import hashlib
    checksum_src = hash_source(fn)
    binary = pickle.dumps(
        {'fn': fn, 'args': args, 'kwargs': kwargs,
         'module': fn.__module__,
         'src': checksum_src}).encode()
    checksum = hashlib.sha1()
    checksum.update(binary)
    return {'callable': binary, KEY_CALLABLE_CHECKSUM: checksum.hexdigest()}

def decode_callable(doc):
    import jsonpickle as pickle
    #import pickle
    import hashlib
    binary = doc['callable']
    checksum = doc[KEY_CALLABLE_CHECKSUM]
    m = hashlib.sha1()
    m.update(binary)
    if not checksum == m.hexdigest():
        raise RuntimeWarning("Checksum deviation! Possible security violation!")
    #c_doc = jsonpickle.decode(binary.decode())
    c_doc = pickle.loads(binary.decode())
    fn = c_doc['fn']
    #if fn is None:
    #    msg = "Failed to unpickle '{}'. Possible version conflict."
    #    raise ValueError(msg.format(doc))
    if not hash_source(c_doc['fn']) == c_doc['src']:
        raise RuntimeWarning("Source checksum deviation! Possible version conflict.")
    return c_doc['fn'], c_doc['args'], c_doc['kwargs']

def encode(item):
    import jsonpickle
    return jsonpickle.encode(item).encode()

def decode(binary):
    import jsonpickle
    return jsonpickle.decode(binary.decode())

class Future(object):

    def __init__(self, executor, _id):
        self._executor = executor
        self._id = _id

    def result(self, timeout = None):
        return self._executor._get_result(self._id, timeout)

def execute_callable(job_queue, result_collection, item):
    _id = item['_id']
    fn, args, kwargs = decode_callable(item)
    logger.info("Executing job '{}({},{})' (id={})...".format(fn, args, kwargs, _id))
    try:
        result = fn(*args, ** kwargs)
    except Exception as error:
        import traceback
        import sys
        exc = traceback.format_exc()
        logger.warning("Execution of job with id={} aborted with error: {}\n{}".format(_id, error, exc))
        error_doc = {'error': error, 'traceback': exc}
        result_collection.update_one(
            {'_id': _id},
            {'$set': {
                KEY_ITEM: item,
                KEY_RESULT_ERROR: encode(error_doc),
                }},
            upsert = True)
    else:
        logger.info("Finished exection of job with id={}.".format(_id))
        result_collection.update_one(
            {'_id': _id},
            {'$set': {
                KEY_ITEM: item,
                KEY_RESULT_RESULT: encode(result),
                }},
            upsert = True)

def execution_worker(stop_event, job_queue, result_collection, timeout, comm = None):
    while(not stop_event.is_set()):
        item = job_queue.get(block = True, timeout = timeout)
        if comm is not None:
            comm.bcast(item, root = MPI_ROOT)
        execute_callable(job_queue, result_collection, item)

class MongoDBExecutor(object):

    def __init__(self, job_queue, result_collection):
        from multiprocessing import Event, Process
        self._job_queue = job_queue
        self._result_collection = result_collection
        self._stop_event = Event()

    def _put(self, item):
        _id = self._job_queue.put(item)
        self._result_collection.insert_one({'_id': _id})
        return Future(self, _id)

    def submit(self, fn, *args, **kwargs):
        item = encode_callable(fn, args, kwargs)
        queued = lambda: item in self._job_queue
        in_results = lambda: self._fetch_result(item) is not None
        if queued() or in_results():
            msg = "Item '{}' already submitted."
            raise ValueError(msg.format(item))
        return self._put(item)

    def resubmit(self, fn, * args, ** kwargs):
        item = encode_callable(fn, args, kwargs)
        return self._put(item)

    def enter_loop(self, timeout = None):
        self._stop_event.clear()
        logger.info("Entering execution loop, timeout={}.".format(timeout))
        execution_worker(self._stop_event, self._job_queue, self._result_collection, timeout)

    def enter_loop_mpi(self, timeout = None):
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        self._stop_event.clear()
        if rank == MPI_ROOT:
            logger.info("Entering execution loop, rank={}, timeout={}.".format(rank, timeout))
            try:
                execution_worker(self._stop_event, self._job_queue, self._result_collection, timeout, comm)
            except:
                comm.bcast(STOP_ITEM, root = MPI_ROOT)
                raise
            else:
                comm.bcast(STOP_ITEM, root = MPI_ROOT)
        else:
            logger.info("Entering execution loop, rank={}.".format(rank))
            while(True):
                item = comm.bcast(None, root = MPI_ROOT)
                if item == STOP_ITEM:
                    break
                else:
                    execute_callable(self._job_queue, self._result_collection, item)
    
    def stop(self):
        self._stop_event.set()

    def _get_result(self, _id, timeout):
        from . utility import mongodb_fetch_find_one
        spec = {
            '_id': _id,
            '$or': [
                {KEY_RESULT_RESULT: {'$exists': True}},
                {KEY_RESULT_ERROR: {'$exists': True}}]}
        result = mongodb_fetch_find_one(self._result_collection, spec, timeout = timeout)
        if KEY_RESULT_RESULT in result:
            return decode(result[KEY_RESULT_RESULT])
        elif KEY_RESULT_ERROR in result:
            raise decode(result[KEY_RESULT_ERROR])['error']
        else:
            assert False

    def _fetch_result(self, item, block = False, timeout = None):
        spec = {'{}.{}'.format(KEY_ITEM, KEY_CALLABLE_CHECKSUM): item[KEY_CALLABLE_CHECKSUM]}
        if block:
            from . utility import mongodb_fetch_find_one
            return mongodb_fetch_find_one(self._result_collection, spec, timeout = timeout)
        else:
            return self._result_collection.find_one(spec)

    def clear_completed(self):
        self._job_queue.clear() 
        self._result_collection.delete_many(
            {KEY_RESULT_RESULT: {'$exists': True}})

    def clear_aborted(self):
        self._result_collection.delete_many(
            {KEY_RESULT_ERROR: {'$exists': True}})

    def clear_results(self):
        self._result_collection.delete_many({})

    def clear_queue(self):
        self._job_queue.clear()

    def get_queued(self):
        for q in self._job_queue.peek():
            yield decode_callable(q)

    def num_queued(self):
        return self._job_queue.qsize()

    def _get_completed(self):
        return self._result_collection.find({KEY_RESULT_RESULT: {'$exists': True}})

    def num_completed(self):
        return self._get_completed().count()

    def get_completed(self):
        for doc in self._get_completed():
            yield decode(doc[KEY_RESULT_RESULT])

    def _get_aborted(self):
        return self._result_collection.find({KEY_RESULT_ERROR: {'$exists': True}})

    def num_aborted(self):
        return self._get_aborted().count()

    def get_aborted(self):
        for doc in self._get_aborted():
            yield decode(doc[KEY_RESULT_ERROR])
