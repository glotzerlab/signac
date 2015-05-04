import logging
logger = logging.getLogger('compdb.job_queue')

KEY_CALLABLE_NAME = 'name'
KEY_CALLABLE_MODULE = 'module'
KEY_CALLABLE_SOURCE_HASH = 'source_hash'
KEY_CALLABLE_MODULE_HASH = 'module_hash'

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
    #import jsonpickle
    import pickle
    import hashlib
    checksum_src = hash_source(fn)
    binary = pickle.dumps(
        {'fn': fn, 'args': args, 'kwargs': kwargs,
         'module': fn.__module__,
         'src': checksum_src})
    checksum = hashlib.sha1()
    checksum.update(binary)
    return {'callable': binary, 'checksum': checksum.hexdigest()}

def decode_callable(doc):
    #import jsonpickle
    import pickle
    import hashlib
    binary = doc['callable']
    checksum = doc['checksum']
    m = hashlib.sha1()
    m.update(binary)
    if not checksum == m.hexdigest():
        raise RuntimeWarning("Checksum deviation! Possible security violation!")
    #c_doc = jsonpickle.decode(binary.decode())
    c_doc = pickle.loads(binary)
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
        result_collection.update_one({'_id': _id}, {'$set': {KEY_RESULT_ERROR: encode(error_doc)}}, upsert = True)
    else:
        logger.info("Finished exection of job with id={}.".format(_id))
        result_collection.update_one({'_id': _id}, {'$set': {KEY_RESULT_RESULT: encode(result)}}, upsert = True)

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
    
    def submit(self, fn, *args, **kwargs):
        item = encode_callable(fn, args, kwargs)
        _id = self._job_queue.put(item)
        self._result_collection.insert_one({'_id': _id})
        return Future(self, _id)

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
        from threading import Thread, Event
        import queue
        result_queue = queue.Queue()
        error_queue = queue.Queue()
        result = None
        stop_event = Event()
        def try_to_get():
            from math import tanh
            from itertools import count
            w = (tanh(0.05 * i) for i in count())
            spec = {'_id': _id}
            while(not stop_event.is_set()):
                result = self._result_collection.find_one(spec)
                if result is not None:
                    if KEY_RESULT_RESULT in result:
                        result_queue.put(decode(result[KEY_RESULT_RESULT]))
                        break
                    elif KEY_RESULT_ERROR in result:
                        error_queue.put(decode(result[KEY_RESULT_ERROR]))
                        break
                    else:
                        pass # no results yet
                stop_event.wait(max(0.001, next(w)))
        t_get = Thread(target = try_to_get)
        t_get.start()
        t_get.join(timeout = timeout)
        if t_get.is_alive():
            stop_event.set()
            t_get.join()
        error = None
        try:
            return result_queue.get_nowait()
        except queue.Empty:
            try:
                error_doc = error_queue.get_nowait()
            except queue.Empty:
                pass
            else:
                print(error_doc['traceback'])
                raise error_doc['error']
        raise TimeoutError()

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
