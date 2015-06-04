import logging
logger = logging.getLogger(__name__)

MPI_ROOT = 0
STOP_ITEM = "STOP".encode()

KEY_ITEM = 'item'
KEY_RESULT_RESULT = 'result'
KEY_RESULT_ERROR = 'error'

from . serialization import encode, decode, encode_callable, decode_callable, KEY_CALLABLE_CHECKSUM

class Future(object):

    def __init__(self, executor, _id):
        self._executor = executor
        self._id = _id

    def result(self, timeout = None):
        return self._executor._get_result(self._id, timeout)

def execute_callable(job_queue, result_collection, item, reload):
    _id = item['_id']
    fn, args, kwargs = decode_callable(item, reload = reload)
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

def execution_worker(stop_event, job_queue, result_collection, timeout, comm = None, reload = True):
    while(not stop_event.is_set()):
        item = job_queue.get(
            block = True,
            timeout = timeout,
            stop_event = stop_event)
        if comm is not None:
            comm.bcast(item, root = MPI_ROOT)
        execute_callable(job_queue, result_collection, item, reload = reload)

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

    @property
    def stop_event(self):
        return self._stop_event

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

    def enter_loop(self, timeout = None, reload = True):
        self._stop_event.clear()
        logger.info("Entering execution loop, timeout={}.".format(timeout))
        execution_worker(self._stop_event, self._job_queue, self._result_collection, timeout, reload = reload)

    def enter_loop_mpi(self, timeout = None, reload = True):
        try:
            from mpi4py import MPI
        except ImportError:
            from .. import raise_no_mpi4py_error
            raise_no_mpi4py_error()
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        self._stop_event.clear()
        if rank == MPI_ROOT:
            logger.info("Entering execution loop, rank={}, timeout={}.".format(rank, timeout))
            try:
                execution_worker(self._stop_event, self._job_queue, self._result_collection, timeout, comm, reload = reload)
            except (KeyboardInterrupt, SystemExit):
                logger.warning("Execution interrupted.")
                comm.bcast(STOP_ITEM, root = MPI_ROOT)
                raise
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
                    execute_callable(self._job_queue, self._result_collection, item, reload = reload)
    
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
