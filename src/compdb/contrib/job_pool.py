import logging
logger = logging.getLogger('job_pool')

from weakref import WeakValueDictionary
from queue import Queue

_executors = WeakValueDictionary()

MPI_ROOT = 0
TAG_INDECES = 0

def count_pools():
    return len(JobPool._instances)

def count_jobs():
    n = 0
    for id_, pool in JobPool._instances.items():
        n += len(pool)
    return n

def all_pools():
    yield from JobPool._instances.values()

class JobPool(object):

    _instances = WeakValueDictionary()

    def __init__(self, project, parameter_set, exclude_condition = None):
        self._instances[id(self)] = self
        self._project = project
        self._parameter_set = parameter_set
        self._exclude_condition = exclude_condition
        self._job_queue = Queue()
        self._job_queue_done = Queue()
        self._job_queue_failed = Queue()
        self._indeces = None

    def get_id(self):
        from . hashing import generate_hash_from_spec
        pool_spec = {
            'project_id': self._project.get_id(),
            'set':  self._parameter_set}
        return generate_hash_from_spec(pool_spec)

    def _comm(self):
        from mpi4py import MPI
        return MPI.COMM_WORLD

    def _get_rank(self):
        return self._comm().Get_rank()

    def _get_size(self):
        return self._comm().Get_size()

    def submit(self, c):
        self._job_queue.put(c)

    def start(self):
        assert self._indeces is not None
        msg = "Starting pool with rank '{}'."
        logger.info(msg.format(self._get_rank()))
        if not len(self._indeces):
            return
        while not self._job_queue.empty():
            job = self._job_queue.get()
            try:
                msg = "Executing job '{}' with rank {}."
                logger.debug(msg.format(job, self._get_rank()))
                for i in self._indeces:
                    job(self._open_job(i))
            except:
                self._job_queue_failed.put(job)
                raise
            else:
                self._job_queue_done.put(job)
            finally:
                self._job_queue.task_done()

    def reset_queue(self):
        from itertools import chain
        for queue in chain((self._job_queue_done, self._job_queue_failed)):
            while not queue.empty():
                self._job_queue.put(queue.get())

    def _calculate_indeces(self):
        if self._exclude_condition is None:
            yield from range(len(self._parameter_set))
        else:
            docs = list(self._project.find(spec = self._exclude_condition))
            doc_ids = set(doc['_id'] for doc in docs)
            job_ids = (self._project.open_job(p).get_id() for p in self._parameter_set)
            for index, job_id in enumerate(job_ids):
                if job_id in doc_ids:
                    continue
                else:
                    yield index

    def __len__(self):
        return len(list(self._calculate_indeces()))

    def _setup(self):
        from mpi4py import MPI
        def serialize(indeces):
            return ','.join(str(i) for i in indeces).encode()
        def deserialize(data):
            try:
                return [int(i) for i in data.decode().split(',')]
            except ValueError:
                return []

        if self._get_rank() == MPI_ROOT:
            size = self._get_size()
            indeces = list(self._calculate_indeces())
            cs = len(indeces) // size
            rest = len(indeces) % size
            for node in range(1, size):
                self._comm().send(
                    serialize(indeces[rest+node*cs:rest+(node+1)*cs]),
                    dest = node, tag = TAG_INDECES)
            self._indeces = indeces[:cs+rest]
        else:
            self._indeces = deserialize(self._comm().recv(
                source = MPI_ROOT, tag = TAG_INDECES))

    def open(self):
        assert self._indeces is None
        self._setup()

    def close(self):
        self._indeces = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_val, traceback):
        self.close()

    def _get_index(self, rank):
        try:
            with open(self._fn_pool(), 'rb') as file:
                indeces = file.read().decode().split(',')
                try:
                    return int(indeces[rank])
                except (ValueError, IndexError) as error:
                    msg = "Invalid rank: {}"
                    logger.error(msg.format(rank))
                    raise IndexError(msg.format(rank)) from error
        except FileNotFoundError:
            msg = "Pool not opened."
            raise RuntimeError(msg)

    def parameters(self, rank):
        assert self._indeces is not None
        return self._parameter_set[self._indeces[rank]]

    def _open_job(self, index):
        assert self._indeces is not None
        return self._project.open_job(
            self._parameter_set[index])
