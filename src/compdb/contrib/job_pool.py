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

    def get_id(self):
        from . hashing import generate_hash_from_spec
        pool_spec = {
            'project_id': self._project.get_id(),
            'set':  self._parameter_set}
        return generate_hash_from_spec(pool_spec)

    def _comm(self):
        from mpi4py import MPI
        return MPI.COMM_WORLD

    def submit(self, c):
        self._job_queue.put(c)

    def start(self, rank, size):
        msg = "Starting pool with rank '{}'."
        logger.info(msg.format(rank))
        indeces = self._setup(rank, size)
        if rank >= len(indeces):
            return
        while not self._job_queue.empty():
            job = self._job_queue.get()
            try:
                msg = "Executing job '{}' with rank {}."
                logger.debug(msg.format(job, rank))
                for i in indeces[rank]:
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

    def _setup(self, rank, size):
        matrix = None
        if rank == MPI_ROOT:
            indeces = list(self._calculate_indeces())
            cs = len(indeces) // size
            rest = len(indeces) % size
            matrix = [indeces[:cs+rest]]
            for node in range(1,size):
                matrix.append(indeces[rest+node*cs:rest+(node+1)*cs])
        return self._comm().bcast(matrix, root = MPI_ROOT)

    def _open_job(self, index):
        return self._project.open_job(
            self._parameter_set[index])
