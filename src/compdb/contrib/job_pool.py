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

    def __init__(self, project, parameter_set, include = None, exclude = None):
        self._instances[id(self)] = self
        self._project = project
        self._parameter_set = parameter_set
        self._include_condition = include
        self._exclude_condition = exclude
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

    def start(self, rank = None, size = None, limit = None, blocking = True, timeout = -1):
        if rank is None:
            rank = self._comm().Get_rank()
        if size is None:
            size = self._comm().Get_size()
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
                for i, index in enumerate(indeces[rank]):
                    if limit is not None:
                        if i >= limit:
                            break
                    msg = "Rank {}: Parameter index: {}."
                    logger.debug(msg.format(rank, index))
                    job(self._project.open_job( 
                        parameters = self._parameter_set[index],
                        blocking = blocking, timeout = timeout))
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
        if self._include_condition is None and self._exclude_condition is None:
            yield from range(len(self._parameter_set))
        else:
            job_ids = [self._project.open_job(p).get_id()
                for p in self._parameter_set]
            if self._include_condition is not None:
                included = set(self._check_condition(
                    self._include_condition, job_ids))
            else:
                included = set(job_ids)
            if self._exclude_condition is not None:
                excluded = set(self._check_condition(
                    self._exclude_condition, job_ids))
            else:
                excluded = set()
            print(job_ids, included, excluded)

            for index, job_id in enumerate(job_ids):
                if job_id in included and not job_id in excluded:
                    yield index

    def _check_condition(self, condition, ids):
        cond_docs = self._project.find(spec = condition)
        cond_ids = set(doc['_id'] for doc in cond_docs)
        for id_ in ids:
            if id_ in cond_ids:
                yield id_


    def __len__(self):
        return len(list(self._calculate_indeces()))

    def _setup(self, rank, size):
        rank = self._comm().Get_rank()
        matrix = None
        if rank == MPI_ROOT:
            indeces = list(self._calculate_indeces())
            matrix = [indeces[i:j] for i,j in decompose(len(indeces), size)]
            logger.info("Broadcasting domain decomposition.")
            logger.debug(matrix)
        logger.debug("Reached barrier.")
        self._comm().Barrier()
        return self._comm().bcast(matrix, root = MPI_ROOT)

def decompose(num_jobs, num_ranks):
    assert num_ranks >= 1
    num_jobs_per_rank = num_jobs // num_ranks
    rest = num_jobs % num_ranks
    i = 0
    for rank in range(num_ranks):
        j = i + num_jobs_per_rank
        if rank < rest:
            j += 1 
        yield i, j
        i = j
