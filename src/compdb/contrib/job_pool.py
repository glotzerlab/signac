import logging
logger = logging.getLogger('compdb.job_pool')

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

    def _broadcast(self, rank, data):
        self._comm().Barrier()
        if rank == MPI_ROOT:
            return self._comm().bcast(data, root = MPI_ROOT)
        else:
            return self._comm().bcast(None, root = MPI_ROOT)

    def submit(self, c):
        self._job_queue.put(c)

    def _write_jobfile(self, filename, matrix):
        with open(filename, 'xb') as file:
            file.write("{}\n".format(len(self)).encode())
            for jobs in matrix:
                file.write("{}\n".format(','.join(jobs)).encode())

    def write_jobfile(self, filename, size = None):
        if size is None:
            size = self._comm().Get_size()
        matrix = self._get_matrix(size)
        logger.debug("Job matrix: {}".format(matrix))
        self._write_jobfile(filename, matrix)

    def _read_jobfile(self, filename, rank):
        with open(filename, 'rb') as file:
            try:
                num_jobs = int(file.readline().decode())
            except ValueError:
                return []
            if num_jobs < len(self):
                msg = "Jobfile '{}' seems outdated! "
                msg += "# of jobs in pool: {}, # of jobs in file: {}."
                logger.warning(msg.format(filename, len(self), num_jobs))
            for i in range(rank): # Skip the lines with lower rank
                file.readline()
            line = file.readline().decode()
            ids = line.split(',')
            if ids == ['\n']:
                return []
            else:
                return [id_.strip() for id_ in ids]

    def start(self, rank = None, size = None, limit = None,
              blocking = True, timeout = -1, jobfile = None):
        if rank is None:
            rank = self._comm().Get_rank()
        if size is None:
            size = self._comm().Get_size()
        if rank >= size:
            msg = "Illegal rank {}, size is {}."
            raise ValueError(msg.format(rank, size))
        msg = "Starting pool with rank '{}'."
        logger.info(msg.format(rank))
        if jobfile is None:
            job_ids = self._broadcast_matrix(rank, size)
        else:
            try:
                self.write_jobfile(jobfile, size)
            except FileExistsError:
                pass
            job_ids = self._read_jobfile(jobfile, rank)
        while not self._job_queue.empty():
            job = self._job_queue.get()
            try:
                msg = "Executing job '{}' with rank {}."
                logger.debug(msg.format(job, rank))
                for i, job_id in enumerate(job_ids):
                    if limit is not None:
                        if i >= limit:
                            break
                    if self._exclude_condition is not None:
                        if job_id in self._check_condition(
                                self._exclude_condition, job_ids):
                            msg = "Skipping '{}' due to exclude condition."
                            logger.debug(msg.format(job_id))
                            continue
                    job(self._project.get_job(
                        job_id = job_id,
                        blocking = blocking, timeout = timeout))
            except:
                logger.debug("Job '{}' failed.".format(job))
                self._job_queue_failed.put(job)
                raise
            else:
                logger.debug("Job '{}' done.".format(job))
                self._job_queue_done.put(job)
            finally:
                self._job_queue.task_done()

    def reset_queue(self):
        from itertools import chain
        for queue in chain((self._job_queue_done, self._job_queue_failed)):
            while not queue.empty():
                self._job_queue.put(queue.get())

    def _get_valid_ids(self):
        job_ids = [self._project.open_job(p).get_id()
            for p in self._parameter_set]
        if self._include_condition is None and self._exclude_condition is None:
            yield from job_ids
        else:
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

            for index, job_id in enumerate(job_ids):
                if job_id in included and not job_id in excluded:
                    yield job_id

    def _check_condition(self, condition, ids):
        cond_docs = self._project.find(spec = condition)
        cond_ids = set(doc['_id'] for doc in cond_docs)
        for id_ in ids:
            if id_ in cond_ids:
                yield id_

    def __len__(self):
        return len(list(self._get_valid_ids()))

    def _get_matrix(self, size):
        ids = list(self._get_valid_ids())
        matrix = [ids[i:j] for i,j in decompose(len(ids), size)]
        return matrix

    def _broadcast_matrix(self, rank, size):
        matrix = None
        if rank == MPI_ROOT:
            matrix = self._get_matrix(size)
            logger.info("Broadcasting domain decomposition.")
            logger.debug(matrix)
        logger.debug("Reached barrier.")
        return self._broadcast(rank, matrix)[rank]

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
