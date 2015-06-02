from . import job_pool

def import_module(module_path):
    import inspect, sys, os
    module_info = inspect.getmoduleinfo(module_path)
    head, tail = os.path.split(module_path)
    sys.path.append(head)
    return __import__(module_info.name)

def get_num_jobs(module_path):
    import_module(module_path)
    return job_pool.count_jobs()

def find_all_pools(module_path):
    import_module(module_path)
    yield from job_pool.all_pools()
    
def submit_mpi(pool):
    try:
        from mpi4py import MPI
    except ImportError:
        from .. import raise_no_mpi4py_error
        raise_no_mpi4py_error()
    else:
        comm = MPI.COMM_WORLD
        pool.start(comm.Get_rank(), comm.Get_size())
