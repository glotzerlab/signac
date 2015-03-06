from . import job_pool

def import_module(module_path):
    import inspect, sys, os
    module_info = inspect.getmoduleinfo(module_path)
    sys.path.append(os.getcwd())
    return __import__(module_info.name)

def get_num_jobs(module_path):
    import_module(module_path)
    return job_pool.count_jobs()

def start_pool(module_path, rank):
    import_module(module_path)
    for pool in job_pool.all_pools():
        with pool:
            pool.start(rank)

def submit_serial(module_path):
    import_module(module_path)
    for pool in job_pool.all_pools():
        ranks = range(len(pool))
        with pool:
            for rank in ranks:
                pool.start(rank)
                pool.reset_queue()

def submit_mpi(module_path):
    import_module(module_path)
    n = get_num_jobs(module_path)
    for pool in job_pool.all_pools():
        with pool:
            pool.start()
