def get_project():
    from . project import Project
    open_job.project = None
    project = Project()
    project.get_id()
    return project

def open_job(parameters = None, blocking = True, timeout = -1, rank = 0):
    raise PendingDeprecationWarning()
    project = get_project()
    return project.open_job(
        parameters = parameters,
        blocking = blocking, timeout = timeout,
        rank = rank)

def find_jobs(job_spec = {}, spec = None):
    raise PendingDeprecationWarning()
    project = get_project()
    yield from project.find_jobs(job_spec, spec)

def find(job_spec = {}, spec = {}):
    raise PendingDeprecationWarning()
    project = get_project()
    yield from project.find(job_spec, spec)

def sleep_random(time = 1.0):
    from random import uniform
    from time import sleep
    sleep(uniform(0, time))
