def get_project():
    from . project import Project
    open_job.project = None
    return Project()

def open_job(parameters = None, blocking = True, timeout = -1, rank = 0):
    if open_job.project is None:
        open_job.project = get_project()
    return open_job.project.open_job(
        parameters = parameters,
        blocking = blocking, timeout = timeout,
        rank = rank)
open_job.project = None

def find_jobs(job_spec = {}, spec = None):
    project = get_project()
    yield from project.find_jobs(job_spec, spec)

def find(job_spec = {}, spec = {}):
    project = get_project()
    yield from project.find(job_spec, spec)

def sleep_random(time = 1.0):
    from random import uniform
    from time import sleep
    sleep(uniform(0, time))
