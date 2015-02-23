def get_project():
    from . project import Project
    return Project()

def open_job(name, parameters = None, blocking = True, timeout = -1):
    project = get_project()
    return project.open_job(
        name = name, parameters = parameters,
        blocking = blocking, timeout = timeout)

def find_job_docs(name = None, parameters = None):
    project = get_project()
    yield from project.find_job_docs(
        name = name, parameters = parameters)

def find_jobs(name = None, parameters = None):
    from .job import Job
    jobs = find_job_docs(name = name, parameters = parameters)
    for job in jobs:
        yield reopen_job(job_id = job['_id'])

def sleep_random(time = 1.0):
    from random import uniform
    from time import sleep
    sleep(uniform(0, time))
