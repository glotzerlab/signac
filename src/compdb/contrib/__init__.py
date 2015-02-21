def get_project():
    from . job import Project
    return Project()

def open_job(name, parameters = None, blocking = True, timeout = -1):
    from . job import Job, job_spec
    project = get_project()
    spec = job_spec(name = name, parameters = parameters)
    return Job(
        project = project,
        spec = spec,
        blocking = blocking,
        timeout = timeout)

#def reopen_job(job_id):
#    from .job import Job, get_jobs_collection
#    spec = get_jobs_collection().find_one({'_id': job_id})
#    assert spec is not None
#    return Job(spec)

def find_job_docs(name = None, parameters = None):
    from .job import job_spec
    project = get_project()
    yield from project.get_jobs_collection().find(
        job_spec(name = name, parameters = parameters))

def find_jobs(name = None, parameters = None):
    from .job import Job
    jobs = find_job_docs(name = name, parameters = parameters)
    for job in jobs:
        yield reopen_job(job_id = job['_id'])

def sleep_random(time = 1.0):
    from random import uniform
    from time import sleep
    sleep(uniform(0, time))
