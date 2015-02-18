def open_job(name, parameters = None):
    from . job import Job, job_spec, get_jobs_collection
    spec = job_spec(name = name, parameters = parameters)
    previous = get_jobs_collection().find_one(spec)
    if previous is None:
        return Job(spec = spec)
    else:
        return reopen_job(previous['_id'])

def reopen_job(job_id):
    from .job import Job
    return Job(spec = {'_id': job_id})

def find_job_docs(name, parameters = None):
    from .job import get_jobs_collection, job_spec
    jobs_collection = get_jobs_collection()
    yield from jobs_collection.find(
        job_spec(name = name, parameters = parameters))

def find_jobs(name, parameters = None):
    from .job import Job
    jobs = find_job_docs(name = name, parameters = parameters)
    for job in jobs:
        yield reopen_job(job_id = job['_id'])
