def open_job(name, parameters = None):
    from . job import Job
    return Job(name = name, parameters = parameters)
