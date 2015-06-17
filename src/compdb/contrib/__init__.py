def get_project(project_path = None):
    from . project import Project
    open_job.project = None
    if project_path is not None:
        import os
        cwd = os.getcwd()
        os.chdir(project_path)
        project = Project()
        os.chdir(cwd)
    else:
        project = Project()
    project.get_id()
    return project

def get_basic_project_from_id(project_id, client = None):
    from .project import BasicProject
    from ..core.config import load_config
    config = load_config()
    config['project'] = project_id
    return BasicProject(config=config, client=client)

def get_all_project_ids(client = None):
    from .project import Project
    from ..core.config import load_config
    from ..core.dbclient_connector import DBClientConnector
    config = load_config()
    if client is None:
        connector = DBClientConnector(config, prefix = 'database_')
        connector.connect()
        connector.authenticate()
        client = connector.client
    for dbname in client.database_names():
        config['project'] = dbname
        project = Project(config)
        try:
            next(project.find_job_ids())
        except StopIteration:
            continue
        else:
            yield dbname

def get_all_active_jobs():
    for project_id in get_all_project_ids():
        project = get_basic_project_from_id(project_id)
        yield from project.active_jobs()

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
