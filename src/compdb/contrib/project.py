import logging
logger = logging.getLogger('project')

JOB_PARAMETERS_KEY = 'parameters'
JOB_NAME_KEY = 'name'

def valid_name(name):
    return not name.startswith('_compdb')

def job_spec(name, parameters):
    spec = {}
    if name is not None:
        spec.update({JOB_NAME_KEY: name})
    if parameters is not None:
        spec.update({JOB_PARAMETERS_KEY: parameters})
    return spec

class Project(object):
    
    def __init__(self, config = None):
        if config is None:
            from compdb.core.config import read_config
            config = read_config()
        self._config = config

        if self.debug_mode():
            logger.warning("Project '{}' opened in DEBUG mode.".format(
            self.config['project']))

    @property 
    def config(self):
        return self._config

    def _get_db(self, db_name):
        from pymongo import MongoClient
        import pymongo.errors
        host = self.config['database_host']
        try:
            client = MongoClient(host)
            return client[db_name]
        except pymongo.errors.ConnectionFailure as error:
            msg = "Failed to connect to database '{}' at '{}'."
            #logger.error(msg.format(db_name, host))
            raise ConnectionFailure(msg.format(db_name, host)) from error
    def get_db(self, db_name):
        assert valid_name(db_name)
        return self._get_db(db_name)

    def _get_meta_db(self):
        return self._get_db(self.config['database_meta'])

    def get_jobs_collection(self):
        return self._get_meta_db()['compdb_jobs']

    def get_id(self):
        return self.config['project']

    def get_project_db(self):
        return self.get_db(self.get_id())

    def filestorage_dir(self):
        return self.config['filestorage_dir']

    def remove(self):
        from pymongo import MongoClient
        import pymongo.errors
        self.get_cache().clear()
        try:
            host = self.config['database_host']
            client = MongoClient(host)
            client.drop_database(self.get_id())
        except pymongo.errors.ConnectionFailure as error:
            msg = "{}: Failed to remove project database on '{}'."
            raise ConnectionFailure(msg.format(self.get_id(), host)) from error

    def lock_job(self, job_id, blocking = True, timeout = -1):
        from . concurrency import DocumentLock
        return DocumentLock(
            self.get_jobs_collection(), job_id,
            blocking = blocking, timeout = timeout)

    def get_milestones(self, job_id):
        from . milestones import Milestones
        return Milestones(self, job_id)

    def get_cache(self):
        from . cache import Cache
        return Cache(self)

    def debug_mode(self):
        return bool(self.config.get('debug', False))

    def _find_job_docs(self, spec):
        yield from self.get_jobs_collection().find(spec)
    
    def _find_jobs(self, spec, blocking = True, timeout = -1):
        for doc in self._find_job_docs(spec):
            yield self._open_job({'_id': doc['_id']}, blocking, timeout)

    def _open_job(self, spec, blocking = True, timeout = -1):
        from . job import Job
        return Job(
            project = self,
            spec = spec,
            blocking = blocking,
            timeout = timeout)

    def find_jobs(self, name = None, parameters = None, blocking = True, timeout = -1):
        for doc in self.find_job_docs(name, parameters):
            yield self._open_job({'_id': doc['_id']}, blocking, timeout)

    def open_job(self, name, parameters = None, blocking = True, timeout = -1):
        if self.debug_mode():
            parameters.update({'_debug': True})
        spec = job_spec(name = name, parameters = parameters)
        return self._open_job(spec, blocking, timeout)

    def find_job_docs(self, name = None, parameters = None):
        spec = job_spec(name = name, parameters = parameters)
        yield from self._find_job_docs(spec)

    def clear_debug(self, force = True):
        spec = {'_debug': {'$exists': True}}
        debug_jobs = self._find_jobs(spec)
        for debug_job in debug_jobs:
            debug_job.remove(force = force)
