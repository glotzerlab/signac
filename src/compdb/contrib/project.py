import logging
logger = logging.getLogger('project')

JOB_PARAMETERS_KEY = 'parameters'
JOB_NAME_KEY = 'name'
JOB_DOCS = 'compdb_job_docs'

def valid_name(name):
    return not name.startswith('_compdb')

class Project(object):
    
    def __init__(self, config = None):
        if config is None:
            from compdb.core.config import load_config
            config = load_config()
        self._config = config

    @property 
    def config(self):
        return self._config

    def root_directory(self):
        return self._config['project_dir']

    def _get_db(self, db_name):
        from pymongo import MongoClient
        import pymongo.errors
        host = self.config['database_host']
        try:
            client = MongoClient(host)
            return client[db_name]
        except pymongo.errors.ConnectionFailure as error:
            from . errors import ConnectionFailure
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
    
    @property
    def collection(self):
        return self.get_project_db()[JOB_DOCS]

    def filestorage_dir(self):
        return self.config['filestorage_dir']

    def remove(self):
        from pymongo import MongoClient
        import pymongo.errors
        self.get_cache().clear()
        for job in self.find_jobs():
            job.remove()
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

    def develop_mode(self):
        return bool(self.config.get('develop', False))

    def activate_develop_mode(self):
        self.config['develop'] = True

    def _job_spec(self, name, parameters):
        spec = {}
        if name is not None:
            spec.update({JOB_NAME_KEY: name})
        if parameters is not None:
            spec.update({JOB_PARAMETERS_KEY: parameters})
        if self.develop_mode():
            spec.update({'develop': True})
        spec.update({
            'project': self.get_id(),
        })
        return spec

    def _open_job(self, spec, blocking = True, timeout = -1):
        from . job import Job
        return Job(
            project = self,
            spec = spec,
            blocking = blocking,
            timeout = timeout)

    def open_job(self, name, parameters = None, blocking = True, timeout = -1):
        spec = self._job_spec(name = name, parameters = parameters)
        return self._open_job(spec, blocking, timeout)

    def _find_jobs(self, job_spec, * args, **kwargs):
        from copy import copy
        job_spec_ = copy(job_spec)
        if 'project' in job_spec_:
            raise ValueError("You cannot provide a value for 'project' using this search method.")
        job_spec_.update({'project': self.get_id()})
        if self.develop_mode():
            job_spec_.update({'develop': True})
        yield from self.get_jobs_collection().find(
            job_spec_, * args, ** kwargs)

    def find_job_ids(self, spec):
        for job in self._find_jobs(spec, fields = ['_id']):
            yield job['_id']
    
    def find_jobs(self, job_spec = {}, spec = None, blocking = True, timeout = -1):
        job_ids = list(self.find_job_ids(job_spec))
        if spec is not None:
            spec.update({'_id': {'$in': job_ids}})
            docs = self.collection.find(spec)
            job_ids = (doc['_id'] for doc in docs)
        for _id in job_ids:
            yield self._open_job({'_id': _id}, blocking, timeout)
    
    def find(self, job_spec = {}, spec = {}, * args, ** kwargs):
        job_ids = self.find_job_ids(job_spec)
        spec.update({'_id': {'$in': list(job_ids)}})
        yield from self.collection.find(spec, * args, ** kwargs)

    def clear_develop(self, force = True):
        spec = {'develop': True}
        job_ids = self.find_job_ids(spec)
        for develop_job in self.find_jobs(spec):
            develop_job.remove(force = force)
        self.collection.remove({'id': {'$in': list(job_ids)}})

    def active_jobs(self):
        spec = {'$where': 'this.executing.length > 0'}
        yield from self.find_job_ids(spec)

    def _unique_jobs_from_heartbeat(self):
        docs = self.get_jobs_collection().find(
            {'heartbeat': {'$exists': True}},
            ['heartbeat'])
        beats = [doc['heartbeat'] for doc in docs]
        for beat in beats:
            for uid, timestamp in beat.items():
                yield uid

    def job_pulse(self):
        uids = self._unique_jobs_from_heartbeat()
        for uid in uids:
            hb_key = 'heartbeat.{}'.format(uid)
            doc = self.get_jobs_collection().find_one(
                {hb_key: {'$exists': True}})
            yield uid, doc['heartbeat'][uid]

    def kill_dead_jobs(self, seconds = 10):
        import datetime
        from datetime import datetime, timedelta
        cut_off = datetime.utcnow() - timedelta(seconds = seconds)
        uids = self._unique_jobs_from_heartbeat()
        for uid in uids:
            hbkey = 'heartbeat.{}'.format(uid)
            doc = self.get_jobs_collection().update(
                #{'heartbeat.{}'.format(uid): {'$exists': True},
                {hbkey: {'$lt': cut_off}},
                {   '$pull': {'executing': uid},
                    '$unset': {hbkey: ''},
                })
