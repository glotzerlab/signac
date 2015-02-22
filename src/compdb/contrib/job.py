import logging
logger = logging.getLogger('job')

from compdb.core import get_db
from compdb.core.storage import Storage
from compdb.core.dbdocument import DBDocument

from . concurrency import DocumentLock

JOB_STATUS_KEY = 'status'
JOB_ERROR_KEY = 'error'
JOB_NAME_KEY = 'name'
JOB_PARAMETERS_KEY = 'parameters'
MILESTONE_KEY = '_milestones'

def valid_name(name):
    return not name.startswith('_compdb')

class DatabaseError(BaseException):
    pass

class ConnectionFailure(RuntimeError):
    pass

class Project(object):
    
    def __init__(self, config = None):
        if config is None:
            from compdb.core.config import read_config
            config = read_config()
        self._config = config

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
        try:
            host = self.config['database_host']
            client = MongoClient(host)
            client.drop_database(self.get_id())
        except pymongo.errors.ConnectionFailure as error:
            msg = "{}: Failed to remove project database on '{}'."
            raise ConnectionFailure(msg.format(self.get_id(), host)) from error

    def lock_job(self, job_id, blocking = True, timeout = -1):
        return DocumentLock(
            self.get_jobs_collection(), job_id,
            blocking = blocking, timeout = timeout)

    def get_milestones(self, job_id):
        return Milestones(self, job_id)

def job_spec(name, parameters):
    spec = {}
    if name is not None:
        spec.update({JOB_NAME_KEY: name})
    if parameters is not None:
        spec.update({JOB_PARAMETERS_KEY: parameters})
    return spec

def generate_hash_from_spec(spec):
    import json, hashlib
    blob = json.dumps(spec, sort_keys = True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()

class JobSection(object):

    def __init__(self, job, name):
        self._job = job
        self._name = name
        self._key = "_job_section_{}".format(name)

    def __enter__(self):
        return self

    def __exit__(self, err_type, err_val, traceback):
        if err_type:
            self._job.document[self._key] = False
            return False
        else:
            self._job.document[self._key] = True
            return True
    
    def completed(self):
        return self._job.document.get(self._key, False)

class Milestones(object):

    def __init__(self, project, job_id):
        self._project = project
        self._job_id = job_id

    def _spec(self):
        return {'_id': self._job_id}

    def _collection(self):
        return self._project.get_jobs_collection()

    def mark(self, name):
        result = self._collection().update(
            self._spec(),
            {'$addToSet': {MILESTONE_KEY: name}},
            upsert = True)
        assert result['ok']

    def remove(self, name):
        assert self._collection().update(
            self._spec(),
            {'$pull': {MILESTONE_KEY: name}})['ok']

    def reached(self, name):
        spec = self._spec()
        spec.update({
            MILESTONE_KEY: { '$in': [name]}})
        result = self._collection().find_one(
            spec,
            fields = [MILESTONE_KEY])
        logger.debug(result)
        return result is not None

    def clear(self):
        self._collection().update(
            self._spec(),
            {'$unset': {MILESTONE_KEY: ''}})

class JobNoIdError(RuntimeError):
    pass

class Job(object):
    
    def __init__(self, project, spec, blocking = True, timeout = -1):
        import uuid
        self._unique_id = uuid.uuid4()
        self._project = project
        self._spec = spec
        self._collection = None
        self._cwd = None
        self._wd = None
        self._fs = None
        self._obtain_id()
        self._with_id()
        self._lock = DocumentLock(
            self._project.get_jobs_collection(), self.get_id(),
            blocking = blocking, timeout = timeout)
        self._jobs_doc_collection = self._project.get_project_db()[str(self.get_id())]
        self._dbuserdoc = DBDocument(
            self._project.get_project_db()['compdb_job_docs'],
            self.get_id())
        self._cache = self._project.get_project_db()['compdb_cache'.format(self.get_id())]
        #self._dbcachedoc = DBDocument(
        #    get_project_db()['compdb_cache'],
        #    self.get_id())

    @property
    def spec(self):
        return self._spec

    def get_id(self):
        return self.spec.get('_id', None)

    def _with_id(self):
        if self.get_id() is None:
            raise JobNoIdError()
        assert self.get_id() is not None
    
    def _job_doc_spec(self):
        self._with_id()
        return {'_id': self._spec['_id']}

    def get_working_directory(self):
        self._with_id()
        return self._wd

    def get_filestorage_directory(self):
        self._with_id()
        return self._fs

    def _create_directories(self):
        import os
        self._with_id()
        for dir_name in (self.get_working_directory(), self.get_filestorage_directory()):
            if not os.path.isdir(dir_name):
                os.makedirs(dir_name)

    def _add_instance(self):
        self._project.get_jobs_collection().update(
            spec = self._job_doc_spec(),
            document = {'$push': {'executing': self._unique_id}})

    def _remove_instance(self):
        result = self._project.get_jobs_collection().find_and_modify(
            query = self._job_doc_spec(),
            update = {'$pull': {'executing': self._unique_id}},
            new = True)
        return len(result['executing'])

    def _open(self):
        import os
        self._with_id()
        self._cwd = os.getcwd()
        self._wd = os.path.join(self._project.config['working_dir'], str(self.get_id()))
        self._fs = os.path.join(self._project.filestorage_dir(), str(self.get_id()))
        self._create_directories()
        self._storage = Storage(
            fs_path = self._fs,
            wd_path = self._wd)
        os.chdir(self.get_working_directory())
        self._add_instance()
        msg = "Opened job with id: '{}'."
        logger.debug(msg.format(self.get_id()))

    def _close_with_error(self):
        import shutil, os
        self._with_id()
        os.chdir(self._cwd)
        self._cwd = None
        self._remove_instance()

    def _close(self):
        import shutil, os
        if self.num_open_instances() == 0:
            shutil.rmtree(self.get_working_directory())

    def open(self):
        with self._lock:
            self._open()

    def close(self):
        with self._lock:
            self._close()

    @property
    def storage(self):
        return self._storage

    def _obtain_id(self):
        import os
        from pymongo.errors import DuplicateKeyError
        from . import sleep_random
        if not '_id' in self._spec:
            try:
                _id = generate_hash_from_spec(self._spec)
            except TypeError:
                logger.error(self._spec)
                raise TypeError("Unable to hash specs.")
            self._spec.update({'_id': _id})
        num_attempts = 3
        for attempt in range(num_attempts):
            try:
                result = self._project.get_jobs_collection().update(
                    spec = self._spec,
                    document = {'$set': self._spec},
                    upsert = True)
                break
            except DuplicateKeyError as error:
                if attempt >= (num_attempts-1):
                    raise RuntimeError("Unable to open job after {} attempts. "
                     "Use `contrib.sleep_random` if you have trouble with "
                     "opening jobs in concurrency.".format(attempt+1)) from error
                else:
                    sleep_random(1)
        assert result['ok']
        if result['updatedExisting']:
            _id = self._project.get_jobs_collection().find_one(self._spec)['_id']
        else:
            _id = result['upserted']
        self._spec = self._project.get_jobs_collection().find_one({'_id': _id})
        assert self._spec is not None
        assert self.get_id() == _id

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_value, traceback):
        logger.debug("Exiting.")
        import os
        with self._lock:
            if err_type is None:
                self._close_with_error()
            else:
                err_doc = '{}:{}'.format(err_type, err_value)
                self._project.get_jobs_collection().update(
                    self.spec, {'$push': {JOB_ERROR_KEY: err_doc}})
                self._close_with_error()
                return False
    
    def clear_working_directory(self):
        import shutil
        try:
            shutil.rmtree(self.get_working_directory())
        except FileNotFoundError:
            pass
        self._create_directories()

    #def clear_filestorage_directory(self):
    #    import shutil
    #    try:
    #        shutil.rmtree(self.get_filestorage_directory())
    #    except FileNotFoundError:
    #        pass
    #    self._create_directories()

    def clear(self):
        self.clear_working_directory()
        self._storage.clear()
        self._dbuserdoc.clear()
        self._jobs_doc_collection.drop()

    def clear_cache(self):
        self._cache.drop()

    def remove(self, force = False):
        self._with_id()
        if not force:
            if not self.num_open_instances() == 0:
                msg = "You are trying to remove a job, which has {} open instances. Use 'force=True' to ignore this."
                raise RuntimeError(msg.format(self.num_open_instances()))
        self._remove()

    def _remove(self):
        import shutil
        self.clear()
        self._storage.remove()
        try:
            shutil.rmtree(self.get_working_directory())
        except FileNotFoundError:
            pass
        self._dbuserdoc.remove()
        self._project.get_jobs_collection().remove(self._job_doc_spec())
        del self.spec['_id']

    @property
    def collection(self):
        return self._jobs_doc_collection

    def _open_instances(self):
        self._with_id()
        job_doc = self._project.get_jobs_collection().find_one(self._job_doc_spec())
        if job_doc is None:
            return list()
        else:
            return job_doc.get('executing', list())

    def num_open_instances(self):
        return len(self._open_instances())

    def is_exclusive_instance(self):
        return self.num_open_instances <= 1

    def lock(self, blocking = True, timeout = -1):
        return self._project.lock_job(
            self.get_id(),
            blocking = blocking, timeout = timeout)

    @property
    def document(self):
        return self._dbuserdoc

    def storage_filename(self, filename):
        from os.path import join
        return join(self.get_filestorage_directory(), filename)

    def section(self, name):
        return JobSection(self, name)

    def _store_in_cache(self, spec, doc, data):
        import pickle
        try:
            logger.debug("Trying to cache results.")
            blob = pickle.dumps(data)
            #doc = dict(spec)
            doc['data'] = blob
            self._cache.update(spec, doc, upsert = True)
            rdoc = self._cache.find_one(spec)
            assert rdoc is not None
            assert rdoc['data'] == blob
            assert pickle.loads(rb) == data
        except InvalidDocument as error:
            logger.error("Failed to encode: {}".format(error))
        except AssertionError:
            logger.warning("Test retrieval did not pass equality test.")
            self._cache.remove(spec)
        finally:
            if self._cache.find_one(spec) is None:
                logger.debug("Caching failed.")
            else:
                logger.debug("Cached succesfully.")
            return data

    def cached(self, function, * args, ** kwargs):
        import inspect, pickle
        from bson.errors import InvalidDocument
        signature = str(inspect.signature(function))
        arguments = inspect.getcallargs(function, *args, ** kwargs)
        spec = {
            'name': function.__name__,
            'module': function.__module__,
            'signature': signature,
        }
        doc_template = dict(spec)
        doc_template['argument'] = arguments
        spec.update(
            {'argument.{}'.format(k): v for k,v in arguments.items() if not type(v) == dict})
        spec.update(
            {'argument.{}.{}'.format(k, k2): v2 for k,v in arguments.items() if type(v) == dict for k2,v2 in v.items()})
        logger.debug("Cached function call for '{}{}'.".format(
            function.__name__, signature))
        try:
            doc = self._cache.find_one(spec)
        except InvalidDocument as error:
            raise RuntimeError("Failed to encode function arguments.") from error
        else:
            if doc is None:
                result = function(* args, ** kwargs)
                logger.debug("No results found. Executing...")
                return self._store_in_cache(spec, doc_template, result)
            else:
                logger.debug("Results found. Trying to load.")
                try:
                    return pickle.loads(doc['data'])
                except Exception as error:
                    raise RuntimeWarning("Unable to retrieve chached result.") from error

    @property
    def milestones(self):
        return self._project.get_milestones(self.get_id()) 
