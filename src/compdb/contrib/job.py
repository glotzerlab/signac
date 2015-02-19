import logging
logger = logging.getLogger('job')

from compdb.core.config import CONFIG
from compdb.core import _get_db

JOB_STATUS_KEY = 'status'
JOB_ERROR_KEY = 'error'
JOB_NAME_KEY = 'name'
JOB_PARAMETERS_KEY = 'parameters'

def valid_name(name):
    return not name.startswith('_compdb')

def _get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient(CONFIG['database']['host'])
    return client[db_name]

def get_db(db_name):
    assert valid_name(db_name)
    return _get_db(db_name)

def get_meta_db():
    return _get_db(CONFIG['database_meta'])

def get_jobs_collection():
    return get_meta_db()['jobs']

def get_project_id():
    return CONFIG['project']

def get_project_db():
    return get_db(get_project_id())

def filestorage_dir():
    return CONFIG['filestorage_dir']

def job_spec(name, parameters):
    spec = {
        'project':  get_project_id(),
    }
    if name is not None:
        spec.update({JOB_NAME_KEY: name})
    if parameters is not None:
        spec.update({JOB_PARAMETERS_KEY: parameters})
    return spec

class JobNoIdError(RuntimeError):
    pass

class Job(object):
    
    def __init__(self, spec):
        import uuid
        self._unique_id = uuid.uuid4()
        self._spec = spec
        self._jobs_collection = get_meta_db()['jobs']
        self._lock = None
        self._collection = None
        self._cwd = None
        self._wd = None
        self._fs = None

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
        self._jobs_collection.update(
            spec = self._job_doc_spec(),
            document = {'$push': {'executing': self._unique_id}})

    def _remove_instance(self):
        result = self._jobs_collection.find_and_modify(
            query = self._job_doc_spec(),
            update = {'$pull': {'executing': self._unique_id}},
            new = True)
        return len(result['executing'])

    def _open(self):
        import os
        self._with_id()
        self._cwd = os.getcwd()
        self._wd = os.path.join(CONFIG['working_dir'], str(self.get_id()))
        self._fs = os.path.join(filestorage_dir(), str(self.get_id()))
        self._create_directories()
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

    def __enter__(self):
        import os
        from . concurrency import DocumentLock
        from pymongo.errors import DuplicateKeyError
        from . import sleep_random
        num_attempts = 3
        for attempt in range(num_attempts):
            try:
                result = get_jobs_collection().update(
                    spec = self._spec,
                    document = {'$set': self._spec},
                    upsert = True)
                break
            except DuplicateKeyError as error:
                if attempt >= (num_attempts-1):
                    raise RuntimeError("Unable to open job. "
                     "Use `contrib.sleep_random` if you have trouble with "
                     "opening jobs in concurrency.") from error
                else:
                    sleep_random(0.1)
        assert result['ok']
        if result['updatedExisting']:
            _id = get_jobs_collection().find_one(self._spec)['_id']
        else:
            _id = result['upserted']
        self._spec = get_jobs_collection().find_one({'_id': _id})
        assert self._spec is not None
        assert self.get_id() == _id
        self._lock = DocumentLock(self._jobs_collection, self.get_id())
        self.open()
        return self

    def __exit__(self, err_type, err_value, traceback):
        import os
        with self._lock:
            if err_type is None:
                self._close_with_error()
            else:
                err_doc = '{}:{}'.format(err_type, err_value)
                get_jobs_collection().update(
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

    def clear_filestorage_directory(self):
        import shutil
        try:
            shutil.rmtree(self.get_filestorage_directory())
        except FileNotFoundError:
            pass
        self._create_directories()

    def clear(self):
        self.clear_working_directory()
        self.clear_filestorage_directory()
        self.collection.remove()

    def remove(self, force = False):
        self._with_id()
        if not force:
            if not self.num_open_instances() == 0:
                raise RuntimeWarning("You are trying to remove an open instance. "
                "Use 'force = True' to ignore this.")
        self._remove()

    def _remove(self):
        import shutil
        for dir in (self.get_working_directory(), self.get_filestorage_directory()):
            try:
                shutil.rmtree(dir)
            except FileNotFoundError:
                pass
        self.collection.drop()
        get_jobs_collection().remove(self._job_doc_spec())

    @property
    def collection(self):
        return get_project_db()['job_{}'.format(self.get_id())]

    def _open_instances(self):
        self._with_id()
        job_doc = self._jobs_collection.find_one(self._job_doc_spec())
        if job_doc is None:
            return list()
        else:
            return job_doc.get('executing', list())

    def num_open_instances(self):
        return len(self._open_instances())

    def is_exclusive_instance(self):
        return self.num_open_instances <= 1

    def lock(self, blocking = True, timeout = -1):
        from . concurrency import DocumentLock
        self._with_id()
        return DocumentLock(
            collection = self._jobs_collection,
            document_id = self.get_id(),
            blocking = blocking, timeout = timeout)

    def store(self, key, value):
        self.collection.update({}, {key: value}, upsert = True)

    def get(self, key):
        doc = self.collection.find_one(
            {key: {'$exists': True}}, fields = [key,])
        return doc if doc is None else doc.get(key)

    def storage_filename(self, filename):
        from os.path import join
        return join(self.get_filestorage_directory(), filename)

    def open_file(self, filename, * args, ** kwargs):
        return open(self.storage_filename(filename), * args, ** kwargs)

    def remove_file(self, filename):
        import os
        os.remove(self.storage_filename(filename))
    
    def store_file(self, filename):
        import shutil
        shutil.move(filename, self.storage_filename(filename))

    def restore_file(self, filename):
        import shutil
        shutil.move(self.storage_filename(filename), filename)

    def list_stored_files(self):
        import os 
        return os.listdir(self.get_filestorage_directory())

    def store_all(self):
        import os
        for file_or_dir in os.listdir(self.get_working_directory()):
            self.store_file(file_or_dir)
    
    def restore_all(self):
        import os
        for file_or_dir in self.list_stored_files():
            self.restore_file(file_or_dir)
