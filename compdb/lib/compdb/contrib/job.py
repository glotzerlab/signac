import logging
logger = logging.getLogger('job')

from .. config import CONFIG

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

#    def _get_status(self):
#        doc = self._jobs_collection.find_one(self._job_doc_spec())
#        assert doc is not None
#        return doc[JOB_STATUS_KEY]
#
#    def _set_status(self, status):
#        self._with_id()
#        self._jobs_collection.update(
#            spec = self._job_doc_spec(),
#            document = {'$set': {JOB_STATUS_KEY: status}})
#
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

    def open(self):
        import os
        with self._lock:
            self._with_id()
            self._cwd = os.getcwd()
            self._wd = os.path.join(CONFIG['working_dir'], str(self.get_id()))
            self._fs = os.path.join(filestorage_dir(), str(self.get_id()))
            self._create_directories()
            os.chdir(self.get_working_directory())
            self._jobs_collection.update(
                spec = self._job_doc_spec(),
                document = {'$push': {'executing': self._unique_id}})
            #self._set_status('open')
            msg = "Opened job with id: '{}'."
            logger.debug(msg.format(self.get_id()))

    def close(self):
        import shutil, os
        with self._lock:
            self._with_id()
            os.chdir(self._cwd)
            self._cwd = None
            result = self._jobs_collection.find_and_modify(
                query = self._job_doc_spec(),
                update = {'$pull': {'executing': self._unique_id}},
                new = True)
            if len(result['executing']) == 0:
                shutil.rmtree(self.get_working_directory())
                #self._set_status('closed')

    def __enter__(self):
        import os
        from . concurrency import DocumentLock
        result = get_jobs_collection().update(
            spec = self._spec,
            document = {'$set': self._spec},
            #document = self._spec,
            upsert = True)
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
        if err_type is None:
            self.close()
        else:
            with self._lock:
                #self._set_status('error')
                err_doc = '{}:{}'.format(err_type, err_value)
                get_jobs_collection().update(
                    self.spec, {'$push': {JOB_ERROR_KEY: err_doc}})
                os.chdir(self._cwd)
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
            assert self.num_open_instances() == 0
        self._remove()

    def _remove(self):
        import shutil
        #assert self._get_status() != 'open'
        for dir in (self.get_working_directory(), self.get_filestorage_directory()):
            try:
                shutil.rmtree(self.get_working_directory())
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

    def storage_filename(self, filename):
        from os.path import join
        return join(self.get_filestorage_directory(), filename)

    def open_storagefile(self, filename, * args, ** kwargs):
        return open(self.storage_filename(filename), * args, ** kwargs)

    def remove_file(self, filename):
        import os
        os.remove(self.storage_filename(filename))
