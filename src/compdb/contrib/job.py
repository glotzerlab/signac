import logging, threading
logger = logging.getLogger('job')

JOB_ERROR_KEY = 'error'
MILESTONE_KEY = '_milestones'
PULSE_PERIOD = 10

def generate_hash_from_spec(spec):
    import json, hashlib
    blob = json.dumps(spec, sort_keys = True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()

def spec_for_nested_dict(nd):
    spec.update(
        {'argument.{}'.format(k): v for k,v in nd.items() if not type(v) == dict})
    spec.update(
        {'argument.{}.{}'.format(k, k2): v2 for k,v in nd.items() if type(v) == dict for k2,v2 in v.items()})

class PulseThread(threading.Thread):
    def __init__(self, collection, _id, unique_id, period = PULSE_PERIOD):
        super().__init__()
        from threading import Event
        self._collection = collection
        self._job_id = _id
        self._unique_id = unique_id
        self._period = period
        self._stop_event = Event()

    def run(self):
        from datetime import datetime
        from time import sleep
        while(True):
            if self._stop_event.is_set():
                return
            self._collection.update(
                {'_id': self._job_id},
                {'$set':
                    {'pulse.{}'.format(self._unique_id): datetime.utcnow()}},
                upsert = True,)
            sleep(self._period)

    def stop(self):
        self._stop_event.set()

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

class JobNoIdError(RuntimeError):
    pass

class Job(object):
    
    def __init__(self, project, spec, blocking = True, timeout = -1):
        import uuid, os
        from ..core.storage import Storage
        from ..core.dbdocument import DBDocument
        from . concurrency import DocumentLock

        self._unique_id = str(uuid.uuid4())
        self._project = project
        self._spec = spec
        self._collection = None
        self._cwd = None
        self._obtain_id()
        self._with_id()
        self._wd = os.path.join(self._project.config['working_dir'], str(self.get_id()))
        self._fs = os.path.join(self._project.filestorage_dir(), str(self.get_id()))
        self._create_directories()
        self._storage = Storage(
            fs_path = self._fs,
            wd_path = self._wd)
        self._lock = DocumentLock(
            self._project.get_jobs_collection(), self.get_id(),
            blocking = blocking, timeout = timeout)
        self._jobs_doc_collection = self._project.get_project_db()[str(self.get_id())]
        self._dbuserdoc = DBDocument(
            self._project.collection, self.get_id())
        if self._project.develop_mode():
            msg = "Project '{}' is in development mode."
            logger.warning(msg.format(self._project.get_id()))
        self._pulse = PulseThread(
            self._project.get_jobs_collection(),
            self.get_id(), self._unique_id)

    def __str__(self):
        return self.get_id()

    @property
    def spec(self):
        return self._spec

    def parameters(self):
        return self._spec.get('parameters', None)

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

    def _start_pulse(self):
        self._pulse.start()

    def _stop_pulse(self):
        self._pulse.stop()
        self._pulse.join(1)
        self._project.get_jobs_collection().update(
            {'_id': self.get_id()},
            {'$unset': 
                {'pulse.{}'.format(self._unique_id): ''}})

    def _open(self):
        import os
        self._with_id()
        self._start_pulse()
        self._cwd = os.getcwd()
        self._create_directories()
        os.chdir(self.get_working_directory())
        self._add_instance()
        msg = "Opened job with id: '{}'."
        logger.info(msg.format(self.get_id()))

    def _close_with_error(self):
        import shutil, os
        self._with_id()
        os.chdir(self._cwd)
        self._cwd = None
        self._stop_pulse()
        self._remove_instance()

    def _close(self):
        import shutil, os
        if self.num_open_instances() == 0:
            shutil.rmtree(self.get_working_directory())
        msg = "Closing job with id: '{}'."
        logger.info(msg.format(self.get_id()))

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
            try:
                self._spec.update({'_id': _id})
                logger.debug("Opening with spec: {}".format(self._spec))
                result = self._project.get_jobs_collection().update(
                    spec = self._spec,
                    document = {'$setOnInsert': self._spec},
                    upsert = True)
            except DuplicateKeyError as error:
                pass
            else:
                assert result['ok']
                if result['updatedExisting']:
                    _id = self._project.get_jobs_collection().find_one(self._spec)['_id']
                else:
                    _id = result['upserted']
        else:
            _id = self._spec['_id']
        self._spec = self._project.get_jobs_collection().find_one({'_id': _id})
        assert self._spec is not None
        assert self.get_id() == _id

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_value, traceback):
        import os
        with self._lock:
            if err_type is None:
                self._close_with_error()
                self._close()
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

    def clear(self):
        self.clear_working_directory()
        self._storage.clear()
        self._dbuserdoc.clear()
        self._jobs_doc_collection.drop()

    def remove(self, force = False):
        self._with_id()
        if not force:
            if not self.num_open_instances() == 0:
                msg = "You are trying to remove a job, which has {} open instance(s). Use 'force=True' to ignore this."
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

    @property
    def cache(self):
        return self._project.get_cache()

    def cached(self, function, * args, ** kwargs):
        return self.cache.run(function, * args, ** kwargs) 

    @property
    def milestones(self):
        return self._project.get_milestones(self.get_id()) 
