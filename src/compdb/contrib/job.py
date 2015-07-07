import logging, threading
logger = logging.getLogger(__name__)
import warnings

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

import multiprocessing
import threading

JOB_ERROR_KEY = 'error'
MILESTONE_KEY = '_milestones'
PULSE_PERIOD = 1

FN_MANIFEST = '.compdb.json'
MANIFEST_KEYS = ['project', 'parameters']

from . project import JOB_DOCS

def pulse_worker(collection, job_id, unique_id, stop_event, period = PULSE_PERIOD):
    from datetime import datetime
    from time import sleep
    while(True):
        logger.debug("Pulse while loop.")
        if stop_event.wait(timeout = PULSE_PERIOD):
            logger.debug("Stop pulse.")
            return
        else:
            logger.debug("Pulsing...")
            collection.update(
                {'_id': job_id},
                {'$set': {'pulse.{}'.format(unique_id): datetime.utcnow()}},
                upsert = True)

class JobNoIdError(RuntimeError):
    pass

class BaseJob(object):
    """Base class for all jobs classes.

    All properties and methods in this class do not require a online database connection."""
    
    def __init__(self, project, parameters):
        import uuid, os
        self._unique_id = str(uuid.uuid4())
        self._project = project
        self._parameters = parameters
        self._id = None
        self._cwd = None
        self._wd = os.path.join(self._project.config['workspace_dir'], str(self.get_id()))
        self._fs = os.path.join(self._project.filestorage_dir(), str(self.get_id()))
        self._storage = None

    def get_id(self):
        """Returns the job's id."""
        if self._id is None:
            # Cache the id calcuation.
            from .hashing import generate_hash_from_spec
            self._id = generate_hash_from_spec(self._parameters)
        return self._id

    def __str__(self):
        """Returns the job's id."""
        return self.get_id()

    def get_workspace_directory(self):
        return self._wd

    def get_filestorage_directory(self):
        return self._fs

    def _get_manifest(self):
        return dict(project = self.get_project().get_id(), parameters = self.parameters())

    def _create_directories(self):
        import os
        import json as serializer
        manifest = self._get_manifest()
        for dir_name in (self.get_workspace_directory(), self.get_filestorage_directory()):
            try:
                os.makedirs(dir_name)
            except OSError:
                pass
            fn_manifest = os.path.join(dir_name, FN_MANIFEST)
            msg = "Writing job manifest to '{fn}'."
            logger.debug(msg.format(fn=fn_manifest))
            try:
                with open(fn_manifest, 'wb') as file:
                    blob = serializer.dumps(manifest)+'\n'
                    file.write(blob.encode())
            except FileNotFoundError as error:
                msg = "Unable to write manifest file to '{}'."
                raise RuntimeError(msg.format(fn_manifest)) from error

    def parameters(self):
        return self._parameters

    def get_project(self):
        return self._project

    def _open(self):
        import os
        msg = "Opened job with id: '{}'."
        logger.info(msg.format(self.get_id()))
        self._cwd = os.getcwd()
        self._create_directories()
        os.chdir(self.get_workspace_directory())

    def _close_stage_one(self):
        import os
        os.chdir(self._cwd)
        self._cwd = None

    def _close_stage_two(self):
        # The working directory is no longer removed so this function does nothing.
        #import shutil, os
        #if self.num_open_instances() == 0:
        #    shutil.rmtree(self.get_workspace_directory(), ignore_errors = True)
        msg = "Closing job with id: '{}'."
        logger.info(msg.format(self.get_id()))

    def open(self):
        return self._open()

    def close(self):
        return self._close_stage_two()

    @property
    def storage(self):
        from ..core.storage import Storage
        if self._storage is None:
            self._create_directories()
            self._storage = Storage(
                fs_path = self._fs,
                wd_path = self._wd)
        return self._storage

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_value, traceback):
        self._close_stage_one() # always executed
        if err_type is None:
            self._close_stage_two() # only executed if no error occurd
        return False

    def clear_workspace_directory(self):
        import shutil
        try:
            shutil.rmtree(self.get_workspace_directory())
        except FileNotFoundError:
            pass
        self._create_directories()

    def storage_filename(self, filename):
        warnings.warn("This function may be deprecated in future releases.", PendingDeprecationWarning)
        from os.path import join
        return join(self.get_filestorage_directory(), filename)

class OnlineJob(BaseJob):
    
    def __init__(self, project, parameters, blocking = True, timeout = -1):
        super(OnlineJob, self).__init__(project=project, parameters=parameters)
        self._collection = None
        self._timeout = timeout
        self._blocking = blocking
        self._lock = None
        self._dbdocument = None
        self._pulse = None
        self._pulse_stop_event = None
        self._register_online()

    def _get_jobs_doc_collection(self):
        return self._project.get_project_db()[str(self.get_id())]

    @property
    def spec(self):
        raise DeprecationWarning("The 'spec' property is deprecated.")
        #return self._spec

    def _with_id(self):
        warnings.warn("The job's id is calculated offline, which makes this function obsolete.", DeprecationWarning)
        #if self.get_id() is None:
        #    raise JobNoIdError()
        #assert self.get_id() is not None
    
    def _filter(self):
        return {'_id': self.get_id()}

    def _add_instance(self):
        doc = {'$push': {'executing': self._unique_id}}
        if PYMONGO_3:
            self._project.get_jobs_collection().update_one(self._filter(), doc)
        else:
            self._project.get_jobs_collection().update(self._filter(), doc)

    def _remove_instance(self):
        update = {'$pull': {'executing': self._unique_id}}
        if PYMONGO_3:
            result = self._project.get_jobs_collection().find_one_and_update(
                self._filter(), update=update, return_document = pymongo.ReturnDocument.AFTER)
        else:
            result = self._project.get_jobs_collection().find_and_modify(self._filter(), update=update, new=True)
        return len(result['executing'])

    def _start_pulse(self, process = True):
        from multiprocessing import Process
        from threading import Thread
        logger.debug("Starting pulse.")
        assert self._pulse is None
        assert self._pulse_stop_event is None
        kwargs = {
            'collection': self._project.get_jobs_collection(),
            'job_id': self.get_id(),
            'unique_id': self._unique_id}
        if not self._project.config.get('noforking', False):
            try:
                self._pulse_stop_event = multiprocessing.Event()
                kwargs['stop_event'] = self._pulse_stop_event
                self._pulse = Process(target = pulse_worker, kwargs = kwargs, daemon = True)
                self._pulse.start()
                return
            except AssertionError as error:
                logger.debug("Failed to start pulse process, falling back to pulse thread.")
        self._pulse_stop_event = threading.Event()
        kwargs['stop_event'] = self._pulse_stop_event
        self._pulse = Thread(target = pulse_worker, kwargs = kwargs)
        self._pulse.start()

    def _stop_pulse(self):
        if self._pulse is not None:
            logger.debug("Trying to stop pulse.")
            self._pulse_stop_event.set()
            self._pulse.join(2 * PULSE_PERIOD)
            assert not self._pulse.is_alive()
            doc = {'$unset': {'pulse.{}'.format(self._unique_id): ''}}
            if PYMONGO_3:
                self._project.get_jobs_collection().update_one(self._filter(), doc)
            else:
                self._project.get_jobs_collection().update(self._filter(), doc)
            self._pulse = None
            self._pulse_stop_event = None

    def _open(self):
        super(OnlineJob, self)._open()
        self._start_pulse()
        self._add_instance()
        #self._dbdocument.open()

    def _close_stage_one(self):
        super(OnlineJob, self)._close_stage_one()
        #self._dbdocument.close()
        self._stop_pulse()
        self._remove_instance()

    def _get_lock(self, blocking = None, timeout = None):
        from . concurrency import DocumentLock
        return DocumentLock(
                self._project.get_jobs_collection(), self.get_id(),
                blocking = blocking or self._blocking,
                timeout = timeout or self._timeout,)

    def open(self):
        with self._get_lock():
            self._open()

    def close(self):
        with self._get_lock():
            self._close_stage_two()

    def force_release(self):
        self._get_lock().force_release()

    def _obtain_id(self):
        msg = "This function is obsolete, as the id is always(!) calculate offline!"
        raise DeprecationWarning(msg)

    def _register_online(self):
        from . errors import ConnectionFailure
        from pymongo.errors import DuplicateKeyError
        try:
            doc = dict(parameters = self._parameters, _id = self.get_id())
            result = self._project.get_jobs_collection().find_one_and_update(
                filter = self._filter(),
                update = {'$setOnInsert': doc},
                upsert = True,
                return_document = pymongo.ReturnDocument.AFTER)
        except DuplicateKeyError as error:
            warnings.warn(error)
        else:
            assert str(result['_id']) == str(self.get_id())

    def __exit__(self, err_type, err_value, traceback):
        import os
        with self._get_lock():
            if err_type is None:
                self._close_stage_one() # always executed
                self._close_stage_two() # only executed if no error occurd
            else:
                err_doc = '{}:{}'.format(err_type, err_value)
                self._project.get_jobs_collection().update(
                    self._filter(), {'$push': {JOB_ERROR_KEY: err_doc}})
                self._close_stage_one()
                return False
    
    def clear(self):
        self.clear_workspace_directory()
        self.storage.clear()
        self.document.clear()
        self._get_jobs_doc_collection().drop()

    def remove(self, force = False):
        if not force:
            if not self.num_open_instances() == 0:
                msg = "You are trying to remove a job, which has {} open instance(s). Use 'force=True' to ignore this."
                raise RuntimeError(msg.format(self.num_open_instances()))
        self._remove()

    def _remove(self):
        import shutil
        self.clear()
        self.storage.remove()
        self.document.remove()
        try:
            shutil.rmtree(self.get_workspace_directory())
        except FileNotFoundError:
            pass
        if PYMONGO_3:
            self._project.get_jobs_collection().delete_one(self._filter())
        else:
            self._project.get_jobs_collection().remove(self._filter())

    @property
    def collection(self):
        return self._get_jobs_doc_collection()

    def _open_instances(self):
        job_doc = self._project.get_jobs_collection().find_one(self._filter())
        if job_doc is None:
            return list()
        else:
            return job_doc.get('executing', list())

    def num_open_instances(self):
        return len(self._open_instances())

    def is_exclusive_instance(self):
        return self.num_open_instances() <= 1

    def lock(self, blocking = True, timeout = -1):
        return self._project.lock_job(
            self.get_id(),
            blocking = blocking, timeout = timeout)

    @property
    def document(self):
        if self._dbdocument is None:
            from ..core.mongodbdict import MongoDBDict as DBDocument
            self._dbdocument = DBDocument(
                self._project.collection,
                self.get_id())
        return self._dbdocument

    @property
    def cache(self):
        return self._project.get_cache()

    def cached(self, function, * args, ** kwargs):
        return self.cache.run(function, * args, ** kwargs) 

    def import_job(self, other):
        for key in other.document:
            self.document[key] = other.document[key]
        for fn in other.storage.list_files():
            with other.storage.open_file(fn, 'rb') as src:
                with self.storage.open_file(fn, 'wb') as dst:
                    dst.write(src.read())
        for doc in other.collection.find():
            self.collection.insert_one(doc)

class Job(OnlineJob):
    pass
