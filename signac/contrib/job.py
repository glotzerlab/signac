import logging
import os
import shutil
import warnings
import threading
import datetime
import uuid
import multiprocessing
import json as serializer
from threading import Thread
from multiprocessing import Process

import pymongo
from pymongo.errors import DuplicateKeyError

from .. import VERSION_TUPLE
from ..core.storage import Storage
from ..core.jsondict import JSonDict
from ..core.mongodbdict import MongoDBDict as DBDocument
from .concurrency import DocumentLock
from .hashing import generate_hash_from_spec
from .constants import *

logger = logging.getLogger(__name__)

PYMONGO_3 = pymongo.version_tuple[0] == 3


def pulse_worker(collection, job_id, unique_id, stop_event, period=PULSE_PERIOD):
    while(True):
        logger.debug("Pulse while loop.")
        if stop_event.wait(timeout=PULSE_PERIOD):
            logger.debug("Stop pulse.")
            return
        else:
            logger.debug("Pulsing...")
            filter = {'_id': job_id}
            update = {'$set': {'pulse.{}'.format(
                unique_id): datetime.datetime.utcnow()}}
            if PYMONGO_3:
                collection.update_one(filter, update, upsert=True)
            else:
                collection.update(filter, update, upsert=True)


class JobNoIdError(RuntimeError):
    pass


class BaseJob(object):
    """Base class for all jobs classes.

    All properties and methods in this class do not require a online database connection."""

    def __init__(self, project, parameters, version=None):
        self._unique_id = str(uuid.uuid4())
        self._project = project
        if version is None:
            version = tuple(
                (int(v) for v in project.config.get('signac_version', (0, 1, 0))))
        self._version = version
        try:
            dict(parameters)
        except ValueError:
            raise TypeError("Illegal parameter type.")
        self._parameters = parameters
        self._id = None
        self._cwd = None
        self._wd = os.path.join(self._project.config[
                                'workspace_dir'], str(self.get_id()))
        self._fs = os.path.join(
            self._project.filestorage_dir(), str(self.get_id()))
        self._storage = None
        self._fn_dict = os.path.join(self._fs, FN_JOB_DOCUMENT)
        self._on_disk_document = None

    def get_id(self):
        """Returns the job's id.

        .. note::
           This function respects the project's version key."""
        if self._id is None:  # The id calculation is cached
            # The ID calculation was changed after version 0.1.
            # This is why we need to check the project version, to calculate it
            # in the correct way.
            if self._version == (0, 1):
                spec = dict(JOB_PARAMETERS_KEY=self._parameters,
                            project=self.get_project().get_id())
                self._id = generate_hash_from_spec(spec)
            else:  # new style
                self._id = generate_hash_from_spec(self._parameters)
            if VERSION_TUPLE < self._version:
                msg = "The project is configured for signac version {}, but the current signac version is {}. Update signac to use this project."
                raise RuntimeError(msg.format(self._version, VERSION_TUPLE))
            if VERSION_TUPLE > self._version:
                msg = "The project is configured for signac version {}, but the current signac version is {}. Execute `signac update` to update your project and get rid of this warning."
                warnings.warn(msg.format(self._version, VERSION_TUPLE))
        return self._id

    def get_uid(self):
        """Returns the job's unique id."""
        return self._unique_id

    def __str__(self):
        "Returns the job's id."
        return str(self.get_id())

    def parameters(self):
        "Returns the job's parameters."
        return dict(self._parameters)

    def get_project(self):
        "Returns the job's associated project."
        return self._project

    def get_workspace_directory(self):
        "Returns the job's working directory."
        return self._wd

    def get_filestorage_directory(self):
        "Returns the job's filestorage directory."
        return self._fs

    @property
    def storage(self):
        "Return the storage, associated with this job."
        if self._storage is None:
            self._create_directories()
            self._storage = Storage(
                fs_path=self._fs,
                wd_path=self._wd)
        return self._storage

    def _get_on_disk_document(self):
        "Return the on-disk document."
        if self._on_disk_document is None:
            self._create_directories()
            self._on_disk_document = JSonDict(
                self._fn_dict, synchronized=True, write_concern=True)
        return self._on_disk_document

    def _make_manifest(self):
        "Create the manifest, to be stored within the job's directories."
        return dict(project=self.get_project().get_id(), parameters=self.parameters())

    def _create_directories(self):
        "Create the job's associated directories."
        manifest = self._make_manifest()
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
                    blob = serializer.dumps(manifest) + '\n'
                    file.write(blob.encode())
            except FileNotFoundError as error:
                msg = "Unable to write manifest file to '{}'."
                raise RuntimeError(msg.format(fn_manifest)) from error

    def _fn_open_flag(self):
        "Return the job's unique open flag filename."
        return os.path.join(self.get_workspace_directory(), FN_OPEN_FLAG.format(uid=self.get_uid()))

    def _flag_open(self):
        "Mark job as active."
        with open(self._fn_open_flag(), 'wb'):
            pass

    def _remove_open_flag(self):
        "Remove active flag from job."
        try:
            os.remove(self._fn_open_flag())
        except FileNotFoundError:
            pass

    def _open(self):
        msg = "Opened job with id: '{}'."
        logger.info(msg.format(self.get_id()))
        self._cwd = os.getcwd()
        self._create_directories()
        os.chdir(self.get_workspace_directory())
        self._flag_open()

    def open(self):
        """Open the job.

        Creates and changes into the job's working directory.
        It is generally advised to not use this method directly, but use the job as context manager instead.
        """
        return self._open()

    def _close_stage_one(self):
        os.chdir(self._cwd)
        self._cwd = None

    def _close_stage_two(self):
        # The automatic removal is deactivated since 0.1.1
        # if self.num_open_instances() == 0:
        #    shutil.rmtree(self.get_workspace_directory(), ignore_errors = True)
        self._remove_open_flag()
        msg = "Closing job with id: '{}'."
        logger.info(msg.format(self.get_id()))

    def close(self):
        self._close_stage_one()
        self._close_stage_two()

    def __enter__(self):
        """Open the job as context manager.

        This function behaves like this::
            try:
                job.open()
                # do something
            except Exception:
                # an error occured
                raise
            finally:
                job.close()
        """
        self.open()
        return self

    def __exit__(self, err_type, err_value, traceback):
        "Close the job."
        self._close_stage_one()  # always executed
        if err_type is None:
            self._close_stage_two()  # only executed if no error occurd
        return False

    def clear_workspace_directory(self):
        "Remove all content from the job's working directory."
        try:
            shutil.rmtree(self.get_workspace_directory())
        except FileNotFoundError:
            pass
        self._create_directories()

    def storage_filename(self, filename):
        warnings.warn("This function is deprecated.", DeprecationWarning)
        return os.path.join(self.get_filestorage_directory(), filename)


class OfflineJob(BaseJob):

    def __init__(self, project, parameters, version=None):
        super(OfflineJob, self).__init__(project=project,
                                         parameters=parameters, version=version)

    @property
    def document(self):
        "Access the job's disk document."
        return self._get_on_disk_document()

    @property
    def collection(self):
        msg = "Access to the job's collection requires a database connection!"
        raise AttributeError(msg)


class OnlineJob(BaseJob):
    """A OnlineJob is a job with active database connection.

    .. note::
       Instances of OnlineJob should only be used with reliable database connection. See also :class OfflineJob:.
    """

    def __init__(self, project, parameters, blocking=True, timeout=-1, version=None):
        """Initialize a job, specified by its parameters.

        :param parameters: A dictionary specifying the job parameters.
        :param blocking: Block until the job is openend.
        :param timeout: Wait a maximum of :param timeout: seconds. A value -1 specifies to wait infinitely.
        :returns: An instance of OnlineJob.
        :raises: DocumentLockError

        .. note::
           The constructor will raise a DocumentLockError if it was impossible to instantiate the job within the specified timeout.
        """
        super(OnlineJob, self).__init__(project=project,
                                        parameters=parameters, version=version)
        self._collection = None
        self._timeout = timeout
        self._blocking = blocking
        self._lock = None
        self._dbdocument = None
        self._pulse = None
        self._pulse_stop_event = None
        self._registered_flag = False

    def _filter(self):
        "Returns a filter, to identify job documents by id."
        return {'_id': self.get_id()}

    def _make_doc(self):
        "Create the job document for this job."
        doc = dict(self._filter())
        doc[JOB_PARAMETERS_KEY] = self._parameters
        return doc

    def _register_online(self):
        "Register this job in the project database."
        try:
            if PYMONGO_3:
                result = self._project._get_jobs_collection().find_one_and_update(
                    filter=self._filter(),
                    update={'$setOnInsert': self._make_doc()},
                    upsert=True,
                    return_document=pymongo.ReturnDocument.AFTER)
                assert str(result['_id']) == str(self.get_id())
            else:
                f = self._filter()
                result = self._project._get_jobs_collection().update(
                    f,
                    document={'$setOnInsert': self._make_doc()},
                    upsert=True,
                    new=True)
                assert f['_id'] == str(self.get_id())
        except DuplicateKeyError as error:
            warnings.warn(error)

    def _registered(self):
        "Register the job, if not already registered."
        if not self._registered_flag:
            self._register_online()
            self._registered_flag = True

    def _get_jobs_doc_collection(self):
        "Return the job's document collection."
        return self._project.get_db()[str(self.get_id())]

    def _add_instance(self):
        "Add the job's unique id to the executing list in the database."
        doc = {'$push': {'executing': self._unique_id}}
        if PYMONGO_3:
            self._project._get_jobs_collection().update_one(self._filter(), doc)
        else:
            self._project._get_jobs_collection().update(self._filter(), doc)

    def _remove_instance(self):
        "Remove the job's unique id from the executing list in the database."
        update = {'$pull': {'executing': self._unique_id}}
        if PYMONGO_3:
            self._project._get_jobs_collection().find_one_and_update(
                self._filter(), update=update, return_document=pymongo.ReturnDocument.AFTER)
        else:
            self._project._get_jobs_collection().find_and_modify(
                self._filter(), update=update, new=True)

    def _start_pulse(self, process=True):
        "Start the job pulse, used for identifying 'dead' jobs."
        logger.debug("Starting pulse.")
        assert self._pulse is None
        assert self._pulse_stop_event is None
        kwargs = {
            'collection': self._project._get_jobs_collection(),
            'job_id': self.get_id(),
            'unique_id': self._unique_id}
        if not self._project.config.get('noforking', False):
            try:
                self._pulse_stop_event = multiprocessing.Event()
                kwargs['stop_event'] = self._pulse_stop_event
                self._pulse = Process(
                    target=pulse_worker, kwargs=kwargs, daemon=True)
                self._pulse.start()
                return
            except AssertionError:
                logger.debug(
                    "Failed to start pulse process, falling back to pulse thread.")
        self._pulse_stop_event = threading.Event()
        kwargs['stop_event'] = self._pulse_stop_event
        self._pulse = Thread(target=pulse_worker, kwargs=kwargs)
        self._pulse.start()

    def _stop_pulse(self):
        "Stop the job pulse, used for identifying 'dead' jobs."
        if self._pulse is not None:
            logger.debug("Trying to stop pulse.")
            self._pulse_stop_event.set()
            self._pulse.join(2 * PULSE_PERIOD)
            assert not self._pulse.is_alive()
            doc = {'$unset': {'pulse.{}'.format(self._unique_id): ''}}
            if PYMONGO_3:
                self._project._get_jobs_collection().update_one(self._filter(), doc)
            else:
                self._project._get_jobs_collection().update(self._filter(), doc)
            self._pulse = None
            self._pulse_stop_event = None

    def _open(self):
        "Open this job."
        super(OnlineJob, self)._open()
        self._start_pulse()
        self._add_instance()

    def _close_stage_one(self):
        super(OnlineJob, self)._close_stage_one()
        self._stop_pulse()
        self._remove_instance()

    def _get_lock(self, blocking=None, timeout=None):
        "Obtain a lock for the job's database document."
        self._registered()
        return DocumentLock(
            self._project._get_jobs_collection(), self.get_id(),
            blocking=blocking or self._blocking,
            timeout=timeout or self._timeout,)

    def open(self):
        "Try to lock the job and then open."
        with self._get_lock():
            self._open()

    def close(self):
        "Try to lock the job and then close."
        with self._get_lock():
            self._close_stage_one()
            self._close_stage_two()

    def force_release(self):
        "Release the job's lock forcibly. Use with caution!"
        self._get_lock().force_release()

    def __exit__(self, err_type, err_value, traceback):
        "Exit the context manager."
        with self._get_lock():
            if err_type is None:
                self._close_stage_one()  # always executed
                self._close_stage_two()  # only executed if no error occurd
            else:
                err_doc = '{}:{}'.format(err_type, err_value)
                if PYMONGO_3:
                    self._project._get_jobs_collection().update_one(
                        self._filter(), {'$push': {JOB_ERROR_KEY: err_doc}})
                else:
                    self._project._get_jobs_collection().update(
                        self._filter(), {'$push': {JOB_ERROR_KEY: err_doc}})
                self._close_stage_one()
                return False

    def load_document(self):
        """Load a on-disk document to the online document.
        """
        self.document.update(self._get_on_disk_document())

    def save_document(self):
        "Save the online document on disk."
        self._get_on_disk_document().update(self.document)

    @property
    def document(self):
        "Return the document, associated with this job."
        if self._dbdocument is None:
            self._dbdocument = DBDocument(
                self._project._collection,
                self.get_id())
        return self._dbdocument

    @property
    def collection(self):
        "Return the database collection, associated with this job."
        return self._get_jobs_doc_collection()

    def clear(self):
        "Remove all content from this job, but not the registration."
        self.clear_workspace_directory()
        self.storage.clear()
        self.document.clear()
        self._get_jobs_doc_collection().drop()

    def _remove(self):
        "Remove all content from this job, including the registration."
        self.clear()
        self.storage.remove()
        self.document.remove()
        try:
            shutil.rmtree(self.get_workspace_directory())
        except FileNotFoundError:
            pass
        if PYMONGO_3:
            self._project._get_jobs_collection().delete_one(self._filter())
        else:
            self._project._get_jobs_collection().remove(self._filter())

    def remove(self, force=False):
        """"Remove all content and registration of this job with the project.

        :param force: Set to True, to ignore warnings about open instances.
        :raises RuntimeError

        This method will raise a RuntimeError, if :param force: is not True and the job signals open instances.
        """
        if not force:
            if not self.num_open_instances() == 0:
                msg = "You are trying to remove a job, which has {} open instance(s). Use 'force=True' to ignore this."
                raise RuntimeError(msg.format(self.num_open_instances()))
        self._remove()

    def _open_instances(self):
        "Return the unique id's of open instances."
        job_doc = self._project._get_jobs_collection().find_one(self._filter())
        if job_doc is None:
            return list()
        else:
            return job_doc.get('executing', list())

    def num_open_instances(self):
        "Return the number of open instances."
        return len(self._open_instances())

    def is_exclusive_instance(self):
        "Returns True when this is the only unqiue instance of this job."
        return self.num_open_instances() <= 1

    def lock(self, blocking=True, timeout=-1):
        """Try to lock this job.

        :param blocking: Block until the lock was aquired if True.
        :param timeout: Maximum number of seconds to block, before timeout.
        """
        return self._project._lock_job(
            self.get_id(),
            blocking=blocking, timeout=timeout)

    def import_job(self, other):
        "Import the storage and job document from :param other:."
        for key in other.document:
            self.document[key] = other.document[key]
        for fn in other.storage.list_files():
            with other.storage.open_file(fn, 'rb') as src:
                with self.storage.open_file(fn, 'wb') as dst:
                    dst.write(src.read())
        for doc in other.collection.find():
            if PYMONGO_3:
                self.collection.insert_one(doc)
            else:
                self.collection.save(doc)


class Job(OnlineJob):
    pass
