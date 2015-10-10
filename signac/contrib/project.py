import logging
import os
import re
import shutil
import warnings
import collections
import itertools
from queue import Queue
from logging.handlers import QueueHandler, QueueListener
from datetime import datetime, timedelta

import pymongo
import pymongo.errors
from bson.json_util import dumps

from .. import VERSION_TUPLE
from ..common.config import load_config
from ..common import host
from .concurrency import DocumentLock
from .errors import ConnectionFailure, DatabaseError
from .logging import MongoDBHandler, record_from_doc
from .job import OnlineJob, OfflineJob, PULSE_PERIOD
from .hashing import generate_hash_from_spec
from .constants import *

logger = logging.getLogger(__name__)

PYMONGO_3 = pymongo.version_tuple[0] == 3


def valid_name(name):
    return not name.startswith(SIGNAC_PREFIX)


class BaseProject(object):
    """Base class for all project classes.

    All properties and methods in this class do not require a online database connection.
    Application developers should usually not need to instantiate this class.
    See ``Project`` instead.
    """

    def __init__(self, config=None):
        """Initializes a BaseProject instance.

        Application developers should usually not need to instantiate this class.
        See ``Project`` instead.
        """
        if config is None:
            config = load_config()
        config.verify()
        self._config = config
        self._job_queue = None
        self._job_queue_ = None
        self._fetched_set = None

    def __str__(self):
        "Returns the project's id."
        return str(self.get_id())

    @property
    def config(self):
        "The project's configuration."
        return self._config

    def root_directory(self):
        "Returns the project's root directory as determined from the configuration."
        return self._config['project_dir']

    def get_id(self):
        """"Returns the project's id as determined from the configuration.

        :returns: str - The project id.
        :raises: KeyError

        This method raises ``KeyError`` if no project id could be determined.
        """
        try:
            return str(self.config['project'])
        except KeyError:
            msg = "Unable to determine project id. "
            msg += "Are you sure '{}' is a compDB project path?"
            raise LookupError(msg.format(os.path.abspath(os.getcwd())))

    def open_offline_job(self, parameters=None):
        """Open an offline job, specified by its parameters.

        :param parameters: A dictionary specifying the job parameters.
        :returns: An instance of OfflineJob.
        """
        return OfflineJob(
            project=self,
            parameters=parameters)

    def filestorage_dir(self):
        "Return the project's filestorage directory."
        return self.config['filestorage_dir']

    def workspace_dir(self):
        "Return the project's workspace directory."
        return self.config['workspace_dir']

    def _filestorage_dir(self):
        warnings.warn(
            "The method '_filestorage_dir' is deprecated.", DeprecationWarning)
        return self.config['filestorage_dir']

    def _workspace_dir(self):
        warnings.warn("The method '_workspace_dir' is deprecated.",
                      DeprecationWarning)
        return self.config['workspace_dir']


class OnlineProject(BaseProject):
    """OnlineProject extends BaseProject with properties and methods that require a database connection."""

    def __init__(self, config=None, client=None):
        """Initialize a Onlineproject.

        :param config:     A signac configuration instance.
        :param client:     A pymongo client instance.

        Both arguments are optional.
        If no config is provided, it will be fetched from the environment.
        If no client is provided, the client will be instantiated from the configuration when needed.

        .. note::
           Some methods in this class requires an online connection to a database!
        """
        super(OnlineProject, self).__init__(config=config)
        self._client = client
        # Online logging
        self._loggers = [logging.getLogger('signac')]
        self._logging_queue = Queue()
        self._logging_queue_handler = QueueHandler(self._logging_queue)
        self._logging_listener = None

    def _get_client(self):
        "Attempt to connect to the database host and store the client instance."
        if self._client is None:
            self._client = host.get_client(config=self.config)
        return self._client

    def get_db(self, db_name=None):
        """Return a database from the project's database host.

        :param db_name: The name of the database.
        :returns: The datbase with :param db_name: or the project's root database.
        """
        if db_name is None:
            return self._get_client()[self.get_id()]
        else:
            assert valid_name(db_name)
            return self._get_client()[db_name]

    def get_project_db(self):
        "Return the project's root database."
        warnings.warn(
            "The method 'get_project_db' will be deprecated in the future. Use 'get_db' instead.", PendingDeprecationWarning)
        return self.get_db(self.get_id())

    def _get_meta_db(self):
        return self.get_db()

    def get_jobs_collection(self):
        warnings.warn(
            "The method 'get_jobs_collection' is no longer part of the public API.", DeprecationWarning)
        return self._get_jobs_collection()

    def _get_jobs_collection(self):
        return self._get_meta_db()[JOB_META_DOCS]

    @property
    def _collection(self):
        return self.get_db()[JOB_DOCS]

    @property
    def collection(self):
        warnings.warn(
            "The property 'collection' is no longer part of the public API.", DeprecationWarning)
        return self._collection

    def _parameters_from_id(self, job_id):
        "Determine a job's parameters from the job id."
        result = self._get_jobs_collection().find_one(
            job_id, [JOB_PARAMETERS_KEY])
        if result is None:
            raise KeyError(job_id)
        try:
            return result[JOB_PARAMETERS_KEY]
        except KeyError:
            msg = "Unable to retrieve parameters for job '{}'. Database corrupted."
            raise DatabaseError(msg.format(job_id))

    def register_job(self, parameters=None):
        """Register a job for this project.

        :param parameters: A dictionary specifying the job parameters.
        """
        job = OnlineJob(self, parameters=parameters)
        job._register_online()

    def open_job(self, parameters=None, blocking=True, timeout=-1):
        """Open an online job, specified by its parameters.

        :param parameters: A dictionary specifying the job parameters.
        :param blocking: Block until the job is openend.
        :param timeout: Wait a maximum of :param timeout: seconds. A value -1 specifies to wait infinitely.
        :returns: An instance of OnlineJob.
        :raises: DocumentLockError

        .. note::
           This method will raise a DocumentLockError if it was impossible to open the job within the specified timeout.
        """
        return OnlineJob(
            project=self,
            parameters=parameters,
            blocking=blocking,
            timeout=timeout)

    def _open_job(self, **kwargs):
        return OnlineJob(project=self, **kwargs)

    def open_job_from_id(self, job_id, blocking=True, timeout=-1):
        """Open an online job, specified by its job id.

        :param job_id: The job's job_id.
        :param blocking: Block until the job is openend.
        :param timeout: Wait a maximum of :param timeout: seconds. A value -1 specifies to wait infinitely.
        :returns: An instance of OnlineJob.
        :raises: DocumentLockError

        .. warning: The job must be registered in the database, otherwise it is impossible to determine the parameters.

        .. note::
           This method will raise a DocumentLockError if it was impossible to open the job within the specified timeout.
        """
        return OnlineJob(
            project=self,
            parameters=self._parameters_from_id(job_id),
            blocking=blocking, timeout=timeout)

    def find_jobs(self, job_spec=None, spec=None, blocking=True, timeout=-1):
        """Find jobs, specified by the job's parameters and/or the job's document.

        :param job_spec: The filter for the job parameters.
        :param spec: The filter for the job document.
        :returns: An iterator over all OnlineJob instances, that match the criteria.
        """
        if job_spec is None:
            job_spec = {JOB_PARAMETERS_KEY: {'$exists': True}}
        else:
            job_spec = {JOB_PARAMETERS_KEY +
                        '.{}'.format(k): v for k, v in job_spec.items()}
        job_ids = list(self.find_job_ids(job_spec))
        if spec is not None:
            spec.update({'_id': {'$in': job_ids}})
            docs = self._collection.find(spec)
            job_ids = (doc['_id'] for doc in docs)
        for _id in job_ids:
            yield self.open_job_from_id(_id, blocking, timeout)

    def _active_job_ids(self):
        "Returns an iterator over all job_ids of active online jobs."
        spec = {
            'executing': {'$exists': True},
            '$where': 'this.executing.length > 0'}
        yield from self.find_job_ids(spec)

    def active_jobs(self, blocking=True, timeout=-1):
        "Returns an iterator over all active jobs."
        warnings.warn("This method returns actual jobs, not job ids now!")
        for _id in self._active_job_ids():
            yield self.open_job_from_id(_id, blocking, timeout)

    def num_active_jobs(self):
        spec = {
            'executing': {'$exists': True},
            '$where': 'this.executing.length > 0'}
        return self._get_jobs_collection().find(spec).count()

    def find(self, job_spec={}, spec={}, * args, ** kwargs):
        """Find job documents, specified by the job's parameters and/or the job's document.

        :param job_spec: The filter for the job parameters.
        :param spec: The filter for the job document.
        :returns: Documents matching all specifications.
        """
        job_ids = self.find_job_ids(job_spec)
        spec.update({'_id': {'$in': list(job_ids)}})
        yield from self._collection.find(spec, * args, ** kwargs)

    def clear(self, force=False):
        """Clear the project jobs and logs.

        .. note::
           Clearing the project is permanent. Use with caution!"""
        self.clear_logs()
        for job in self.find_jobs():
            job.remove(force=force)

    def remove(self, force=False):
        """Remove all jobs, logs and the complete project database.

          .. note::
             The removal is permanent. Use with caution!"""
        try:
            self.clear(force=force)
        except Exception as error:
            if force:
                logger.error("Error during clearing. Forced to ignore.")
            else:
                raise
        try:
            client = self._get_client()
            client.drop_database(self.get_id())
        except pymongo.errors.ConnectionFailure as error:
            msg = "{}: Failed to remove project database on '{}'."
            raise ConnectionFailure(msg.format(
                self.get_id(), client.address)) from error

    def _lock_job(self, job_id, blocking=True, timeout=-1):
        "Lock the job document of job with ``job_id``."
        return DocumentLock(
            self._get_jobs_collection(), job_id,
            blocking=blocking, timeout=timeout)

    def _unique_jobs_from_pulse(self):
        docs = self._get_jobs_collection().find(
            {'pulse': {'$exists': True}},
            ['pulse'])
        beats = [doc['pulse'] for doc in docs]
        for beat in beats:
            for uid, timestamp in beat.items():
                yield uid

    def job_pulse(self):
        uids = self._unique_jobs_from_pulse()
        for uid in uids:
            hb_key = 'pulse.{}'.format(uid)
            doc = self._get_jobs_collection().find_one(
                {hb_key: {'$exists': True}})
            yield uid, doc['pulse'][uid]

    def kill_dead_jobs(self, seconds=5 * PULSE_PERIOD):
        cut_off = datetime.utcnow() - timedelta(seconds=seconds)
        uids = self._unique_jobs_from_pulse()
        for uid in uids:
            hbkey = 'pulse.{}'.format(uid)
            self._get_jobs_collection().update(
                #{'pulse.{}'.format(uid): {'$exists': True},
                {hbkey: {'$lt': cut_off}},
                {'$pull': {'executing': uid},
                    '$unset': {hbkey: ''},
                 })

    def _get_links(self, url, parameters, fs):
        for w in self._walk_job_docs(parameters):
            src = os.path.join(fs, w['_id'])
            try:
                dd = collections.defaultdict(lambda: 'None')
                dd.update(w['parameters'])
                dst = url.format(** dd)
            except KeyError as error:
                msg = "Unknown parameter: {}"
                raise KeyError(msg.format(error)) from error
            yield src, dst

    def get_storage_links(self, url=None):
        if url is None:
            url = self.get_default_view_url()
        parameters = re.findall('\{\w+\}', url)
        yield from self._get_links(
            url, parameters, self.filestorage_dir())

    def get_workspace_links(self, url=None):
        if url is None:
            url = self.get_default_view_url()
        parameters = re.findall('\{\w+\}', url)
        yield from self._get_links(
            url, parameters, self.workspace_dir())

    def _aggregate_parameters(self, job_spec=None, uniqueonly=False):
        pipe = [
            {'$match': job_spec or dict()},
            {'$group': {
                '_id': False,
                'parameters': {'$addToSet': '$parameters'}}},
        ]
        result = self._get_jobs_collection().aggregate(pipe)
        parameters = collections.defaultdict(set)
        if PYMONGO_3:
            for doc in result:
                for p in doc['parameters']:
                    for k, v in p.items():
                        try:
                            parameters[k].add(v)
                        except TypeError:
                            parameters[k].add(generate_hash_from_spec(v))
        else:
            assert result['ok']
            if len(result['result']):
                for p in result['result'][0]['parameters']:
                    for k, v in p.items():
                        parameters[k].add(v)
        return set(k for k, v in sorted(parameters.items(), key=lambda i: len(i[1])) if not uniqueonly or len(v) > 1)

    def get_default_view_url(self):
        params = sorted(self._aggregate_parameters(uniqueonly=True))
        if len(params):
            return str(os.path.join(* itertools.chain.from_iterable(
                (str(p), '{' + str(p) + '}') for p in params)))
        else:
            return str()

    def create_view(self, url=None, make_copy=False, workspace=False, prefix=None):
        if url is None:
            url = self.get_default_view_url()
        if prefix is not None:
            url = os.path.join(prefix, url)
        parameters = re.findall('\{\w+\}', url)
        if workspace:
            links = self._get_links(url, parameters, self.workspace_dir())
        else:
            links = self._get_links(url, parameters, self.filestorage_dir())
            for src, dst in links:
                self._make_link(src, dst, make_copy=make_copy)

    def _make_link(self, src, dst, make_copy=False):
        try:
            os.makedirs(os.path.dirname(dst))
        except FileExistsError:
            pass
        try:
            if make_copy:
                shutil.copytree(src, dst)
            else:
                os.symlink(src, dst, target_is_directory=True)
        except FileExistsError:
            msg = "Failed to create view for url '{url}'. "
            msg += "Possible causes: A view with the same path exists already or you are not using enough parameters for uniqueness."
            raise RuntimeError(msg)

    def create_flat_view(self, job_spec=None, prefix=None):
        if prefix is None:
            prefix = os.getcwd()
        for fs_dst, fs_src in [('workspace', self.workspace_dir()),
                               ('storage', self.filestorage_dir())]:
            for job_id in self.find_job_ids():
                src = os.path.join(fs_src, job_id)
                dst = os.path.join(prefix, fs_dst, job_id)
                self._make_link(src, dst)

    def _find_job_docs(self, job_spec, * args, **kwargs):
        yield from self._get_jobs_collection().find(job_spec, *args, **kwargs)

    def _walk_job_docs(self, parameters, job_spec=None, * args, ** kwargs):
        yield from self._find_job_docs(
            job_spec=job_spec or dict(),
            sort=[('parameters.{}'.format(p), 1) for p in parameters],
            * args, ** kwargs)

    def dump_db_snapshot(self):
        job_docs = self._get_jobs_collection().find()
        for doc in job_docs:
            print(dumps(doc))
        docs = self.find()
        for doc in docs:
            print(dumps(doc))

    def _get_logging_collection(self):
        return self.get_db()[COLLECTION_LOGGING]

    def clear_logs(self):
        self._get_logging_collection().drop()

    def _logging_db_handler(self, lock_id=None):
        return MongoDBHandler(
            collection=self._get_logging_collection(),
            lock_id=lock_id)

    def logging_handler(self):
        return self._logging_queue_handler

    def start_logging(self, level=logging.INFO):
        for logger in self._loggers:
            logger.addHandler(self.logging_handler())
        if self._logging_listener is None:
            self._logging_listener = QueueListener(
                self._logging_queue, self._logging_db_handler())
        self._logging_listener.start()

    def stop_logging(self):
        self._logging_listener.stop()

    def get_logs(self, level=logging.INFO, limit=0):
        log_collection = self._get_logging_collection()
        log_collection.create_index('created')
        try:
            levelno = int(level)
        except ValueError:
            levelno = logging.getLevelName(level)
        spec = {'levelno': {'$gte': levelno}}
        sort = [('created', 1)]
        if limit:
            skip = max(0, log_collection.find(spec).count() - limit)
        else:
            skip = 0
        docs = log_collection.find(spec).sort(sort).skip(skip)
        for doc in docs:
            yield record_from_doc(doc)

    def find_job_ids(self, spec={}):
        if PYMONGO_3:
            docs = self._find_job_docs(spec, projection=['_id'])
        else:
            docs = self._find_job_docs(spec, fields=['_id'])
        for doc in docs:
            yield doc['_id']

    def _check_version(self):
        """Check the project version

        returns: True if the version matches, otherwise False.
        raises: UserWarning, RuntimeError

        The function will raise a UserWarning if the signac version is higher than the project version.
        The function will raise a RuntimeError if the signac version is less than the project version, because correct behaviour can't be guaranteed in this case.
        """
        version = tuple(self.config.get('signac_version', (0, 1, 0)))
        if VERSION_TUPLE < version:
            msg = "The project is configured for signac version {}, but the current signac version is {}. Update signac to use this project."
            raise RuntimeError(msg.format(version, VERSION_TUPLE))
        if VERSION_TUPLE > version:
            msg = "The project is configured for signac version {}, but the current signac version is {}. Execute `signac update` to update your project and get rid of this warning."
            raise UserWarning(msg.format(version, VERSION_TUPLE))
            warnings.warn(msg.format(version, VERSION_TUPLE))
            return False
        return True

    def check_version(self):
        """Check the project version against the signac version.

        returns: True if the version matches, otherwise False.
        raises: RuntimeError

        The function will issue a warning if the signac version is higher than the project version.
        The function will raise a RuntimeError if the signac version is less than the project version, because correct behaviour can't be guaranteed in this case.
        """
        try:
            return self._check_version()
        except UserWarning as warning:
            warnings.warn(warning)


class Project(OnlineProject):
    pass
