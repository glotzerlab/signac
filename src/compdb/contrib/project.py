import logging
logger = logging.getLogger('compdb.project')
from queue import Queue
#from multiprocessing import Queue
from logging.handlers import QueueHandler, QueueListener

JOB_PARAMETERS_KEY = 'parameters'
JOB_NAME_KEY = 'name'
JOB_DOCS = 'compdb_job_docs'
JOB_META_DOCS = 'compdb_jobs'
from . job import PULSE_PERIOD

FN_DUMP_JOBS = 'compdb_jobs.json'
FN_DUMP_STORAGE = 'storage'
FN_DUMP_DB = 'dump'
FN_RESTORE_SCRIPT_SH = 'restore.sh'
FN_DUMP_FILES = [FN_DUMP_JOBS, FN_DUMP_STORAGE, FN_DUMP_DB, FN_RESTORE_SCRIPT_SH]
FN_STORAGE_BACKUP = '_fs_backup'

COLLECTION_LOGGING = 'logging'
COLLECTION_JOB_QUEUE = 'compdb_job_queue'
COLLECTION_JOB_QUEUE_RESULTS = 'compdb_job_results'

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

from ..core.mongodb_queue import Empty

def valid_name(name):
    return not name.startswith('compdb')

class RollBackupExistsError(RuntimeError):
    pass

class Project(object):
    
    def __init__(self, config = None):
        if config is None:
            from compdb.core.config import load_config
            config = load_config()
        config.verify()
        self._config = config
        self._loggers = [logging.getLogger('compdb')]
        self._logging_queue = Queue()
        self._logging_queue_handler = QueueHandler(self._logging_queue)
        self._logging_listener = None
        self._client = None
        self._job_queue = None

    def __str__(self):
        return self.get_id()

    @property 
    def config(self):
        return self._config

    def root_directory(self):
        return self._config['project_dir']

    def _get_client(self):
        if self._client is None:
            from ..core.dbclient_connector import DBClientConnector
            prefix = 'database_'
            connector = DBClientConnector(self.config, prefix = prefix)
            logger.debug("Connecting to database.")
            try:
                connector.connect()
                connector.authenticate()
            except:
                logger.error("Connection failed.")
                raise
            else:
                logger.debug("Connected and authenticated.")
            self._client = connector.client
        return self._client

    def _get_db(self, db_name):
        import pymongo.errors
        host = self.config['database_host']
        try:
            return self._get_client()[db_name]
        except pymongo.errors.ConnectionFailure as error:
            from . errors import ConnectionFailure
            msg = "Failed to connect to database '{}' at '{}'."
            #logger.error(msg.format(db_name, host))
            raise ConnectionFailure(msg.format(db_name, host)) from error

    def get_db(self, db_name = None):
        if db_name is None:
            return self.get_project_db()
        else:
            assert valid_name(db_name)
            return self._get_db(db_name)

    def _get_meta_db(self):
        return self._get_db(self.config['database_meta'])

    def get_jobs_collection(self):
        return self._get_meta_db()[JOB_META_DOCS]

    def get_id(self):
        try:
            return self.config['project']
        except KeyError:
            import os
            msg = "Unable to determine project id. "
            msg += "Are you sure '{}' is a compDB project path?"
            raise LookupError(msg.format(os.path.realpath(os.getcwd())))
    def get_project_db(self):
        return self.get_db(self.get_id())
    
    @property
    def collection(self):
        return self.get_project_db()[JOB_DOCS]

    def filestorage_dir(self):
        return self.config['filestorage_dir']

    def _workspace_dir(self):
        return self.config['workspace_dir']

    def remove(self, force = False):
        import pymongo.errors
        self.get_cache().clear()
        for job in self.find_jobs():
            job.remove(force = force)
        try:
            host = self.config['database_host']
            client = self._get_client()
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
        msg = "{}: Activating develop mode!"
        logger.warning(msg.format(self.get_id()))
        self.config['develop'] = True

    def _job_spec(self, parameters):
        spec = dict()
        #if not len(parameters):
        #    msg = "Parameters dictionary cannot be empty!"
        #    raise ValueError(msg)
        if parameters is None:
            parameters = {}
        spec.update({JOB_PARAMETERS_KEY: parameters})
        if self.develop_mode():
            spec.update({'develop': True})
        spec.update({
            'project': self.get_id(),
        })
        return spec

    def get_job(self, job_id, blocking = True, timeout = -1):
        from . job import Job
        return Job(
            project = self,
            spec = {'_id': job_id},
            blocking = blocking, timeout = timeout)

    def _open_job(self, spec, blocking = True, timeout = -1, rank = 0):
        from . job import Job
        return Job(
            project = self,
            spec = spec,
            blocking = blocking,
            timeout = timeout,
            rank = rank)

    def open_job(self, parameters = None, blocking = True, timeout = -1, rank = 0):
        spec = self._job_spec(parameters = parameters)
        return self._open_job(
            spec = spec,
            blocking = blocking,
            timeout = timeout,
            rank = rank)

    def _job_spec_modifier(self, job_spec = {}, develop = None):
        from copy import copy
        job_spec_ = copy(job_spec)
        if 'project' in job_spec_:
            raise ValueError("You cannot provide a value for 'project' using this search method.")
        job_spec_.update({'project': self.get_id()})
        if develop or (develop is None and self.develop_mode()):
            job_spec_.update({'develop': True})
        return job_spec_

    def _find_jobs(self, job_spec, * args, **kwargs):
        yield from self.get_jobs_collection().find(
            self._job_spec_modifier(job_spec), * args, ** kwargs)

    def find_job_ids(self, spec = {}):
        if PYMONGO_3:
            docs = self._find_jobs(spec, projection = ['_id'])
        else:
            docs = self._find_jobs(spec, fields = ['_id'])
        for doc in docs:
            yield doc['_id']
    
    def find_jobs(self, job_spec = None, spec = None, blocking = True, timeout = -1):
        if job_spec is None:
            job_spec = {}
        else:
            job_spec = {'parameters.{}'.format(k): v for k,v in job_spec.items()}
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
        spec = {
            'executing': {'$exists': True},
            '$where': 'this.executing.length > 0'}
        yield from self.find_job_ids(spec)

    def _unique_jobs_from_pulse(self):
        docs = self.get_jobs_collection().find(
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
            doc = self.get_jobs_collection().find_one(
                {hb_key: {'$exists': True}})
            yield uid, doc['pulse'][uid]

    def kill_dead_jobs(self, seconds = 5 * PULSE_PERIOD):
        import datetime
        from datetime import datetime, timedelta
        cut_off = datetime.utcnow() - timedelta(seconds = seconds)
        uids = self._unique_jobs_from_pulse()
        for uid in uids:
            hbkey = 'pulse.{}'.format(uid)
            doc = self.get_jobs_collection().update(
                #{'pulse.{}'.format(uid): {'$exists': True},
                {hbkey: {'$lt': cut_off}},
                {   '$pull': {'executing': uid},
                    '$unset': {hbkey: ''},
                })

    def _aggregate_parameters(self, job_spec = {}):
        pipe = [
            {'$match': self._job_spec_modifier(job_spec)},
            {'$group': {
                '_id': False,
                'parameters': { '$addToSet': '$parameters'}}},
            ]
        result = self.get_jobs_collection().aggregate(pipe)
        if PYMONGO_3:
            return set([k for r in result for p in r['parameters'] for k in p.keys()])
        else:
            assert result['ok']
            if len(result['result']):
                return set([k for r in result['result'][0]['parameters'] for k in r.keys()])
            else:
                return set()

    def _walk_jobs(self, parameters, job_spec = {}, * args, ** kwargs):
        yield from self._find_jobs(
            job_spec,
            sort = [('parameters.{}'.format(p), 1) for p in parameters],
            * args, ** kwargs)
    
    def _get_links(self, url, parameters, fs):
        import os
        walk = self._walk_jobs(parameters)
        for w in walk:
            src = os.path.join(fs, w['_id'])
            try:
                from collections import defaultdict
                dd = defaultdict(lambda: 'None')
                dd.update(w['parameters'])
                dst = url.format(** dd)
            except KeyError as error:
                msg = "Unknown parameter: {}"
                raise KeyError(msg.format(error)) from error
            yield src, dst

    def get_storage_links(self, url = None):
        import re
        if url is None:
            url = self.get_default_view_url()
        parameters = re.findall('\{\w+\}', url)
        yield from self._get_links(
            url, parameters, self.filestorage_dir())

    def get_workspace_links(self, url = None):
        import re
        if url is None:
            url = self.get_default_view_url()
        parameters = re.findall('\{\w+\}', url)
        yield from self._get_links(
            url, parameters, self.workspace_dir())

    def get_default_view_url(self):
        import os
        from itertools import chain
        params = sorted(self._aggregate_parameters())
        if len(params):
            return str(os.path.join(* chain.from_iterable(
                (str(p), '{'+str(p)+'}') for p in params)))
        else:
            return str()

    def create_view(self, url = None, copy = False, workspace = False):
        import os, re, shutil
        if url is None:
            url = self.get_default_view_url()
        parameters = re.findall('\{\w+\}', url)
        if workspace:
            links = self._get_links(url, parameters, self._workspace_dir())
        else:
            links = self._get_links(url, parameters, self.filestorage_dir())
        for src, dst in links:
            try:
                os.makedirs(os.path.dirname(dst))
            except FileExistsError:
                pass
            try:
                if copy:
                    shutil.copytree(src, dst)
                else:
                    os.symlink(src, dst, target_is_directory = True)
            except FileExistsError as error:
                msg = "Failed to create view for url '{url}'. "
                msg += "Possible causes: A view with the same path exists already or you are not using enough parameters for uniqueness."
                raise RuntimeError(msg)

    def create_view_script(self, url = None, cmd = None, fs = None):
        import os, re
        if cmd is None:
            cmd = 'mkdir -p {head}\nln -s {src} {head}/{tail}'
        if url is None:
            url = self.get_default_view_url()
        parameters = re.findall('\{\w+\}', url)
        for src, dst in self._get_links(url, parameters, fs):
            head, tail = os.path.split(dst)
            yield cmd.format(src = src, head = head, tail = tail)

    def dump_db_snapshot(self):
        from bson.json_util import dumps
        spec = self._job_spec_modifier(develop = False)
        job_docs = self.get_jobs_collection().find(spec)
        for doc in job_docs:
            print(dumps(doc))
        docs = self.find()
        for doc in docs:
            print(dumps(doc))

    def _create_db_snapshot(self, dst):
        import os
        from bson.json_util import dumps
        from . utility import dump_db_from_config
        spec = self._job_spec_modifier(develop = False)
        job_docs = self.get_jobs_collection().find(spec)
        docs = self.collection.find()
        fn_dump_jobs = os.path.join(dst, FN_DUMP_JOBS)
        with open(fn_dump_jobs, 'wb') as file:
            for job_doc in job_docs:
                file.write("{}\n".format(dumps(job_doc)).encode())
        fn_dump_db = os.path.join(dst, FN_DUMP_DB)
        dump_db_from_config(self.config, fn_dump_db)
        return [fn_dump_jobs, fn_dump_db]

    def _create_config_snapshot(self, dst):
        from os.path import join
        fn_config = join(dst, 'compdb.rc')
        self.config.write(fn_config)
        return [fn_config]

    def _create_restore_scripts(self, dst):
        import os
        from . templates import RESTORE_SH
        fn_restore_script_sh = os.path.join(dst, FN_RESTORE_SCRIPT_SH)
        with open(fn_restore_script_sh, 'wb') as file:
            file.write("#/usr/bin/env sh\n# -*- coding: utf-8 -*-\n".encode())
            file.write(RESTORE_SH.format(
                project = self.get_id(),
                db_host = self.config['database_host'],
                fs_dir = self.filestorage_dir(),
                db_meta = self.config['database_meta'],
                compdb_docs = JOB_META_DOCS,
                compdb_job_docs = JOB_DOCS,
                sn_storage_dir= FN_DUMP_STORAGE,
                    ).encode())
        return [fn_restore_script_sh]

    def _create_config_snapshot(self, dst):
        from os.path import join
        fn_config = join(dst, 'compdb.rc')
        self.config.write(fn_config)
        return [fn_config]

    def _create_snapshot_view_script(self, dst):
        from os.path import join
        fn_script = join(dst, 'link_view.sh')
        with open(fn_script, 'wb') as file:
            file.write("CMD_LINK='ln -s'\n".encode())
            file.write("CMD_MKDIR='mkdir -p'\n".encode())
            cmd = "$CMD_MKDIR view/{head}\n$CMD_LINK {src} view/{head}/{tail}\n"
            for line in self.create_view_script(
                cmd = cmd, fs = FN_DUMP_STORAGE):
                file.write(line.encode())
        return [fn_script]

    def _check_snapshot(self, src):
        from os.path import isfile, isdir
        import tarfile
        # TODO Implement check of content routine!
        if isfile(src):
            with tarfile.open(src, 'r') as tarfile:
                pass
        elif isdir(src):
            pass
        else:
            raise FileNotFoundError(src)

    def _restore_snapshot_from_src(self, src, force = False):
        import shutil, os
        from os.path import join, isdir, dirname, exists
        from bson.json_util import loads
        from . utility import restore_db_from_config
        fn_storage = join(src, FN_DUMP_STORAGE)
        try:
            with open(join(src, FN_DUMP_JOBS), 'rb') as file:
                for job in self.find_jobs():
                    job.remove()
                for line in file:
                    job_doc = loads(line.decode())
                    assert job_doc['project'] == self.get_id()
                    self.get_jobs_collection().save(job_doc)
        except FileNotFoundError as error:
            logger.warning(error)
            if not force:
                raise
        try:
            fn_dump_db = join(src, FN_DUMP_DB, self.get_id())
            if not exists(fn_dump_db):
                raise NotADirectoryError(fn_dump_db)
            restore_db_from_config(self.config, fn_dump_db)
        except NotADirectoryError as error:
            logger.warning(error)
            if not force:
                raise
        for root, dirs, files in os.walk(fn_storage):
            for dir in dirs:
                try:
                    shutil.rmtree(join(self.filestorage_dir(), dir))
                except (FileNotFoundError, IsADirectoryError):
                    pass
                shutil.move(join(root, dir), self.filestorage_dir())
            assert exists(join(self.filestorage_dir(), dir))
            break
    
    def _restore_snapshot(self, src):
        from os.path import isfile, isdir, join
        import tarfile
        from tempfile import TemporaryDirectory
        if isfile(src):
            with TemporaryDirectory() as tmp_dir:
                with tarfile.open(src, 'r') as tarfile:
                    tarfile.extractall(tmp_dir)
                    self._restore_snapshot_from_src(tmp_dir)
        elif isdir(src):
            self._restore_snapshot_from_src(src)
        else:
            raise FileNotFoundError(src)

    def _create_snapshot(self, dst, full = True, mode = 'w'):
        import os
        import tarfile
        from tempfile import TemporaryDirectory
        from itertools import chain
        with TemporaryDirectory(prefix = 'compdb_dump_') as tmp:
            try:
                with tarfile.open(dst, mode) as tarfile:
                    for fn in chain(
                            self._create_db_snapshot(tmp),
                            self._create_restore_scripts(tmp),
                            self._create_config_snapshot(tmp),
                            self._create_snapshot_view_script(tmp)):
                        logger.debug("Storing '{}'...".format(fn))
                        tarfile.add(fn, os.path.relpath(fn, tmp))
                    if full:
                        for id_ in self.find_job_ids():
                            src_ = os.path.join(self.filestorage_dir(), id_)
                            dst_ = os.path.join(FN_DUMP_STORAGE, id_)
                            tarfile.add(src_, dst_)
            except Exception:
                os.remove(dst)
                raise

    def create_snapshot(self, dst, full = True):
        import os
        fn, ext = os.path.splitext(dst)
        mode = 'w:'
        if ext in ['.gz', '.bz2']:
            mode += ext[1:]
        return self._create_snapshot(dst, full = full, mode = mode)

    def _create_rollbackup(self, dst):
        import os, shutil
        from os.path import join
        logger.debug("Creating rollback backup...")
        os.mkdir(dst)
        fn_db_backup = os.path.join(dst, 'db_backup.tar')
        fn_storage_backup = join(dst, FN_STORAGE_BACKUP)

        self.create_snapshot(fn_db_backup, full = False)
        for job_id in self.find_job_ids():
            try:
                shutil.move(
                    os.path.join(self.filestorage_dir(), job_id), 
                    os.path.join(fn_storage_backup, job_id))
            except FileNotFoundError as error:
                pass
    
    def _restore_rollbackup(self, dst):
        import os, shutil
        from os.path import join
        fn_storage_backup = join(dst, FN_STORAGE_BACKUP)
        fn_db_backup = os.path.join(dst, 'db_backup.tar')

        for root, dirs, files in os.walk(fn_storage_backup):
            for dir in dirs:
                try:
                    shutil.rmtree(join(self.filestorage_dir(), dir))
                except (FileNotFoundError, IsADirectoryError):
                    pass
                shutil.move(join(root, dir), self.filestorage_dir())
                self._restore_snapshot(fn_db_backup)
    
    def _remove_rollbackup(self, dst):
        import shutil
        shutil.rmtree(dst)

    def restore_snapshot(self, src):
        import os, shutil
        from tempfile import TemporaryDirectory
        from os.path import join
        logger.info("Trying to restore from '{}'.".format(src))
        num_active = len(list(self.active_jobs()))
        if num_active > 0:
            msg = "This project has indication of active jobs. "
            msg += "You can use 'compdb cleanup' to remove dead jobs."
            raise RuntimeError(msg.format(num_active))
        self._check_snapshot(src)
        dst_rollbackup = join(
            self._workspace_dir(), 
            'restore_rollback_{}'.format(self.get_id()))
        try:
            self._create_rollbackup(dst_rollbackup)
        except FileExistsError as error:
            raise RollBackupExistsError(dst_rollbackup)
        except BaseException as error:
            try:
                self._remove_rollbackup(str(error))
            except FileNotFoundError:
                pass
            raise
        else:
            try:
                self._restore_snapshot(src)
            except BaseException as error:
                if type(error) == KeyboardInterrupt:
                    print("Interrupted.")
                else:
                    msg = "Error during restore from '{src}': {error}."
                    logger.error(msg.format(src = src, error = error))
                logger.info("Restoring previous state...")
                try:
                    self._restore_rollbackup(dst_rollbackup)
                except:
                    msg = "Failed to rollback!"
                    logger.critical(msg)
                    raise
                else:
                    logger.info("Rolled back.")
                    self._remove_rollbackup(dst_rollbackup)
                raise
            else:
                self._remove_rollbackup(dst_rollbackup)
                logger.info("Restored snapshot '{}'.".format(src))

    def job_pool(self, parameter_set, include = None, exclude = None):
        from . job_pool import JobPool
        from copy import copy
        return JobPool(self, parameter_set, copy(include), copy(exclude))

    def _get_logging_collection(self):
        return self.get_project_db()[COLLECTION_LOGGING]

    def clear_logs(self):
        self._get_logging_collection().drop()

    def _logging_db_handler(self, lock_id = None):
        from . logging import MongoDBHandler
        return MongoDBHandler(
            collection = self._get_logging_collection(),
            lock_id = lock_id)

    def logging_handler(self):
        return self._logging_queue_handler

    def start_logging(self, level = logging.INFO):
        for logger in self._loggers:
            logger.addHandler(self.logging_handler())
        if self._logging_listener is None:
            self._logging_listener = QueueListener(
                self._logging_queue, self._logging_db_handler())
        self._logging_listener.start()

    def stop_logging(self):
        self._logging_listener.stop()

    def get_logs(self, level = logging.INFO, limit = 0):
        from . logging import record_from_doc
        log_collection = self._get_logging_collection()
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

    @property
    def job_queue(self):
        if self._job_queue is None:
            from ..core.mongodb_executor import MongoDBExecutor
            from ..core.mongodb_queue import MongoDBQueue
            mongodb_queue = MongoDBQueue(self.get_project_db()[COLLECTION_JOB_QUEUE])
            collection_job_results = self.get_project_db()[COLLECTION_JOB_QUEUE_RESULTS]
            self._job_queue = MongoDBExecutor(mongodb_queue, collection_job_results)
        return self._job_queue
