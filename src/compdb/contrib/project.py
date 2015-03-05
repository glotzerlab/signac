import logging
logger = logging.getLogger('project')

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

def valid_name(name):
    return not name.startswith('_compdb')

class Project(object):
    
    def __init__(self, config = None):
        if config is None:
            from compdb.core.config import load_config
            config = load_config()
        config.verify()
        self._config = config

    def __str__(self):
        try:
            return self.get_id()
        except KeyError:
            import os
            msg = "Unable to determine project id. "
            msg += "Are you sure '{}' is a compDB project path?"
            raise LookupError(msg.format(os.path.realpath(os.getcwd())))

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
        return self._get_meta_db()[JOB_META_DOCS]

    def get_id(self):
        return self.config['project']

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
        from pymongo import MongoClient
        import pymongo.errors
        self.get_cache().clear()
        for job in self.find_jobs():
            job.remove(force = force)
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
        msg = "{}: Activating develop mode!"
        logger.warning(msg.format(self.get_id()))
        self.config['develop'] = True

    def _job_spec(self, name, parameters):
        spec = {}
        #if name is not None:
        #    spec.update({JOB_NAME_KEY: name})
        if parameters is not None:
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
        assert result['ok']
        return set([k for r in result['result'][0]['parameters'] for k in r.keys()])

    def _walk_jobs(self, parameters, job_spec = {}, * args, ** kwargs):
        yield from self._find_jobs(
            job_spec,
            sort = [('parameters.{}'.format(p), 1) for p in parameters],
            * args, ** kwargs)
    
    def _get_links(self, url, parameters):
        import os
        fs = self.filestorage_dir()
        walk = self._walk_jobs(parameters)
        for w in walk:
            src = os.path.join(fs, w['_id'])
            try:
                dst = url.format(** w['parameters'])
            except KeyError as error:
                msg = "Unknown parameter: {}"
                raise KeyError(msg.format(error)) from error
            yield src, dst

    def get_default_view_url(self):
        import os
        from itertools import chain
        params = sorted(self._aggregate_parameters())
        return str(os.path.join(* chain.from_iterable(
            (str(p), '{'+str(p)+'}') for p in params)))

    def create_view(self, url = None, copy = False):
        import os, re, shutil
        if url is None:
            url = self.get_default_view_url()
        parameters = re.findall('\{\w+\}', url)
        links = self._get_links(url, parameters)
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
                msg = "Failed to create view for url '{url}'. Are you using enough parameters for uniqueness?"
                raise RuntimeError(msg)

    def create_view_script(self, url, cmd = 'ln -s'):
        for src, dst in self._get_links(url):
            yield('{cmd} {src} {dst}'.format(
                cmd = cmd, src = src, dst = dst))

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
        from . utility import dump_db
        spec = self._job_spec_modifier(develop = False)
        job_docs = self.get_jobs_collection().find(spec)
        docs = self.collection.find()
        fn_dump_jobs = os.path.join(dst, FN_DUMP_JOBS)
        with open(fn_dump_jobs, 'wb') as file:
            for job_doc in job_docs:
                file.write("{}\n".format(dumps(job_doc)).encode())
        fn_dump_db = os.path.join(dst, FN_DUMP_DB)
        dump_db(
            host = self.config['database_host'],
            database = self.get_id(),
            dst = fn_dump_db)
        return [fn_dump_jobs, fn_dump_db]

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

    def _restore_snapshot_from_src(self, src, force = False):
        import shutil, os
        from os.path import join, isdir, dirname, exists
        from bson.json_util import loads
        from . utility import restore_db
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
            restore_db(
                host = self.config['database_host'],
                database = self.get_id(), 
                src = fn_dump_db)
        except NotADirectoryError as error:
            logger.warning(error)
            if not force:
                raise
        for root, dirs, files in os.walk(fn_storage):
            for dir in dirs:
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
                #with TarFile(src, 'r') as tarfile:
                    # TODO Implement check of content routine!
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
                            self._create_config_snapshot(tmp)):
                        logger.debug("Storing '{}'...".format(fn))
                        tarfile.add(fn, os.path.relpath(fn, tmp))
                    if full:
                        tarfile.add(
                            self.filestorage_dir(), FN_DUMP_STORAGE,
                            exclude = lambda fn: str(fn) == FN_STORAGE_BACKUP)
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

    def restore_snapshot(self, src):
        import os, shutil
        from tempfile import TemporaryDirectory
        from os.path import join
        num_active = len(list(self.active_jobs()))
        if num_active > 0:
            msg = "This project has indication of active jobs. "
            msg += "You can use 'compdb cleanup' to remove dead jobs."
            raise RuntimeError(msg.format(num_active))
        with TemporaryDirectory() as tmp_dir:
            fn_rollback = os.path.join(tmp_dir, 'rollback.tar')
            fn_storage_backup = os.path.join(
                self.filestorage_dir(), FN_STORAGE_BACKUP)
            rollback_backup_created = False
            try:
                logger.info("Trying to restore from '{}'.".format(src))
                logger.debug("Creating rollback backup...")
                self.create_snapshot(fn_rollback, full = False)
                for job_id in self.find_job_ids():
                    try:
                        shutil.move(
                            os.path.join(self.filestorage_dir(), job_id), os.path.join(fn_storage_backup, job_id))
                    except FileNotFoundError as error:
                        pass
                rollback_backup_created = True
                self._restore_snapshot(src)
            except Exception as error:
                msg = "Error during restore from '{src}': {error}."
                logger.error(msg.format(src = src, error = error))
                if rollback_backup_created:
                    try:
                        logger.info("Restoring previous state...")
                        for root, dirs, files in os.walk(fn_storage_backup):
                            for dir in dirs:
                                shutil.move(join(root, dir), self.filestorage_dir())
                        self._restore_snapshot(fn_rollback)
                    except:
                        msg = "Failed to rollback!"
                        logger.critical(msg)
                        raise
                    else:
                        logger.debug("Rolled back.")
                raise error
            else:
                try:
                    shutil.rmtree(fn_storage_backup)
                except FileNotFoundError:
                    pass
                logger.info("Restored snapshot '{}'.".format(src))

    def job_pool(self, parameter_set, exclude = None):
        from . job_pool import JobPool
        from copy import copy
        return JobPool(self, parameter_set, copy(exclude))
