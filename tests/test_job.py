import unittest
import sys
import os
import io
import warnings
import tempfile
import uuid
import json
import copy
from contextlib import contextmanager

import pymongo

import signac
#from signac.contrib import get_project

# Make sure the jobs created for this test are unique.
test_token = {'test_token': str(uuid.uuid4())}

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')
warnings.filterwarnings('ignore', category=PendingDeprecationWarning, message=r'.*Cache API.*')

PYMONGO_3 = pymongo.version_tuple[0] == 3

try:
    import numpy as np
except ImportError:
    NUMPY = False
else:
    NUMPY = True

def config_from_cfg(cfg):
    cfile = io.StringIO('\n'.join(cfg))
    return signac.common.config.get_config(cfile)

def open_job(cfg, *args, **kwargs):
    config = config_from_cfg(cfg)
    project = signac.contrib.project.Project(config=config)
    return project.open_job(*args, **kwargs)

def open_offline_job(cfg, *args, **kwargs):
    config = config_from_cfg(cfg)
    project = signac.contrib.project.Project(config=config)
    return project.open_offline_job(*args, **kwargs)

def testdata():
    return str(uuid.uuid4())

class BaseJobTest(unittest.TestCase):
    
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix = 'signac_')
        self.addCleanup(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, 'pr')
        self._tmp_wd = os.path.join(self._tmp_dir.name, 'wd')
        self._tmp_fs = os.path.join(self._tmp_dir.name, 'fs')
        #print(self._tmp_pr, self._tmp_wd, self._tmp_fs)
        os.mkdir(self._tmp_pr)
        os.mkdir(self._tmp_wd)
        os.mkdir(self._tmp_fs)
        self.config = signac.common.config.load_config()
        self.config['default_host'] = 'testing'
        self.config['author'] = 'test_author'
        self.config['author_email'] = 'testauthor@example.com'
        self.config['project'] = 'testing_test_project'
        self.config['project_dir'] = self._tmp_pr
        self.config['workspace_dir'] = self._tmp_wd
        self.config['filestorage_dir'] = self._tmp_fs
        self.config['signac_version'] = signac.VERSION_TUPLE
        self.project = signac.contrib.Project(config=self.config)
        self.addCleanup(self.project.remove, force=True)

    def tearDown(self):
        pass

class OldIDJobTest(BaseJobTest):

    def setUp(self):
        os.environ['COMPDB_VERSION'] = '0.1'
        super(OldIDJobTest, self).setUp()

class BaseOnlineJobTest(BaseJobTest):

    def open_job(self, *args, **kwargs):
        project = self.project
        return project.open_job(*args, **kwargs)

class OfflineJobTest(BaseJobTest):

    def open_job(self, *args, **kwargs):
        project = self.project
        return project.open_offline_job(*args, **kwargs)

class JobTest(BaseJobTest):
    pass

class ConfigTest(OfflineJobTest):
    
    @unittest.skip("Verification is no longer this strict.")
    def test_config_verification(self):
        from signac.core.config import IllegalKeyError
        self.project.config.verify() 
        with self.assertRaises(IllegalKeyError):
            self.project.config['illegal_key'] = 'abc'
        with self.assertRaises(IllegalKeyError):
            self.project.config.update(dict(illegal_key = 'abc'))
        self.project.config.__setitem__('illegal_key', 'abc', force=True)
        # Bug in the `warnings` module prevents the usage of the following clause.
        # Possibly related to: 
        #   https://bitbucket.org/gutworth/six/issues/68/assertwarns-and-six
        #   https://code.djangoproject.com/ticket/23841
        #with self.assertWarns(UserWarning): 
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self.project.config.verify() 
        with self.assertRaises(ValueError):
            self.project.config.verify(strict=True)

    def test_set_get_delete(self):  
        key, value = list(test_token.items())[0]
        key, value = 'author_name', list(test_token.values())[0]
        config = copy.deepcopy(self.project.config)
        config[key] = value
        self.assertEqual(config[key], value)
        self.assertIn(key, config)
        del config[key]
        self.assertNotIn(key, config)

    def test_update(self):
        key, value = 'author_name', list(test_token.values())[0]
        config = copy.deepcopy(self.project.config)
        config.update({key: value})
        self.assertEqual(config[key], value)
        self.assertIn(key, config)

    @unittest.skip("Verification is no longer this strict.")
    def test_illegal_argument(self):
        from signac.core.config import IllegalArgumentError, CHOICES
        with self.assertRaises(IllegalArgumentError):
            self.project.config[list(CHOICES.keys())[0]] = 'invalid'

    @unittest.skip("Config logic changed.")
    def test_config_files_and_dirs(self):
        from signac.core.config import FILES, DIRS
        key_file = FILES[0]
        key_dir = DIRS[0]
        cwd = os.getcwd()
        try:
            with tempfile.NamedTemporaryFile() as tmp:
                head, tail = os.path.split(tmp.name)
                os.chdir(head)
                self.project.config[key_file] = tail
                self.assertEqual(os.path.abspath(tmp.name), self.project.config[key_file])
            self.project.config[key_dir] = self._tmp_dir.name
            self.assertEqual(os.path.abspath(self._tmp_dir.name), self.project.config[key_dir])
        except:
            raise
        finally:
            os.chdir(cwd)

    def test_set_and_retrieve_version(self):
        fake_version = 0,0,0
        self.project.config['signac_version'] = fake_version
        self.assertEqual(self.project.config['signac_version'], fake_version)

    def test_str(self): 
        str(self.project.config)

class OnlineConfigTest(BaseOnlineJobTest, ConfigTest):
    pass

class JobOpenAndClosingTest(OfflineJobTest):

    def test_open_job_close(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with self.open_job(test_token) as job:
                pass
            try:
                job.remove()
            except AttributeError: # not possible for offline jobs
                pass

    def test_open_job_close_manual(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            job = self.open_job(test_token)
            job.open()
            job.close()
            try:
                job.remove()
            except AttributeError: # not possible for offline jobs
                pass

    def test_open_job_close_with_error(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            job = self.open_job(test_token)
            class TestError(Exception):
                pass
            with self.assertRaises(TestError):
                with job:
                    raise TestError()
            try:
                job.remove()
            except AttributeError: # not possible for offline jobs
                pass

    def test_reopen_job(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with self.open_job(test_token) as job:
                job_id = job.get_id()
                self.assertEqual(str(job_id), str(job))

            with self.open_job(test_token) as job:
                self.assertEqual(job.get_id(), job_id)
            try:
                job.remove()
            except AttributeError:
                pass

    def test_open_flag(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with self.open_job(test_token) as job:
                self.assertTrue(os.path.isfile(job._fn_open_flag()))
            self.assertFalse(os.path.isfile(job._fn_open_flag()))

class OnlineJobOpenAndClosingTest(BaseOnlineJobTest, JobOpenAndClosingTest):
    pass
class OldIDJobOpenAndClosingTest(OldIDJobTest, JobOpenAndClosingTest):
    pass

class APIOfflineJobTest(OfflineJobTest):

    def test_access_online_job_properties(self):
        with self.open_job(test_token) as job:
            with self.assertRaises(AttributeError):
                job.collection

class OnlineJobTest(BaseOnlineJobTest):
    
    def test_remove(self):  
        with self.open_job(test_token) as test_job:
            with self.assertRaises(RuntimeError):
                test_job.remove()
        test_job.remove()

    def test_remove_force(self):
        with self.open_job(test_token) as test_job:
            with self.assertRaises(RuntimeError):
                test_job.remove()
            # the next command will corrupt the database, as 
            # we ignore the warning, that there is an open instance
            # and we should not remove the job.
            test_job.remove(force=True)
        return
        # to recover from the corruption we simply open the job again
        with self.open_job(test_token) as test_job:
            pass 
        test_job.remove()

class JobStorageTest(BaseOnlineJobTest):
    
    def test_store_and_get(self):
        key = 'my_test_key'
        value = uuid.uuid4()
        with self.open_job(test_token) as test_job:
            test_job.document[key] = value
            self.assertTrue(key in test_job.document)
            self.assertEqual(test_job.document[key], value)
            self.assertIsNotNone(test_job.document.get(key))
            self.assertEqual(test_job.document.get(key), value)

        with self.open_job(test_token) as test_job:
            self.assertTrue(key in test_job.document)
            self.assertIsNotNone(test_job.document.get(key))
            self.assertEqual(test_job.document.get(key), value)
            self.assertEqual(test_job.document[key], value)
        try:
            test_job.remove()
        except AttributeError:
            pass

    def test_store_and_retrieve_value_in_job_collection(self):
        doc = {'a': uuid.uuid4()}
        with self.open_job(test_token) as test_job:
            if PYMONGO_3:
                test_job.collection.insert_one(doc)
            else:
                test_job.collection.save(doc)
            job_id = test_job.get_id()

        with self.open_job(test_token) as job:
            self.assertEqual(job.get_id(), job_id)
            self.assertIsNotNone(job.collection.find_one(doc))
        try:
            test_job.remove()
        except AttributeError:
            pass

    def test_open_file(self):
        data = str(uuid.uuid4())

        with self.open_job(test_token) as job:
            with job.storage.open_file('_my_file', 'wb') as file:
                file.write(data.encode())

            with job.storage.open_file('_my_file', 'rb') as file:
                read_back = file.read().decode()

            job.storage.remove_file('_my_file')
        self.assertEqual(data, read_back)
        try:
            job.remove()
        except AttributeError:
            pass

    def test_store_and_restore_file(self):
        data = str(uuid.uuid4())
        fn = '_my_file'

        with self.open_job(test_token) as job:
            with open(fn, 'wb') as file:
                file.write(data.encode())
            self.assertTrue(os.path.exists(fn))
            job.storage.store_file(fn)
            self.assertFalse(os.path.exists(fn))
            job.storage.restore_file(fn)
            self.assertTrue(os.path.exists(fn))
            with open(fn, 'rb') as file:
                read_back = file.read().decode()
        self.assertEqual(data, read_back)
        try:
            job.remove()
        except AttributeError:
            pass

    def test_store_all_and_restore_all(self):
        data = str(uuid.uuid4())
        fns = ('_my_file', '_my_second_file')

        with self.open_job(test_token) as job:
            for fn in fns:
                with open(fn, 'wb') as file:
                    file.write(data.encode())
                self.assertTrue(os.path.exists(fn))
            job.storage.store_files()
            for fn in fns:
                self.assertFalse(os.path.exists(fn))
            job.storage.restore_files()
            for fn in fns:
                self.assertTrue(os.path.exists(fn))
                with open(fn, 'rb') as file:
                    read_back = file.read().decode()
                self.assertEqual(data, read_back)
        try:
            job.remove()
        except AttributeError:
            pass

    def test_job_clearing(self):
        data = str(uuid.uuid4())
        doc = {'a': uuid.uuid4()}

        with self.open_job(test_token) as job:
            with job.storage.open_file('_my_file', 'wb') as file:
                file.write(data.encode())
            if PYMONGO_3:
                job.collection.insert_one(doc)
            else:
                job.collection.save(doc)
            
        with self.open_job(test_token) as job:
            with job.storage.open_file('_my_file', 'rb') as file:
                read_back = file.read().decode()
            self.assertEqual(data, read_back)
            self.assertIsNotNone(job.collection.find_one(doc))
            job.clear()
            with self.assertRaises(IOError):
                job.storage.open_file('_my_file', 'rb')
            self.assertIsNone(job.collection.find_one(doc))
        try:
            job.remove()
        except AttributeError:
            pass

def open_and_lock_and_release_job(cfg, token):
    with open_job(cfg, test_token, timeout = 30) as job:
        pass
    return True

class JobConcurrencyTest(BaseOnlineJobTest):

    def test_recursive_job_opening(self):
        project = self.project
        with project.open_job(test_token, timeout = 1) as job0:
            self.assertEqual(job0.num_open_instances(), 1)
            self.assertTrue(job0.is_exclusive_instance())
            with project.open_job(test_token, timeout = 1) as job1:
                self.assertEqual(job0.num_open_instances(), 2)
                self.assertFalse(job0.is_exclusive_instance())
                self.assertEqual(job1.num_open_instances(), 2)
                self.assertFalse(job1.is_exclusive_instance())
            self.assertEqual(job0.num_open_instances(), 1)
            self.assertTrue(job0.is_exclusive_instance())
            self.assertEqual(job1.num_open_instances(), 1)
            self.assertTrue(job1.is_exclusive_instance())
        self.assertEqual(job0.num_open_instances(), 0)
        self.assertTrue(job0.is_exclusive_instance())
        self.assertEqual(job1.num_open_instances(), 0)
        self.assertTrue(job1.is_exclusive_instance())
        job0.remove()

    def test_acquire_and_release(self):
        from signac.contrib.concurrency import DocumentLockError
        with self.project.open_job(test_token, timeout = 1) as job:
            with job.lock(timeout = 1):
                def lock_it():
                    with job.lock(blocking=False):
                        pass
                self.assertRaises(DocumentLockError, lock_it)
        job.remove()

    def test_process_concurrency(self):
        from multiprocessing import Pool

        num_processes = 100
        num_locks = 10
        try:
            with Pool(processes = num_processes) as pool:
                result = pool.starmap_async(
                    open_and_lock_and_release_job,
                    [(self.config.write(), test_token) for i in range(num_locks)])
                result = result.get(timeout = 60)
                self.assertEqual(result, [True] * num_locks)
        except Exception:
            raise
        finally:
            # clean up
            with self.project.open_job(test_token, timeout = 60) as job:
                pass
            job.remove(force = True)

class TestJobImport(BaseOnlineJobTest):

    def test_import_document(self):
        test_token_clone = dict(test_token)
        test_token_clone['clone'] = True
        key = 'my_test_key'
        value = uuid.uuid4()
        with self.open_job(test_token) as test_job:
            test_job.document[key] = value
            self.assertTrue(key in test_job.document)
            self.assertEqual(test_job.document[key], value)
            self.assertIsNotNone(test_job.document.get(key))
            self.assertEqual(test_job.document.get(key), value)
            with self.open_job(test_token_clone) as job_clone:
                job_clone.import_job(test_job)
                self.assertTrue(key in job_clone.document)
                self.assertEqual(job_clone.document[key], value)
                self.assertIsNotNone(job_clone.document.get(key))
                self.assertEqual(job_clone.document.get(key), value)

    def test_import_files(self):
        data = str(uuid.uuid4())
        fn = '_my_file'
        test_token_clone = dict(test_token)
        test_token_clone['clone'] = True

        with self.open_job(test_token) as job:
            with open(fn, 'wb') as file:
                file.write(data.encode())
            self.assertTrue(os.path.exists(fn))
            job.storage.store_file(fn)
            self.assertFalse(os.path.exists(fn))
            with self.open_job(test_token_clone) as job_clone:
                job_clone.import_job(job)
                job_clone.storage.restore_file(fn)
                self.assertTrue(os.path.exists(fn))
                with open(fn, 'rb') as file:
                    read_back = file.read().decode()
        self.assertEqual(data, read_back)

    def test_import_collection(self):
        test_token_clone = dict(test_token)
        test_token_clone['clone'] = True
        key = 'my_test_key'
        value = uuid.uuid4()
        doc = {key: value}
        with self.open_job(test_token) as job:
            if PYMONGO_3:
                job.collection.insert_one(doc)
            else:
                job.collection.save(doc)
            with self.open_job(test_token_clone) as job_clone:
                job_clone.import_job(job)
                doc_check = job_clone.collection.find_one()
                self.assertIsNotNone(doc_check)
                self.assertEqual(doc, doc_check)

class OfflineJobDocumentTest(OfflineJobTest):

    def test_get_set(self):
        key = 'get_set'
        d = testdata()
        job = self.open_job(test_token)
        self.assertFalse(bool(job.document))
        self.assertEqual(len(job.document), 0)
        self.assertNotIn(key, job.document)
        job.document[key] = d
        self.assertTrue(bool(job.document))
        self.assertEqual(len(job.document), 1)
        self.assertIn(key, job.document)
        self.assertEqual(job.document[key], d)
        self.assertEqual(job.document.get(key), d)
        self.assertEqual(job.document.get('bs', d), d)

    def test_update(self):
        key = 'get_set'
        d = testdata()
        job = self.open_job(test_token)
        job.document.update({key: d})
        self.assertIn(key, job.document)

    def test_clear(self):
        key = 'clear'
        d = testdata()
        job = self.open_job(test_token)
        job.document[key] = d
        self.assertIn(key, job.document)
        self.assertEqual(len(job.document), 1)
        job.document.clear()
        self.assertNotIn(key, job.document)
        self.assertEqual(len(job.document), 0)

    def test_reopen(self):
        key = 'clear'
        d = testdata()
        job = self.open_job(test_token)
        job.document[key] = d
        self.assertIn(key, job.document)
        self.assertEqual(len(job.document), 1)
        job2 = self.open_job(test_token)
        self.assertIn(key, job2.document)
        self.assertEqual(len(job2.document), 1)

class OfflineOnlineSynchronizeDocumentTest(BaseJobTest):

    def test_synchronization(self):
        key = 'sync1'
        d = testdata()
        offline_job = open_offline_job(self.project.config.write(), test_token)
        offline_job.document[key] = d
        self.assertIn(key, offline_job.document)
        self.assertEqual(len(offline_job.document), 1)
        online_job = open_job(self.project.config.write(), test_token)
        self.assertNotIn(key, online_job.document)
        online_job.load_document()
        self.assertIn(key, online_job.document)
        key2 = 'sync2'
        d2 = testdata()
        online_job.document[key2] = d2
        self.assertIn(key2, online_job.document)
        online_job.save_document()
        self.assertIn(key, offline_job.document)
        self.assertIn(key2, offline_job.document)

if __name__ == '__main__':
    unittest.main()
