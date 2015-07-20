import unittest
import sys
import os
import warnings
import tempfile
import uuid
import json
import copy
from contextlib import contextmanager

import pymongo

import compdb
from compdb.contrib import get_project

# Make sure the jobs created for this test are unique.
test_token = {'test_token': str(uuid.uuid4())}

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='compdb')
warnings.filterwarnings('ignore', category=PendingDeprecationWarning, message=r'.*Cache API.*')

PYMONGO_3 = pymongo.version_tuple[0] == 3

try:
    import numpy as np
except ImportError:
    NUMPY = False
else:
    NUMPY = True

def open_job(*args, **kwargs):
    project = get_project()
    return project.open_job(*args, **kwargs)

class BaseJobTest(unittest.TestCase):
    
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix = 'compdb_')
        self._tmp_pr = os.path.join(self._tmp_dir.name, 'pr')
        self._tmp_wd = os.path.join(self._tmp_dir.name, 'wd')
        self._tmp_fs = os.path.join(self._tmp_dir.name, 'fs')
        #print(self._tmp_pr, self._tmp_wd, self._tmp_fs)
        os.mkdir(self._tmp_pr)
        os.mkdir(self._tmp_wd)
        os.mkdir(self._tmp_fs)
        os.environ['COMPDB_AUTHOR_NAME'] = 'compdb_test_author'
        os.environ['COMPDB_AUTHOR_EMAIL'] = 'testauthor@example.com'
        os.environ['COMPDB_PROJECT'] = 'testing_compdb_test_project'
        os.environ['COMPDB_PROJECT_DIR'] = self._tmp_pr
        os.environ['COMPDB_FILESTORAGE_DIR'] = self._tmp_fs
        os.environ['COMPDB_WORKING_DIR'] = self._tmp_wd
        #os.environ['COMPDB_VERSION'] = compdb.VERSION
        os.environ['COMPDB_DATABASE_AUTH_MECHANISM'] = 'none'
        os.environ['COMPDB_DATABASE_HOST'] = 'localhost'
        self._project = get_project()
        self.addCleanup(self._tmp_dir.cleanup)
        self.addCleanup(self._project.remove, force=True)

    def tearDown(self):
        pass
        #self._project.remove(force = True)
        #self._tmp_dir.cleanup()

class OldIDJobTest(BaseJobTest):

    def setUp(self):
        os.environ['COMPDB_VERSION'] = '0.1'
        super(OldIDJobTest, self).setUp()

class BaseOnlineJobTest(BaseJobTest):

    def open_job(self, *args, **kwargs):
        project = get_project()
        return project.open_job(*args, **kwargs)

class OfflineJobTest(BaseJobTest):

    def open_job(self, *args, **kwargs):
        project = get_project()
        return project.open_offline_job(*args, **kwargs)

class JobTest(BaseJobTest):
    pass

class ConfigTest(OfflineJobTest):
    
    def test_config_verification(self):
        from compdb.core.config import IllegalKeyError
        self._project.config.verify() 
        with self.assertRaises(IllegalKeyError):
            self._project.config['illegal_key'] = 'abc'
        with self.assertRaises(IllegalKeyError):
            self._project.config.update(dict(illegal_key = 'abc'))
        self._project.config.__setitem__('illegal_key', 'abc', force=True)
        # Bug in the `warnings` module prevents the usage of the following clause.
        # Possibly related to: 
        #   https://bitbucket.org/gutworth/six/issues/68/assertwarns-and-six
        #   https://code.djangoproject.com/ticket/23841
        #with self.assertWarns(UserWarning): 
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self._project.config.verify() 
        with self.assertRaises(ValueError):
            self._project.config.verify(strict=True)

    def test_set_get_delete(self):  
        key, value = list(test_token.items())[0]
        key, value = 'author_name', list(test_token.values())[0]
        config = copy.deepcopy(self._project.config)
        config[key] = value
        self.assertEqual(config[key], value)
        self.assertIn(key, config)
        del config[key]
        self.assertNotIn(key, config)

    def test_update(self):
        key, value = 'author_name', list(test_token.values())[0]
        config = copy.deepcopy(self._project.config)
        config.update({key: value})
        self.assertEqual(config[key], value)
        self.assertIn(key, config)

    def test_illegal_argument(self):
        from compdb.core.config import IllegalArgumentError, CHOICES
        with self.assertRaises(IllegalArgumentError):
            self._project.config[list(CHOICES.keys())[0]] = 'invalid'

    def test_config_files_and_dirs(self):
        from compdb.core.config import FILES, DIRS
        key_file = FILES[0]
        key_dir = DIRS[0]
        cwd = os.getcwd()
        try:
            with tempfile.NamedTemporaryFile() as tmp:
                head, tail = os.path.split(tmp.name)
                os.chdir(head)
                self._project.config[key_file] = tail
                self.assertEqual(os.path.abspath(tmp.name), self._project.config[key_file])
            self._project.config[key_dir] = self._tmp_dir.name
            self.assertEqual(os.path.abspath(self._tmp_dir.name), self._project.config[key_dir])
        except:
            raise
        finally:
            os.chdir(cwd)

    def test_set_and_retrieve_pw(self): 
        self._project.config['database_password'] = 'mypassword'
        self.assertNotEqual(self._project.config._args['database_password'], 'mypassword')
        self.assertEqual(self._project.config['database_password'], 'mypassword')

    def test_set_and_retrieve_version(self):
        fake_version = 0,0,0
        self._project.config['compdb_version'] = fake_version
        self.assertEqual(self._project.config['compdb_version'], fake_version)
        self._project.config['compdb_version'] = '.'.join((str(v) for v in fake_version))
        self.assertEqual(self._project.config['compdb_version'], fake_version)

    def test_str(self): 
        str(self._project.config)

    def test_read(self):    
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(json.dumps(test_token).encode())
            tmp.flush()
            self._project.config.read(tmp.name)
        self.assertEqual(self._project.config['test_token'], list(test_token.values())[0])

    def test_read_bad_file(self):    
        from compdb.core.config import IllegalKeyError
        with tempfile.NamedTemporaryFile() as tmp:
            with self.assertRaises(RuntimeError):
                self._project.config.read(tmp.name)
            tmp.write(json.dumps(test_token).encode())
            tmp.flush()
            self._project.config.read(tmp.name)
            self.assertEqual(self._project.config['test_token'], list(test_token.values())[0])
            with self.assertRaises(IllegalKeyError):
                self._project.config.verify(strict=True)

    def test_clear(self):
        config_copy = copy.deepcopy(self._project.config)
        config_copy.clear()
        self.assertEqual(len(config_copy), 0)

    def test_write_and_read(self):
        config_copy = copy.deepcopy(self._project.config)
        with tempfile.NamedTemporaryFile() as tmp:
            self._project.config.write(tmp.name)
            self._project.config.clear()
            self._project.config.read(tmp.name)
        self.assertEqual(str(self._project.config), str(config_copy))

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
                job.document
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

    def test_job_document_on_disk(self):
        key = 'test_job_document_on_disk'
        data = str(uuid.uuid4)
        job = self.open_job(test_token)
        with self.assertRaises(FileNotFoundError):
            job.load_document()
        job.store_document()
        doc = job.load_document()
        self.assertEqual(len(doc), 0)
        job.document[key] = data
        job.store_document()
        doc = job.load_document()
        self.assertEqual(len(doc), 1)
        self.assertEqual(doc[key], data)
        del job.document[key]
        self.assertNotIn(key, job.document)
        job.store_document()
        doc = job.load_document()
        self.assertEqual(len(doc), 0)

def open_and_lock_and_release_job(token):
    with open_job(test_token, timeout = 30) as job:
        pass
    return True

class JobConcurrencyTest(BaseOnlineJobTest):

    def test_recursive_job_opening(self):
        project = get_project()
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
        from compdb.contrib.concurrency import DocumentLockError
        with open_job(test_token, timeout = 1) as job:
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
                    [(test_token) for i in range(num_locks)])
                result = result.get(timeout = 60)
                self.assertEqual(result, [True] * num_locks)
        except Exception:
            raise
        finally:
            # clean up
            with open_job(test_token, timeout = 60) as job:
                pass
            job.remove(force = True)

class MyCustomClass(object):
    def __init__(self, a):
        self._a = a
        self._b = a
    def __add__(self, rhs):
        return MyCustomClass(self._a + rhs._a)
    def bar(self):
        return 'bar'
    def __eq__(self, rhs):
        return self._a == rhs._a and self._b == rhs._b

class MyCustomHeavyClass(MyCustomClass):
    def __init__(self, a):
        super().__init__(a)
        self._c = np.ones(int(2e7))
    def __eq__(self, rhs):
        return self._a == rhs._a and self._b == rhs._b and np.array_equal(self._c, rhs._c)

ex = False
def open_cache(unittest, data_type):

    a,b,c = range(3)
    global ex
    def foo(a, b, ** kwargs):
        global ex
        ex = True
        return data_type(a+b)

    expected_result = foo(a, b = b, c = c)
    ex = False
    with open_job(test_token) as job:
        result = job.cached(foo, a, b = b, c = c)
        #print(result, expected_result)
        unittest.assertEqual(result, expected_result)
    unittest.assertTrue(ex)

    ex = False
    with open_job(test_token) as job:
        result = job.cached(foo, a, b = b, c = c)
    unittest.assertEqual(result, expected_result)
    unittest.assertFalse(ex)
    job.remove()

@unittest.skipIf(not NUMPY, 'requires numpy')
class TestJobCache(BaseOnlineJobTest):
    
    def test_cache_native(self):
        open_cache(self, int)

    def test_cache_custom(self):
        open_cache(self, MyCustomClass)

    def test_cache_custom_heavy(self):
        open_cache(self, MyCustomHeavyClass)

    def test_cache_clear(self):
        project = get_project()
        open_cache(self, int)
        project.get_cache().clear()
        open_cache(self, int)
        project.get_cache().clear()

    def test_modify_code(self):
        a,b,c = range(3)
        global ex
        def foo(a, b, ** kwargs):
            global ex
            ex = True
            return int(a+b)

        expected_result = foo(a, b = b, c = c)
        ex = False
        with self.open_job(test_token) as job:
            result = job.cached(foo, a, b = b, c = c)
            #print(result, expected_result)
            self.assertEqual(result, expected_result)
        self.assertTrue(ex)

        def foo(a, b, ** kwargs):
            global ex
            ex = True
            return int(b+a)

        ex = False
        with self.open_job(test_token) as job:
            result = job.cached(foo, a, b = b, c = c)
        self.assertEqual(result, expected_result)
        self.assertTrue(ex)
        job.remove()

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

if __name__ == '__main__':
    unittest.main()
