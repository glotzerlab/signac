import unittest
from contextlib import contextmanager

# Make sure the jobs created for this test are unique.
import uuid
test_token = {'test_token': str(uuid.uuid4())}

@contextmanager
def safe_open_job(name, * parameters):
    from compdb.contrib import open_job as oj
    job = oj(name, * parameters)
    try:
        yield job
    except Exception:
        job.remove()
        raise

class JobTest(unittest.TestCase):
    
    def setUp(self):
        import os, tempfile
        from compdb.contrib import get_project
        self._tmp_pr = tempfile.TemporaryDirectory(prefix = 'compdb')
        self._tmp_wd = tempfile.TemporaryDirectory(prefix = 'compdb')
        self._tmp_fs = tempfile.TemporaryDirectory(prefix = 'compdb')
        os.environ['COMPDB_AUTHOR_NAME'] = 'compdb_test_author'
        os.environ['COMPDB_AUTHOR_EMAIL'] = 'testauthor@example.com'
        os.environ['COMPDB_PROJECT'] = 'compdb_test_project'
        os.environ['COMPDB_PROJECT_DIR'] = self._tmp_pr.name
        os.environ['COMPDB_FILESTORAGE_DIR'] = self._tmp_fs.name
        os.environ['COMPDB_WORKING_DIR'] = self._tmp_wd.name
        #os.environ['COMPDB_DATABASE_HOST'] = 'invalid_host'
        self._project = get_project()

    def tearDown(self):
        self._tmp_pr.cleanup()
        self._tmp_wd.cleanup()
        self._tmp_fs.cleanup()
        self._project.remove()

class ConfigTest(JobTest):
    
    def test_config_verification(self):
        import compdb

class JobOpenAndClosingTest(JobTest):

    def test_open_and_close(self):
        import compdb.contrib.job
        project = compdb.contrib.job.Project()
        with compdb.contrib.job.Job(project, {'name': 'testjob'}) as job:
            pass
        job.remove()

    def test_open_job_method(self):
        from compdb.contrib import open_job
        with open_job('testjob', test_token) as job:
            pass
        job.remove()

    def test_reopen_job(self):
        from compdb.contrib import open_job
        job_name = 'test_reopen_job'
        with open_job(job_name, test_token) as job:
            job_id = job.get_id()

        with open_job(job_name, test_token) as job:
            self.assertEqual(job.get_id(), job_id)
        job.remove()

    def test_job_doc_retrieval(self):
        from compdb.contrib import open_job, get_project
        project = get_project()
        with open_job('testjob', test_token) as test_job:
            jobs_collection = project.get_jobs_collection()
            self.assertEqual(test_job.spec, test_job._spec)
            job_doc = jobs_collection.find_one(test_job.spec)
            self.assertIsNotNone(job_doc)
        test_job.remove()

class JobStorageTest(JobTest):
    
    def test_store_and_get(self):
        import uuid
        from compdb.contrib import open_job
        key = 'my_test_key'
        value = uuid.uuid4()
        with open_job('testjob', test_token) as test_job:
            test_job.document[key] = value
            self.assertEqual(test_job.document[key], value)
            self.assertIsNotNone(test_job.document.get(key))
            self.assertEqual(test_job.document.get(key), value)

        with open_job('testjob', test_token) as test_job:
            self.assertIsNotNone(test_job.document.get(key))
            self.assertEqual(test_job.document.get(key), value)
            self.assertEqual(test_job.document[key], value)
        test_job.remove()

    def test_store_and_retrieve_value_in_job_collection(self):
        import compdb.contrib
        from compdb.contrib import open_job
        import uuid
        doc = {'a': uuid.uuid4()}
        job_name = 'store_and_retrieve_value_in_job_collection'
        with open_job(job_name, test_token) as test_job:
            test_job.collection.insert(doc)
        test_job.remove()

        jobs = compdb.contrib.find_jobs(job_name, test_token)
        for job in jobs:
            self.assertIsNotNone(job.collection.find_one(doc))

    def test_reopen_job_and_reretrieve_doc(self):
        from compdb.contrib import open_job
        job_name = 'test_reopen_job'
        import uuid
        doc = {'a': uuid.uuid4()}
        with open_job(job_name, test_token) as job:
            job.collection.save(doc)
            job_id = job.get_id()

        with open_job(job_name, test_token) as job:
            self.assertEqual(job.get_id(), job_id)
            self.assertIsNotNone(job.collection.find_one(doc))
        job.remove()

    def test_open_file(self):
        from compdb.contrib import open_job
        import uuid
        data = str(uuid.uuid4())

        with open_job('testjob', test_token) as job:
            with job.storage.open_file('_my_file', 'wb') as file:
                file.write(data.encode())

            with job.storage.open_file('_my_file', 'rb') as file:
                read_back = file.read().decode()

            job.storage.remove_file('_my_file')
        self.assertEqual(data, read_back)
        job.remove()

    def test_store_and_restore_file(self):
        import os, uuid
        from compdb.contrib import open_job
        data = str(uuid.uuid4())
        fn = '_my_file'

        with open_job('testjob', test_token) as job:
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
        job.remove()

    def test_store_all_and_restore_all(self):
        import os, uuid
        from compdb.contrib import open_job
        data = str(uuid.uuid4())
        fns = ('_my_file', '_my_second_file')

        with open_job('testjob', test_token) as job:
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
        job.remove()

    def test_job_clearing(self):
        from compdb.contrib import open_job
        from os.path import isfile
        import uuid
        data = str(uuid.uuid4())
        doc = {'a': uuid.uuid4()}

        with open_job('test_clean_job', test_token) as job:
            with job.storage.open_file('_my_file', 'wb') as file:
                file.write(data.encode())
            job.collection.save(doc)
            
        with open_job('test_clean_job', test_token) as job:
            with job.storage.open_file('_my_file', 'rb') as file:
                read_back = file.read().decode()
            self.assertEqual(data, read_back)
            self.assertIsNotNone(job.collection.find_one(doc))
            job.clear()
            with self.assertRaises(IOError):
                job.storage.open_file('_my_file', 'rb')
            self.assertIsNone(job.collection.find_one(doc))
        job.remove()

def open_and_lock_and_release_job(jobname, token):
    from compdb.contrib import open_job
    with open_job(jobname, test_token) as job:
        with job.lock(timeout = 1):
            if job.milestones.reached('concurrent'):
                job.milestones.remove('concurrent')
            else:
                job.milestones.mark('concurrent')
            pass
    return True

class JobConcurrencyTest(JobTest):

    def test_recursive_job_opening(self):
        jobname = 'test_multiple_instances'
        from compdb.contrib import open_job
        with open_job(jobname, test_token) as job0:
            self.assertEqual(job0.num_open_instances(), 1)
            with open_job(jobname, test_token) as job1:
                self.assertEqual(job0.num_open_instances(), 2)
                self.assertEqual(job1.num_open_instances(), 2)
            self.assertEqual(job0.num_open_instances(), 1)
            self.assertEqual(job1.num_open_instances(), 1)
        self.assertEqual(job0.num_open_instances(), 0)
        self.assertEqual(job1.num_open_instances(), 0)
        job0.remove()

    def test_acquire_and_release(self):
        jobname = 'test_acquire_and_release'
        from compdb.contrib import open_job
        with open_job(jobname, test_token, timeout = 1) as job:
            with job.lock(timeout = 1):
                pass
        job.remove()

    def test_process_concurrency(self):
        from compdb.contrib import open_job
        from multiprocessing import Pool

        jobname = 'test_process_concurrency'
        num_processes = 4
        num_locks = 4
        try:
            with Pool(processes = num_processes) as pool:
                result = pool.starmap_async(
                    open_and_lock_and_release_job,
                    [(jobname, test_token) for i in range(num_locks)])
                result = result.get(timeout = 5)
                self.assertEqual(result, [True] * num_locks)
        except Exception:
            raise
        finally:
            # clean up
            with open_job(jobname, test_token) as job:
                pass
            job.remove(force = True)

    def test_sections(self):
        from compdb.contrib import open_job
        name = 'test_sections'
        with open_job(name, test_token) as job:
            ex = False
            with job.section('sec0') as sec:
                if not sec.completed():
                    ex = True
            self.assertTrue(ex)

            ex2 = False
            with job.section('sec0') as sec:
                if not sec.completed():
                    ex2 = True
            self.assertFalse(ex2)

        with open_job(name, test_token) as job:
            ex3 = False
            with job.section('sec0') as sec:
                if not sec.completed():
                    ex3 = True
            self.assertFalse(ex3)
        job.remove()

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
        import numpy as np
        super().__init__(a)
        self._c = np.ones(1e6)
    def __eq__(self, rhs):
        import numpy as np
        return self._a == rhs._a and self._b == rhs._b and np.array_equal(self._c, rhs._c)

ex = False
def open_cache(unittest, data_type):
    from compdb.contrib import open_job

    a,b,c = range(3)
    global ex
    def foo(a, b, ** kwargs):
        global ex
        ex = True
        return data_type(a+b)

    job_name = str(data_type)
    expected_result = foo(a, b = b, c = c, job_name = job_name)
    ex = False
    with open_job(job_name, test_token) as job:
        result = job.cached(foo, a, b = b, c = c, job_name = job_name)
        print(result, expected_result)
        unittest.assertEqual(result, expected_result)
    unittest.assertTrue(ex)

    ex = False
    with open_job(job_name, test_token) as job:
        result = job.cached(foo, a, b = b, c = c, job_name = job_name)
    unittest.assertEqual(result, expected_result)
    unittest.assertFalse(ex)
    job.remove()

class TestJobCache(JobTest):
    
    def test_cache_native(self):
        open_cache(self, int)

    def test_cache_custom(self):
        open_cache(self, MyCustomClass)

    def test_cache_custom_heavy(self):
        open_cache(self, MyCustomHeavyClass)

class TestJobMilestones(JobTest):
    
    def test_milestones(self):
        from compdb.contrib import open_job
        name = 'test_milestones'
        with open_job(name, test_token) as job:
            job.milestones.clear()
            self.assertFalse(job.milestones.reached('started'))
            job.milestones.mark('started')
            self.assertTrue(job.milestones.reached('started'))
            self.assertFalse(job.milestones.reached('other'))
            job.milestones.mark('started')
            job.milestones.mark('other')
            self.assertTrue(job.milestones.reached('started'))
            self.assertTrue(job.milestones.reached('other'))
            job.milestones.remove('started')
            self.assertFalse(job.milestones.reached('started'))
            self.assertTrue(job.milestones.reached('other'))
            job.milestones.remove('started')
            self.assertFalse(job.milestones.reached('started'))
            job.milestones.mark('started')
            job.milestones.mark('other')
            self.assertTrue(job.milestones.reached('started'))
            self.assertTrue(job.milestones.reached('other'))
            job.milestones.clear()
            self.assertFalse(job.milestones.reached('started'))
            self.assertFalse(job.milestones.reached('other'))
        job.remove()

    def test_milestones_reopen(self):
        from compdb.contrib import open_job
        name = 'test_milestones'
        with open_job(name, test_token) as job:
            job.milestones.clear()
            self.assertFalse(job.milestones.reached('started'))
            job.milestones.mark('started')

        with open_job(name, test_token) as job:
            self.assertTrue(job.milestones.reached('started'))
        job.remove()

if __name__ == '__main__':
    unittest.main()
