#!/usr/bin/env python

import unittest

# Make sure the jobs created for this test are unique.
import uuid
test_token = {'test_token': uuid.uuid4()}

class ConfigTest(unittest.TestCase):
    
    def test_config_verification(self):
        import compdb

class JobOpenAndClosingTest(unittest.TestCase):

    def test_open_and_close(self):
        import compdb.contrib.job
        with compdb.contrib.job.Job({'name': 'testjob'}) as job:
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
        from compdb.contrib import open_job
        from compdb.contrib import job
        with open_job('testjob', test_token) as test_job:
            jobs_collection = job.get_jobs_collection()
            self.assertEqual(test_job.spec, test_job._spec)
            job_doc = jobs_collection.find_one(test_job.spec)
            self.assertIsNotNone(job_doc)
        test_job.remove()

class JobStorageTest(unittest.TestCase):
    
    def test_store_and_get(self):
        import uuid
        from compdb.contrib import open_job
        key = 'my_test_key'
        value = uuid.uuid4()
        with open_job('testjob', test_token) as test_job:
            test_job.store(key, value)
            self.assertIsNotNone(test_job.get(key))
            self.assertEqual(test_job.get(key), value)

        with open_job('testjob', test_token) as test_job:
            self.assertIsNotNone(test_job.get(key))
            self.assertEqual(test_job.get(key), value)
        test_job.remove()

    def test_store_and_retrieve_value_in_job_collection(self):
        import compdb.contrib
        from compdb.contrib import open_job
        import uuid
        doc = {'a': uuid.uuid4()}
        job_name = 'store_and_retrieve_value_in_job_collection'
        with open_job(job_name, test_token) as test_job:
            test_job.collection.insert(doc)

        jobs = compdb.contrib.find_jobs(job_name, test_token)
        for job in jobs:
            self.assertIsNotNone(job.collection.find_one(doc))
        test_job.remove()

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

    def test_open_storagefile(self):
        from compdb.contrib import open_job
        import uuid
        data = str(uuid.uuid4())

        with open_job('testjob', test_token) as job:
            with job.open_storagefile('_my_file', 'wb') as file:
                file.write(data.encode())

            with job.open_storagefile('_my_file', 'rb') as file:
                read_back = file.read().decode()

            job.remove_file('_my_file')
        self.assertEqual(data, read_back)
        job.remove()

    def test_job_clearing(self):
        from compdb.contrib import open_job
        from os.path import isfile
        import uuid
        data = str(uuid.uuid4())
        doc = {'a': uuid.uuid4()}

        with open_job('test_clean_job', test_token) as job:
            with job.open_storagefile('_my_file', 'wb') as file:
                file.write(data.encode())
            job.collection.save(doc)
            
        with open_job('test_clean_job', test_token) as job:
            with job.open_storagefile('_my_file', 'rb') as file:
                read_back = file.read().decode()
            self.assertEqual(data, read_back)
            self.assertIsNotNone(job.collection.find_one(doc))
            job.clear()
            with self.assertRaises(IOError):
                job.open_storagefile('_my_file', 'rb')
            self.assertIsNone(job.collection.find_one(doc))
        job.remove()

def open_and_lock_and_release_job(jobname, token):
    from compdb.contrib import open_job
    with open_job(jobname, test_token) as job:
        with job.lock(timeout = 1):
            pass
    return True


class JobConcurrencyTest(unittest.TestCase):

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
        job1.remove()

    def test_acquire_and_release(self):
        jobname = 'test_acquire_and_release'
        from compdb.contrib import open_job
        with open_job(jobname, test_token) as job:
            with job.lock(timeout = 1):
                pass
        job.remove()

    def test_process_concurrency(self):
        from compdb.contrib import open_job
        from multiprocessing import Pool

        jobname = 'test_process_concurrency'
        num_processes = 10
        num_locks = 10
        try:
            with Pool(processes = num_processes) as pool:
                result = pool.starmap_async(
                    open_and_lock_and_release_job,
                    [(jobname, test_token) for i in range(num_locks)])

                #result = pool.starmap_async(
                #    self.test_acquire_and_release,
                #    acquire_and_release,
                #    [(doc_id, 0.01) for i in range(num_locks)])
                result = result.get(timeout = 5)
                self.assertEqual(result, [True] * num_locks)
        except Exception:
            raise
        finally:
            # clean up
            with open_job(jobname, test_token) as job:
                pass
            job.remove(force = True)

if __name__ == '__main__':
    unittest.main()
