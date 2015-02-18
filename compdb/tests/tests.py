#!/usr/bin/env python

import unittest

# Make sure the jobs created for this test are unique.
import uuid
test_token = {'test_token': uuid.uuid4()}

class ConfigTest(unittest.TestCase):
    
    def test_config_verification(self):
        import compdb

class JobTest(unittest.TestCase):

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

    def test_job_doc_retrieval(self):
        from compdb.contrib import open_job
        from compdb.contrib import job
        with open_job('testjob', test_token) as test_job:
            jobs_collection = job.get_jobs_collection()
            self.assertEqual(test_job.spec, test_job._spec)
            job_doc = jobs_collection.find_one(test_job.spec)
            self.assertIsNotNone(job_doc)
        test_job.remove()

    def test_job_status(self):
        from compdb.contrib import open_job
        with open_job('testjob', test_token) as job:
            self.assertEqual(job._get_status(), 'open')
        self.assertEqual(job._get_status(), 'closed')
        job.remove()

    def test_job_failure_status(self):
        from compdb.contrib import open_job
        try:
            with open_job('testjob', test_token) as job:
                self.assertEqual(job._get_status(), 'open')
                raise ValueError('expected')
        except ValueError:
            pass
        self.assertEqual(job._get_status(), 'error')
        job.remove()

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

    def test_reopen_job(self):
        from compdb.contrib import open_job
        job_name = 'test_reopen_job'
        with open_job(job_name, test_token) as job:
            job_id = job.get_id()

        with open_job(job_name, test_token) as job:
            self.assertEqual(job.get_id(), job_id)
        job.remove()

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

if __name__ == '__main__':
    unittest.main()
