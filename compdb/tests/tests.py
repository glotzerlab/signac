#!/usr/bin/env python

import unittest

class ConfigTest(unittest.TestCase):
    
    def test_config_verification(self):
        import compdb

class JobTest(unittest.TestCase):

    def test_open_and_close(self):
        import compdb.contrib.job
        with compdb.contrib.job.Job({'name': 'testjob'}) as job:
            pass

    def test_open_job_method(self):
        from compdb.contrib import open_job
        with open_job('testjob') as job:
            pass

    def test_job_doc_retrieval(self):
        from compdb.contrib import open_job
        from compdb.contrib import job
        with open_job('testjob') as test_job:
            jobs_collection = job.get_jobs_collection()
            self.assertEqual(test_job.spec, test_job._spec)
            job_doc = jobs_collection.find_one(test_job.spec)
            self.assertIsNotNone(job_doc)

    def test_job_status(self):
        from compdb.contrib import open_job
        from compdb.contrib import job
        with open_job('testjob') as test_job:
            jobs_collection = job.get_jobs_collection()
            job_doc = jobs_collection.find_one(test_job.spec)
            self.assertIsNotNone(job_doc)
            self.assertEqual(job_doc[job.JOB_STATUS_KEY], 'open')
        job_doc = jobs_collection.find_one(test_job.spec)
        self.assertEqual(job_doc[job.JOB_STATUS_KEY], 'closed')

    def test_job_failure_status(self):
        from compdb.contrib import open_job
        from compdb.contrib import job
        try:
            with open_job('testjob') as test_job:
                jobs_collection = job.get_jobs_collection()
                job_doc = jobs_collection.find_one(test_job.spec)
                self.assertIsNotNone(job_doc)
                self.assertEqual(job_doc[job.JOB_STATUS_KEY], 'open')
                raise ValueError('expected')
        except ValueError:
            pass
        job_doc = jobs_collection.find_one(test_job.spec)
        self.assertEqual(job_doc[job.JOB_STATUS_KEY], 'closed')
        self.assertIsNotNone(job_doc[job.JOB_ERROR_KEY])

    def test_store_and_retrieve_value_in_job_collection(self):
        import compdb.contrib
        from compdb.contrib import open_job
        import uuid
        doc = {'a': uuid.uuid4()}
        job_name = 'store_and_retrieve_value_in_job_collection'
        with open_job(job_name) as job:
            job.collection.insert(doc)

        jobs = compdb.contrib.find_jobs(name = job_name)
        for job in jobs:
            self.assertIsNotNone(job.collection.find_one(doc))

    def test_reopen_job(self):
        from compdb.contrib import open_job
        job_name = 'test_reopen_job'
        with open_job(job_name) as job:
            job_id = job.get_id()

        with open_job(job_name) as job:
            self.assertEqual(job.get_id(), job_id)

    def test_reopen_job_and_reretrieve_doc(self):
        from compdb.contrib import open_job
        job_name = 'test_reopen_job'
        import uuid
        doc = {'a': uuid.uuid4()}
        with open_job(job_name) as job:
            job.collection.save(doc)
            job_id = job.get_id()

        with open_job(job_name) as job:
            self.assertEqual(job.get_id(), job_id)
            self.assertIsNotNone(job.collection.find_one(doc))

if __name__ == '__main__':
    unittest.main()
