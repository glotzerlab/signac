#!/usr/bin/env python

import unittest

class ConfigTest(unittest.TestCase):
    
    def test_config_verification(self):
        import compdb

class JobTest(unittest.TestCase):

    def test_open_and_close(self):
        import compdb.contrib.job
        with compdb.contrib.job.Job('testjob') as job:
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
            job_doc = jobs_collection.find_one(test_job._spec())
            assert job_doc is not None

    def test_job_status(self):
        from compdb.contrib import open_job
        from compdb.contrib import job
        with open_job('testjob') as test_job:
            jobs_collection = job.get_jobs_collection()
            job_doc = jobs_collection.find_one(test_job._spec())
            assert job_doc[job.JOB_STATUS_KEY] == 'open'
        job_doc = jobs_collection.find_one(test_job._spec())
        assert job_doc[job.JOB_STATUS_KEY] == 'closed'

    def test_job_failure_status(self):
        from compdb.contrib import open_job
        from compdb.contrib import job
        try:
            with open_job('testjob') as test_job:
                jobs_collection = job.get_jobs_collection()
                job_doc = jobs_collection.find_one(test_job._spec())
                assert job_doc[job.JOB_STATUS_KEY] == 'open'
                raise ValueError('expected')
        except ValueError:
            pass
        job_doc = jobs_collection.find_one(test_job._spec())
        assert job_doc[job.JOB_STATUS_KEY] == 'closed'
        assert job_doc[job.JOB_ERROR_KEY] is not None

if __name__ == '__main__':
    unittest.main()
