import os
import json
import unittest
import subprocess
import signac
from tempfile import TemporaryDirectory

class BaseDiffTest(unittest.TestCase):

    project_class = signac.Project

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, 'pr')
        self._tmp_wd = os.path.join(self._tmp_dir.name, 'wd')
        os.mkdir(self._tmp_pr)
        self.config = signac.common.config.load_config()
        self.project = self.project_class.init_project(
            name='testing_test_project',
            root=self._tmp_pr,
            workspace=self._tmp_wd)
        self.project.config['default_host'] = 'testing'

    def tearDown(self):
        pass

    def open_job(self, *args, **kwargs):
        project = self.project
        return project.open_job(*args, **kwargs)


class DiffTest(BaseDiffTest):

    def test_two_jobs(self):
        job1 = self.project.open_job({'a': 0, 'b':1})
        job2 = self.project.open_job({'a': 0})
        print(signac.diff_jobs(job1, job2))
        self.assertEqual(signac.diff_jobs(job1, job2), {str(job1.get_id()):{'b':1}})

    def test_one_job(self):
        job1 = self.project.open_job({'a': 0})
        print(signac.diff_jobs(job1))
        self.assertEqual(signac.diff_jobs(job1), {})

    def test_no_jobs(self):
        self.assertTrue(signac.diff_jobs() == {})

    def test_nested(self):
        job1 = self.project.open_job({'a': 0, 'b':{'c':True, 'd':11}})
        job2 = self.project.open_job({'a': 0, 'b':{'c':True, 'd':4}})
        result = signac.diff_jobs(job1, job2)
        self.assertEqual(result, {str(job1.get_id()): {'b.d': 11}, str(job2.get_id()): {'b.d': 4}})

    def test_same_job(self):
        job1 = self.project.open_job({'a': 0, 'b':1})
        print(signac.diff_jobs(job1, job1))
        self.assertTrue(signac.diff_jobs(job1, job1) == {})


if __name__ == '__main__':
    unittest.main()
