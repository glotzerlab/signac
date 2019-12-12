# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import unittest
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
            name='diff_test_project',
            root=self._tmp_pr,
            workspace=self._tmp_wd)

    def tearDown(self):
        pass


class DiffTest(BaseDiffTest):

    def test_two_jobs(self):
        job1 = self.project.open_job({'a': 0, 'b': 1})
        job2 = self.project.open_job({'a': 0})
        expected = {str(job1.id): {'b': 1}, str(job2.id): {}}
        result = signac.diff_jobs(job1, job2)
        self.assertEqual(expected, result, f'{result} is not {expected}')

    def test_one_job(self):
        job1 = self.project.open_job({'a': 0})
        expected = {str(job1.id): {}}
        result = signac.diff_jobs(job1)
        self.assertEqual(expected, result, f'{result} is not {expected}')

    def test_no_jobs(self):
        self.assertTrue(signac.diff_jobs() == {})

    def test_nested(self):
        job1 = self.project.open_job({'a': 0, 'b': {'c': True, 'd': 11}})
        job2 = self.project.open_job({'a': 0, 'b': {'c': True, 'd': 4}})
        expected = {str(job1.id): {'b': {'d': 11}}, str(job2.id): {'b': {'d': 4}}}
        result = signac.diff_jobs(job1, job2)
        self.assertEqual(expected, result, f'{result} is not {expected}')

    def test_same_job(self):
        job1 = self.project.open_job({'a': 0, 'b': 1})
        expected = {str(job1.id): {}}
        result = signac.diff_jobs(job1, job1)
        self.assertEqual(expected, result, f'{result} is not {expected}')


if __name__ == '__main__':
    unittest.main()
