# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import signac
from tempfile import TemporaryDirectory


class TestBaseDiff():

    project_class = signac.Project

    @pytest.fixture
    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        # self.addCleanup(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(
            name='diff_test_project',
            root=self._tmp_dir.name)


class TestDiff(TestBaseDiff):

    def test_two_jobs(self,setUp):
        job1 = self.project.open_job({'a': 0, 'b': 1})
        job2 = self.project.open_job({'a': 0})
        expected = {str(job1.id): {'b': 1}, str(job2.id): {}}
        result = signac.diff_jobs(job1, job2)
        assert expected == result, '{} is not {}'.format(result, expected)

    def test_one_job(self,setUp):
        job1 = self.project.open_job({'a': 0})
        expected = {str(job1.id): {}}
        result = signac.diff_jobs(job1)
        assert expected == result, '{} is not {}'.format(result, expected)

    def test_no_jobs(self,setUp):
        assert signac.diff_jobs() == {}

    def test_nested(self,setUp):
        job1 = self.project.open_job({'a': 0, 'b': {'c': True, 'd': 11}})
        job2 = self.project.open_job({'a': 0, 'b': {'c': True, 'd': 4}})
        expected = {str(job1.id): {'b': {'d': 11}}, str(job2.id): {'b': {'d': 4}}}
        result = signac.diff_jobs(job1, job2)
        assert expected == result, '{} is not {}'.format(result, expected)

    def test_same_job(self,setUp):
        job1 = self.project.open_job({'a': 0, 'b': 1})
        expected = {str(job1.id): {}}
        result = signac.diff_jobs(job1, job1)
        assert expected == result, '{} is not {}'.format(result, expected)

