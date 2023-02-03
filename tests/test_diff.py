# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from tempfile import TemporaryDirectory

import pytest

import signac


class TestDiffBase:
    project_class = signac.Project

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(path=self._tmp_dir.name)


class TestDiff(TestDiffBase):
    def test_two_jobs(self):
        job1 = self.project.open_job({"a": 0, "b": 1})
        job2 = self.project.open_job({"a": 0})
        expected = {str(job1.id): {"b": 1}, str(job2.id): {}}
        result = signac.diff_jobs(job1, job2)
        assert expected == result, f"{result} is not {expected}"

    def test_one_job(self):
        job1 = self.project.open_job({"a": 0})
        expected = {str(job1.id): {}}
        result = signac.diff_jobs(job1)
        assert expected == result, f"{result} is not {expected}"

    def test_no_jobs(self):
        assert signac.diff_jobs() == {}

    def test_nested(self):
        job1 = self.project.open_job({"a": 0, "b": {"c": True, "d": 11}})
        job2 = self.project.open_job({"a": 0, "b": {"c": True, "d": 4}})
        expected = {str(job1.id): {"b": {"d": 11}}, str(job2.id): {"b": {"d": 4}}}
        result = signac.diff_jobs(job1, job2)
        assert expected == result, f"{result} is not {expected}"

    def test_same_job(self):
        job1 = self.project.open_job({"a": 0, "b": 1})
        expected = {str(job1.id): {}}
        result = signac.diff_jobs(job1, job1)
        assert expected == result, f"{result} is not {expected}"
