# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import functools
import gzip
import io
import json
import logging
import os
import pickle
import re
import sys
import textwrap
from contextlib import contextmanager, redirect_stderr
from tarfile import TarFile
from tempfile import TemporaryDirectory
from time import time
from zipfile import ZipFile

import pytest
import test_h5store
from packaging import version
from test_job import TestJobBase

import signac
from signac._config import (
    PROJECT_CONFIG_FN,
    _get_project_config_fn,
    _load_config,
    _read_config_file,
)
from signac.errors import (
    DestinationExistsError,
    IncompatibleSchemaVersion,
    JobsCorruptedError,
    StatepointParsingError,
    WorkspaceError,
)
from signac.job import calc_id
from signac.linked_view import _find_all_links
from signac.project import JobsCursor, Project  # noqa: F401
from signac.schema import ProjectSchema

try:
    import pandas  # noqa
except ImportError:
    PANDAS = False
else:
    PANDAS = True

try:
    import h5py  # noqa

    H5PY = True
except ImportError:
    H5PY = False

try:
    import numpy  # noqa

    NUMPY = True
except ImportError:
    NUMPY = False

WINDOWS = sys.platform == "win32"


@functools.lru_cache
def _check_symlinks_supported():
    """Check if symlinks are supported on the current platform."""
    try:
        with TemporaryDirectory() as tmp_dir:
            os.symlink(
                os.path.realpath(__file__), os.path.join(tmp_dir, "test_symlink")
            )
        return True
    except (NotImplementedError, OSError):
        return False


def skip_windows_without_symlinks(test_func):
    """Skip test if platform is Windows and symlinks are not supported."""

    return pytest.mark.skipif(
        WINDOWS and not _check_symlinks_supported(),
        reason="Symbolic links are unsupported on Windows unless in Developer Mode.",
    )(test_func)


class TestProjectBase(TestJobBase):
    pass


class TestProject(TestProjectBase):
    def test_repr(self):
        p = eval(repr(self.project))
        assert repr(p) == repr(self.project)
        assert p == self.project

    def test_str(self):
        assert str(self.project) == self.project.path

    def test_path(self):
        assert self._tmp_pr == self.project.path

    def test_workspace_directory(self):
        assert os.path.join(self._tmp_pr, "workspace") == self.project.workspace

    def test_config_modification(self):
        # Ensure that the project configuration is immutable.
        with pytest.raises(ValueError):
            self.project.config["foo"] = "bar"

    def test_workspace_directory_exists(self):
        assert os.path.exists(self.project.workspace)

    def test_fn(self):
        assert self.project.fn("test/abc") == os.path.join(
            self.project.path, "test/abc"
        )

    def test_isfile(self):
        assert not self.project.isfile("test")
        with open(self.project.fn("test"), "w"):
            pass
        assert self.project.isfile("test")

    def test_document(self):
        assert not self.project.document
        assert len(self.project.document) == 0
        self.project.document["a"] = 42
        assert len(self.project.document) == 1
        assert self.project.document
        prj2 = type(self.project).get_project(path=self.project.path)
        assert prj2.document
        assert len(prj2.document) == 1
        self.project.document.clear()
        assert not self.project.document
        assert len(self.project.document) == 0
        assert not prj2.document
        assert len(prj2.document) == 0
        self.project.document.a = {"b": 43}
        assert self.project.document == {"a": {"b": 43}}
        self.project.document.a.b = 44
        assert self.project.document == {"a": {"b": 44}}
        self.project.document = {"a": {"b": 45}}
        assert self.project.document == {"a": {"b": 45}}

    def test_doc(self):
        assert not self.project.doc
        assert len(self.project.doc) == 0
        self.project.doc["a"] = 42
        assert len(self.project.doc) == 1
        assert self.project.doc
        prj2 = type(self.project).get_project(path=self.project.path)
        assert prj2.doc
        assert len(prj2.doc) == 1
        self.project.doc.clear()
        assert not self.project.doc
        assert len(self.project.doc) == 0
        assert not prj2.doc
        assert len(prj2.doc) == 0
        self.project.doc.a = {"b": 43}
        assert self.project.doc == {"a": {"b": 43}}
        self.project.doc.a.b = 44
        assert self.project.doc == {"a": {"b": 44}}
        self.project.doc = {"a": {"b": 45}}
        assert self.project.doc == {"a": {"b": 45}}

    @pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
    @pytest.mark.skipif(not NUMPY, reason="test requires the numpy package")
    def test_data(self):
        with self.project.data:
            assert not self.project.data
            assert len(self.project.data) == 0
            self.project.data["a"] = 42
            assert len(self.project.data) == 1
            assert self.project.data
        prj2 = type(self.project).get_project(path=self.project.path)
        with prj2.data:
            assert prj2.data
            assert len(prj2.data) == 1
        with self.project.data:
            self.project.data.clear()
            assert not self.project.data
            assert len(self.project.data) == 0
        with prj2.data:
            assert not prj2.data
            assert len(prj2.data) == 0
        with self.project.data:
            self.project.data.a = {"b": 43}
            assert self.project.data == {"a": {"b": 43}}
            self.project.data.a.b = 44
            assert self.project.data == {"a": {"b": 44}}
            self.project.data["c"] = numpy.zeros(10)
            numpy.testing.assert_array_equal(self.project.data["c"], numpy.zeros(10))
        # This setter will overwrite the file. We leave the context manager so
        # that the file is closed before overwriting it.
        self.project.data = {"a": {"b": 45}}
        assert self.project.data == {"a": {"b": 45}}

    def test_no_workspace_warn_on_find(self, caplog):
        if os.path.exists(self.project.workspace):
            os.rmdir(self.project.workspace)
        with caplog.at_level(logging.INFO):
            list(self.project.find_jobs())
            # Python < 3.8 will return 2 messages.
            # Python >= 3.8 will return 3 messages, because it determines the
            # length of the project one additional time during the list
            # constructor: https://bugs.python.org/issue33234
            assert len(caplog.records) in (2, 3)

    @skip_windows_without_symlinks
    def test_workspace_broken_link_error_on_find(self):
        with TemporaryDirectory() as tmp_dir:
            project = self.project_class.init_project(path=tmp_dir)
            os.rmdir(project.workspace)
            os.symlink(
                os.path.join(tmp_dir, "workspace~"),
                project.workspace,
            )
            with pytest.raises(WorkspaceError):
                list(project.find_jobs())

    def test_workspace_read_only_path(self):
        # Create file where workspace would be, thus preventing the creation
        # of the workspace directory.
        if os.path.exists(self.project.workspace):
            os.rmdir(self.project.workspace)
        with open(os.path.join(self.project.workspace), "w"):
            pass

        with pytest.raises(OSError):  # Ensure that the file is in place.
            os.mkdir(self.project.workspace)

        assert issubclass(WorkspaceError, OSError)

        try:
            logging.disable(logging.ERROR)
            with pytest.raises(WorkspaceError):
                list(self.project.find_jobs())
        finally:
            logging.disable(logging.NOTSET)

        assert not os.path.isdir(self.project.workspace)

    def test_find_jobs(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document["b"] = sp["a"]
        assert len(statepoints) == len(self.project)
        assert len(statepoints) == len(list(self.project.find_jobs()))
        assert len(statepoints) == len(list(self.project.find_jobs({})))
        assert 1 == len(list(self.project.find_jobs({"a": 0})))
        assert 0 == len(list(self.project.find_jobs({"a": 5})))
        assert 1 == len(list(self.project.find_jobs({"a": 0})))
        assert 0 == len(list(self.project.find_jobs({"a": 5})))
        assert 1 == len(list(self.project.find_jobs({"sp.a": 0})))
        assert 0 == len(list(self.project.find_jobs({"sp.a": 5})))
        assert 1 == len(list(self.project.find_jobs({"doc.b": 0})))
        assert 0 == len(list(self.project.find_jobs({"doc.b": 5})))
        assert 1 == len(list(self.project.find_jobs({"a": 0, "doc.b": 0})))
        assert 1 == len(list(self.project.find_jobs({"sp.a": 0, "doc.b": 0})))
        assert 0 == len(list(self.project.find_jobs({"a": 0, "doc.b": 5})))
        assert 0 == len(list(self.project.find_jobs({"sp.a": 0, "doc.b": 5})))
        assert 0 == len(list(self.project.find_jobs({"sp.a": 5, "doc.b": 0})))
        assert 0 == len(list(self.project.find_jobs({"sp.a": 5, "doc.b": 5})))
        for job in self.project.find_jobs():
            assert self.project.open_job(id=job.id).id == job.id

    def test_find_jobs_JobsCursor_contains(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document["test"] = True
        cursor_all = self.project.find_jobs()
        for sp in statepoints:
            assert self.project.open_job(sp) in cursor_all
        cursor_first = self.project.find_jobs(statepoints[0])
        for sp in statepoints:
            if sp["a"] == 0:
                assert self.project.open_job(sp) in cursor_first
            else:
                assert self.project.open_job(sp) not in cursor_first
        cursor_doc = self.project.find_jobs({"doc.test": True})
        for sp in statepoints:
            assert self.project.open_job(sp) in cursor_doc

    def test_find_jobs_arithmetic_operators(self):
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        assert len(self.project) == 10
        assert len(self.project.find_jobs({"a": {"$lt": 5}})) == 5
        assert len(self.project.find_jobs({"a.$lt": 5})) == 5

    def test_find_jobs_logical_operators(self):
        def assert_result_len(q, num):
            assert len(self.project.find_jobs(q)) == num

        for i in range(10):
            job = self.project.open_job({"a": i, "b": {"c": i}}).init()
            job.doc.d = i
        assert len(self.project) == 10
        with pytest.raises(ValueError):
            list(self.project.find_jobs({"$and": {"foo": "bar"}}))

        # implicit sp.-prefix
        assert_result_len({"$and": [{}, {"a": 0}]}, 1)
        assert_result_len({"$or": [{}, {"a": 0}]}, len(self.project))
        assert len(self.project) == 10
        with pytest.raises(ValueError):
            list(self.project.find_jobs({"$and": {"foo": "bar"}}))
        assert_result_len({"$and": [{}, {"a": 0}]}, 1)
        assert_result_len({"$or": [{}, {"a": 0}]}, len(self.project))
        assert_result_len({"$and": [{"a": 0}, {"a": 1}]}, 0)
        assert_result_len({"$or": [{"a": 0}, {"a": 1}]}, 2)
        assert_result_len({"$and": [{"$and": [{"a": 0}, {"a": 1}]}]}, 0)
        assert_result_len({"$and": [{"$or": [{"a": 0}, {"a": 1}]}]}, 2)
        assert_result_len({"$or": [{"$or": [{"a": 0}, {"a": 1}]}]}, 2)
        assert_result_len({"$or": [{"$and": [{"a": 0}, {"a": 1}]}]}, 0)
        assert_result_len({"$and": [{}, {"b": {"c": 0}}]}, 1)
        assert_result_len({"$or": [{}, {"b": {"c": 0}}]}, len(self.project))
        assert_result_len({"$and": [{"b": {"c": 0}}, {"b": {"c": 1}}]}, 0)
        assert_result_len({"$or": [{"b": {"c": 0}}, {"b": {"c": 1}}]}, 2)
        assert_result_len({"$and": [{"$and": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}, 0)
        assert_result_len({"$and": [{"$or": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}, 2)
        assert_result_len({"$or": [{"$or": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}, 2)
        assert_result_len({"$or": [{"$and": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}, 0)

        # explicit sp.-prefix
        assert_result_len({"$and": [{}, {"sp.a": 0}]}, 1)
        assert_result_len({"$or": [{}, {"sp.a": 0}]}, len(self.project))
        assert_result_len({"$and": [{"sp.a": 0}, {"sp.a": 1}]}, 0)
        assert_result_len({"$or": [{"sp.a": 0}, {"sp.a": 1}]}, 2)
        assert_result_len({"$and": [{"$and": [{"sp.a": 0}, {"sp.a": 1}]}]}, 0)
        assert_result_len({"$and": [{"$or": [{"sp.a": 0}, {"sp.a": 1}]}]}, 2)
        assert_result_len({"$or": [{"$or": [{"sp.a": 0}, {"sp.a": 1}]}]}, 2)
        assert_result_len({"$or": [{"$and": [{"sp.a": 0}, {"sp.a": 1}]}]}, 0)
        assert_result_len({"$and": [{}, {"sp.b": {"c": 0}}]}, 1)
        assert_result_len({"$and": [{}, {"sp.b.c": 0}]}, 1)
        assert_result_len({"$or": [{}, {"sp.b": {"c": 0}}]}, len(self.project))
        assert_result_len({"$or": [{}, {"sp.b.c": 0}]}, len(self.project))
        assert_result_len({"$and": [{"sp.b": {"c": 0}}, {"sp.b": {"c": 1}}]}, 0)
        assert_result_len({"$and": [{"sp.b": {"c": 0}}, {"sp.b.c": 1}]}, 0)
        assert_result_len({"$or": [{"sp.b": {"c": 0}}, {"sp.b": {"c": 1}}]}, 2)
        assert_result_len({"$or": [{"sp.b": {"c": 0}}, {"sp.b.c": 1}]}, 2)
        assert_result_len(
            {"$and": [{"$and": [{"sp.b": {"c": 0}}, {"sp.b": {"c": 1}}]}]}, 0
        )
        assert_result_len({"$and": [{"$and": [{"sp.b.c": 0}, {"sp.b.c": 1}]}]}, 0)
        assert_result_len(
            {"$and": [{"$or": [{"sp.b": {"c": 0}}, {"sp.b": {"c": 1}}]}]}, 2
        )
        assert_result_len({"$and": [{"$or": [{"sp.b.c": 0}, {"sp.b.c": 1}]}]}, 2)
        assert_result_len(
            {"$or": [{"$or": [{"sp.b": {"c": 0}}, {"sp.b": {"c": 1}}]}]}, 2
        )
        assert_result_len({"$or": [{"$or": [{"sp.b.c": 0}, {"sp.b.c": 1}]}]}, 2)
        assert_result_len(
            {"$or": [{"$and": [{"sp.b": {"c": 0}}, {"sp.b": {"c": 1}}]}]}, 0
        )
        assert_result_len({"$or": [{"$and": [{"sp.b.c": 0}, {"sp.b.c": 1}]}]}, 0)

        # Explicit doc prefix
        assert_result_len({"doc.d": 1}, 1)

        # Mixed filters

        assert_result_len({"$and": [{"sp": {"a": 0}}, {"doc": {"d": 0}}]}, 1)
        assert_result_len(
            {"$and": [{"$and": [{"sp": {"a": 0}}, {"doc": {"d": 0}}]}]}, 1
        )
        assert_result_len({"$or": [{"sp": {"a": 0}}, {"doc": {"d": 0}}]}, 1)
        assert_result_len({"$or": [{"$and": [{"sp": {"a": 0}}, {"doc": {"d": 0}}]}]}, 1)
        assert_result_len({"$and": [{"sp": {"a": 0}}, {"doc": {"d": 1}}]}, 0)
        assert_result_len(
            {"$and": [{"$and": [{"sp": {"a": 0}}, {"doc": {"d": 1}}]}]}, 0
        )
        assert_result_len({"$or": [{"sp": {"a": 0}}, {"doc": {"d": 1}}]}, 2)

        assert_result_len({"$and": [{"sp.a": 0}, {"doc": {"d": 0}}]}, 1)
        assert_result_len({"$or": [{"sp.a": 0}, {"doc": {"d": 0}}]}, 1)
        assert_result_len({"$and": [{"sp.a": 0}, {"doc": {"d": 1}}]}, 0)
        assert_result_len({"$or": [{"sp.a": 0}, {"doc": {"d": 1}}]}, 2)

        assert_result_len({"$and": [{"sp.a": 0}, {"doc.d": 0}]}, 1)
        assert_result_len({"$or": [{"sp.a": 0}, {"doc.d": 0}]}, 1)
        assert_result_len({"$and": [{"sp.a": 0}, {"doc.d": 1}]}, 0)
        assert_result_len({"$or": [{"sp.a": 0}, {"doc.d": 1}]}, 2)

    def test_len_project(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        assert len(statepoints) == len(self.project)
        assert len(statepoints) == len(self.project.find_jobs())

    def test_len_find_jobs(self):
        statepoints = [{"a": i, "b": i < 3} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        assert len(self.project) == len(self.project.find_jobs())
        assert 3 == len(self.project.find_jobs({"b": True}))

    def test_iteration(self):
        statepoints = [{"a": i, "b": i < 3} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        for i, job in enumerate(self.project):
            pass
        assert i == len(self.project) - 1

    def test_open_job_by_id(self):
        statepoints = [{"a": i} for i in range(5)]
        jobs = [self.project.open_job(sp) for sp in statepoints]
        self.project._sp_cache.clear()
        try:
            logging.disable(logging.WARNING)
            for job in jobs:
                with pytest.raises(KeyError):
                    self.project.open_job(id=str(job))
            for job in jobs:
                job.init()
            for job in jobs:
                self.project.open_job(id=str(job))
            with pytest.raises(KeyError):
                self.project.open_job(id="abc")
            with pytest.raises(ValueError):
                self.project.open_job()
            with pytest.raises(ValueError):
                self.project.open_job(statepoints[0], id=str(jobs[0]))
        finally:
            logging.disable(logging.NOTSET)

    def test_open_job_no_id_or_statepoint(self):
        with pytest.raises(ValueError):
            self.project.open_job()

    def test_open_job_by_abbreviated_id(self):
        statepoints = [{"a": i} for i in range(5)]
        [self.project.open_job(sp).init() for sp in statepoints]
        aid_len = self.project.min_len_unique_id()
        for job in self.project.find_jobs():
            aid = job.id[:aid_len]
            assert self.project.open_job(id=aid) == job
        with pytest.raises(LookupError):
            for job in self.project.find_jobs():
                self.project.open_job(id=job.id[: aid_len - 1])
        with pytest.raises(KeyError):
            self.project.open_job(id="abc")

    def test_missing_statepoint_file(self):
        job = self.project.open_job(dict(a=0))
        job.init()

        os.remove(job.fn(job.FN_STATE_POINT))

        self.project._sp_cache.clear()
        self.project._remove_persistent_cache_file()
        try:
            logging.disable(logging.CRITICAL)
            with pytest.raises(JobsCorruptedError):
                self.project.open_job(id=job.id).init()
        finally:
            logging.disable(logging.NOTSET)

    def test_corrupted_statepoint_file(self):
        job = self.project.open_job(dict(a=0))
        job.init()

        # Overwrite state point file.
        with open(job.fn(job.FN_STATE_POINT), "w"):
            pass

        self.project._sp_cache.clear()
        self.project._remove_persistent_cache_file()
        try:
            logging.disable(logging.CRITICAL)
            with pytest.raises(JobsCorruptedError):
                # Accessing the job state point triggers validation of the
                # state point file.
                self.project.open_job(id=job.id).statepoint
            with pytest.raises(JobsCorruptedError):
                # Initializing the job state point triggers validation of the
                # state point file.
                self.project.open_job(id=job.id).init()
        finally:
            logging.disable(logging.NOTSET)
        # Ensure that the corrupted state point file still exists.
        assert os.path.exists(job.fn(job.FN_STATE_POINT))

    def test_rename_workspace(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        # First, we move the job to the wrong directory.
        wd = job.path
        wd_invalid = os.path.join(self.project.workspace, "0" * 32)
        os.replace(wd, wd_invalid)  # Move to incorrect id.
        assert not os.path.exists(job.path)

        try:
            logging.disable(logging.CRITICAL)

            # This should raise an error when calling check().
            with pytest.raises(JobsCorruptedError):
                self.project.check()

            # The repair attempt should be successful.
            self.project.repair()
            self.project.check()

            # We corrupt it again, but this time ...
            os.replace(wd, wd_invalid)
            with pytest.raises(JobsCorruptedError):
                self.project.check()
            #  ... we reinitialize the initial job, ...
            try:
                job.init()
            except JobsCorruptedError:
                # ... which raises the JobsCorruptedError in update_cache
                pass
            with pytest.raises(JobsCorruptedError):
                # ... which means the repair attempt must fail.
                self.project.repair()
            with pytest.raises(JobsCorruptedError):
                self.project.check()
            # Some manual clean-up should get things back on track.
            job.remove()
            with pytest.raises(JobsCorruptedError):
                self.project.check()
            self.project.repair()
            self.project.check()
        finally:
            logging.disable(logging.NOTSET)

    @pytest.mark.filterwarnings("ignore:write_statepoint")
    def test_repair_corrupted_workspace(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()

        for i, job in enumerate(self.project):
            pass
        assert i == 4

        self.project.update_cache()

        # This job has no state point file.
        with self.project.open_job(statepoints[0]) as job:
            os.remove(job.FN_STATE_POINT)

        # This job has an empty state point file.
        with self.project.open_job(statepoints[1]) as job:
            with open(job.FN_STATE_POINT, "w"):
                pass

        # Need to clear internal cache to encounter error.
        self.project._sp_cache.clear()

        # disable logging temporarily
        try:
            logging.disable(logging.CRITICAL)

            # Iterating through the jobs should now result in an error.
            with pytest.raises(JobsCorruptedError):
                for job in self.project:
                    # Validate the state point.
                    sp = job.statepoint()
                    assert len(sp) == 1
                    assert sp["a"] in range(5)

            self.project.repair()

            self.project._sp_cache.clear()
            self.project._remove_persistent_cache_file()
            for job in self.project:
                # Validate the state point.
                sp = job.statepoint()
                assert len(sp) == 1
                assert sp["a"] in range(5)
        finally:
            logging.disable(logging.NOTSET)

    @pytest.mark.filterwarnings("ignore:index")
    def test_index(self):
        docs = list(self.project._build_index(include_job_document=True))
        assert len(docs) == 0
        docs = list(self.project._build_index(include_job_document=False))
        assert len(docs) == 0
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document["test"] = True
        job_ids = {job.id for job in self.project.find_jobs()}
        docs = dict(self.project._build_index())
        job_ids_cmp = docs.keys()
        assert job_ids == job_ids_cmp
        assert len(docs) == len(statepoints)

    def test_custom_project(self):
        class CustomProject(signac.Project):
            pass

        project = CustomProject.get_project(path=self.project.path)
        assert isinstance(project, signac.Project)
        assert isinstance(project, CustomProject)

    def test_project_contains(self):
        job = self.open_job(dict(a=0))
        assert job not in self.project
        job.init()
        assert job in self.project

    def test_JobsCursor_contains(self):
        cursor = self.project.find_jobs()
        job = self.open_job(dict(a=0))
        assert job not in cursor
        job.init()
        assert job in cursor

    def test_job_move(self):
        path = self._tmp_dir.name
        project_a = signac.init_project(path=os.path.join(path, "a"))
        project_b = signac.init_project(path=os.path.join(path, "b"))
        job = project_a.open_job(dict(a=0))
        job_b = project_b.open_job(dict(a=0))
        assert job != job_b
        assert job not in project_a
        assert job not in project_b
        job.init()
        assert job in project_a
        assert job not in project_b
        job.move(project_b)
        assert job in project_b
        assert job not in project_a
        assert job == job_b
        assert hash(job) == hash(job_b)
        with job:
            job.document["a"] = 0
            with open("hello.txt", "w") as file:
                file.write("world!")
        job_ = project_b.open_job(job.statepoint())
        assert job == job_
        assert hash(job) == hash(job_)
        assert job_ == job_b
        assert hash(job_) == hash(job_b)
        assert job_.isfile("hello.txt")
        assert job_.document["a"] == 0

    def test_job_clone(self):
        path = self._tmp_dir.name
        project_a = signac.init_project(path=os.path.join(path, "a"))
        project_b = signac.init_project(path=os.path.join(path, "b"))
        job_a = project_a.open_job(dict(a=0))
        assert job_a not in project_a
        assert job_a not in project_b
        with job_a:
            job_a.document["a"] = 0
            with open("hello.txt", "w") as file:
                file.write("world!")
        assert job_a in project_a
        assert job_a not in project_b
        job_b = project_b.clone(job_a)
        assert job_a in project_a
        assert job_a in project_b
        assert job_b in project_a
        assert job_b in project_b
        assert job_a.document == job_b.document
        assert job_a.isfile("hello.txt")
        assert job_b.isfile("hello.txt")
        with pytest.raises(DestinationExistsError):
            project_b.clone(job_a)
        try:
            project_b.clone(job_a)
        except DestinationExistsError as error:
            assert error.destination != job_a
            assert error.destination == job_b

    def test_schema_init(self):
        s = ProjectSchema()
        assert len(s) == 0
        assert not s

    def test_schema(self):
        for i in range(10):
            self.project.open_job(
                {
                    "const1": 0,
                    "const2": {"const3": 0},
                    "const4": {},
                    "a": i,
                    "b": {"b2": i},
                    "c": [i if i % 2 else None, 0, 0],
                    "d": [[i, 0, 0]],
                    "e": {"e2": [i, 0, 0]} if i % 2 else 0,  # heterogeneous!
                    "f": {"f2": [[i, 0, 0]]},
                }
            ).init()

        s = self.project.detect_schema()
        assert len(s) == 10
        for k in (
            "const1",
            "const2.const3",
            "const4",
            "a",
            "b.b2",
            "c",
            "d",
            "e.e2",
            "f.f2",
        ):
            assert k in s
            # The following call should not error out.
            s[k]
        repr(s)
        assert s.format() == str(s)
        s = self.project.detect_schema(exclude_const=True)
        assert len(s) == 7
        assert "const1" not in s
        assert "const2.const3" not in s
        assert "const4" not in s
        assert type not in s["e"]

    def test_schema_subset(self):
        for i in range(5):
            self.project.open_job(dict(a=i)).init()
        s_sub = self.project.detect_schema()
        for i in range(10):
            self.project.open_job(dict(a=i)).init()

        assert s_sub != self.project.detect_schema()
        s = self.project.detect_schema(subset=self.project.find_jobs({"a.$lt": 5}))
        assert s == s_sub
        s = self.project.detect_schema(
            subset=[job.id for job in self.project.find_jobs({"a.$lt": 5})]
        )
        assert s == s_sub

    def test_schema_difference(self):
        def get_sp(i):
            return {
                "const": 0,
                "const2": {"const3": 0},
                "a": i,
                "b": {"b2": i},
                "c": [i, 0, 0],
                "d": [[i, 0, 0]],
                "e": {"e2": [i, 0, 0]},
                "f": {"f2": [[i, 0, 0]]},
            }

        for i in range(10):
            self.project.open_job(get_sp(i)).init()

        s = self.project.detect_schema()
        s2 = self.project.detect_schema()
        s3 = self.project.detect_schema(exclude_const=True)
        s4 = self.project.detect_schema(exclude_const=True)

        assert len(s) == 8
        assert len(s2) == 8
        assert len(s3) == 6
        assert len(s4) == 6

        assert s == s2
        assert s != s3
        assert s != s4
        assert s3 == s4

        assert len(s.difference(s3)) == len(s) - len(s3)
        self.project.open_job(get_sp(11)).init()
        s_ = self.project.detect_schema()
        s3_ = self.project.detect_schema(exclude_const=True)

        assert s != s_
        assert s3 != s3_
        assert s.difference(s_) == s3.difference(s3_)
        assert len(s.difference(s_, ignore_values=True)) == 0
        assert len(s3.difference(s3_, ignore_values=True)) == 0

    def test_schema_format(self):
        for i in range(10):
            self.project.open_job(
                {
                    "const": 0,
                    "a": i,
                    "b": {"b2": i},
                    "c": {"c2": {"c3": {"c4": {"c5": [[i, 0, 0]]}}}},
                }
            ).init()

        s = self.project.detect_schema()
        s_format1 = s.format()
        s_format2 = s.format(depth=2)

        S_FORMAT1 = textwrap.dedent(
            """\
            {
             'a': 'int([0, 1, 2, ..., 8, 9], 10)',
             'b.b2': 'int([0, 1, 2, ..., 8, 9], 10)',
             'c.c2.c3.c4.c5': 'tuple([((0, 0, 0),), ((1, 0, 0),), ((2, 0, 0),), ..., ((8, 0, 0),), ((9, 0, 0),)], 10)',
             'const': 'int([0], 1)',
            }"""  # noqa: E501
        )

        assert s_format1 == S_FORMAT1

        S_FORMAT2 = textwrap.dedent(
            """\
            {'a': 'int([0, 1, 2, ..., 8, 9], 10)',
             'b': {'b2': 'int([0, 1, 2, ..., 8, 9], 10)'},
             'c': {'c2': {...}},
             'const': 'int([0], 1)'}"""
        )

        assert s_format2 == S_FORMAT2

    def test_jobs_groupby(self):
        def get_sp(i):
            return {"a": i, "b": i % 2, "c": i % 3}

        def get_doc(i):
            i += 1
            return {"a": i, "b": i % 2, "c": i % 3}

        for i in range(12):
            job = self.project.open_job(get_sp(i)).init()
            job.document = get_doc(i)

        for k, g in self.project.groupby("a"):
            assert len(list(g)) == 1
            for job in list(g):
                assert job.sp["a"] == k
        for k, g in self.project.groupby("b"):
            assert len(list(g)) == 6
            for job in list(g):
                assert job.sp["b"] == k
        assert len(list(self.project.groupby("d"))) == 0
        for k, g in self.project.groupby("d", default=-1):
            assert k == -1
            assert len(list(g)) == len(self.project)
        for k, g in self.project.groupby(("b", "c")):
            assert len(list(g)) == 2
            for job in list(g):
                assert job.sp["b"] == k[0]
                assert job.sp["c"] == k[1]
        for k, g in self.project.groupby(lambda job: job.sp["a"] % 4):
            assert len(list(g)) == 3
            for job in list(g):
                assert job.sp["a"] % 4 == k
        for k, g in self.project.groupby(lambda job: str(job)):
            assert len(list(g)) == 1
            for job in list(g):
                assert str(job) == k
        group_count = 0
        for k, g in self.project.groupby():
            assert len(list(g)) == 1
            group_count = group_count + 1
            for job in list(g):
                assert str(job) == k
        assert group_count == len(list(self.project.find_jobs()))

        # using sp.key and doc.key
        for k, g in self.project.groupby("sp.a"):
            assert len(list(g)) == 1
            for job in list(g):
                assert job.sp["a"] == k

        assert len(list(self.project.groupby("d"))) == 0
        for k, g in self.project.groupby("sp.d", default=-1):
            assert k == -1
            assert len(list(g)) == len(self.project)

        for k, g in self.project.groupby(("sp.b", "c")):
            assert len(list(g)) == 2
            for job in list(g):
                assert job.sp["b"] == k[0]
                assert job.sp["c"] == k[1]

        for k, g in self.project.groupby("doc.a"):
            assert len(list(g)) == 1
            for job in list(g):
                assert job.document["a"] == k

        for k, g in self.project.groupby("doc.a", default=-1):
            assert len(list(g)) == 1
            for job in list(g):
                assert job.document["a"] == k

        for k, g in self.project.groupby("doc.b"):
            assert len(list(g)) == 6
            for job in list(g):
                assert job.document["b"] == k

        assert len(list(self.project.groupby("doc.d"))) == 0
        for k, g in self.project.groupby("doc.d", default=-1):
            assert k == -1
            assert len(list(g)) == len(self.project)

        for k, g in self.project.groupby(("doc.b", "doc.c")):
            assert len(list(g)) == 2
            for job in list(g):
                assert job.document["b"] == k[0]
                assert job.document["c"] == k[1]

        for k, g in self.project.groupby(("b", "doc.c")):
            assert len(list(g)) == 2
            for job in list(g):
                assert job.sp["b"] == k[0]
                assert job.document["c"] == k[1]

        for k, g in self.project.groupby(("sp.b", "doc.c")):
            assert len(list(g)) == 2
            for job in list(g):
                assert job.sp["b"] == k[0]
                assert job.document["c"] == k[1]

        for k, g in self.project.groupby(lambda job: job.doc["a"] % 4):
            assert len(list(g)) == 3
            for job in list(g):
                assert job.document["a"] % 4 == k

        for k, g in self.project.groupby(lambda job: str(job.doc)):
            assert len(list(g)) == 1
            for job in list(g):
                assert str(job.document) == k

        # Make the schema heterogeneous
        self.project.open_job({"a": 20}).init()
        for k, g in self.project.groupby("b"):
            assert len(list(g)) == 6
            for job in list(g):
                assert job.sp["b"] == k
        for k, g in self.project.groupby(("b", "c")):
            assert len(list(g)) == 2
            for job in list(g):
                assert job.sp["b"] == k[0]
                assert job.sp["c"] == k[1]

        group_count = 0
        for k, g in self.project.groupby(lambda job: job.id):
            assert len(list(g)) == 1
            group_count += 1
            for job in list(g):
                assert str(job) == k
        assert group_count == len(list(self.project.find_jobs()))

    def test_temp_project(self):
        with self.project.temporary_project() as tmp_project:
            assert len(tmp_project) == 0
            tmp_path = tmp_project.path
            assert os.path.isdir(tmp_path)
            for i in range(10):  # init some jobs
                tmp_project.open_job(dict(a=i)).init()
            assert len(tmp_project) == 10
        assert not os.path.isdir(tmp_path)


class TestProjectExportImport(TestProjectBase):
    def test_export(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data)
        assert len(self.project) == 10
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "a", str(i)))
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_single_job(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(1):
            self.project.open_job(dict(a=i)).init()

        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data)
        assert len(self.project) == 1
        assert len(os.listdir(prefix_data)) == 1
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_custom_path_function(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        with pytest.raises(RuntimeError):
            self.project.export_to(target=prefix_data, path=lambda job: "non_unique")

        self.project.export_to(
            target=prefix_data, path=lambda job: os.path.join("my_a", str(job.sp.a))
        )

        assert len(self.project) == 10
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "my_a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "my_a", str(i)))
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_custom_path_string_modify_tree_flat(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        self.project.open_job(
                            dict(a=a_value, b=b_value, c=c_value, d=d_value)
                        ).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        with pytest.raises(RuntimeError):
            self.project.export_to(target=prefix_data, path="non_unique")

        self.project.export_to(
            target=prefix_data, path=os.path.join("a", "{a}", "b", "{b}", "{{auto:_}}")
        )

        assert len(self.project) == 80
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a"))) == 10
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        assert os.path.isdir(
                            os.path.join(
                                prefix_data,
                                "a",
                                str(a_value),
                                "b",
                                str(b_value),
                                "c_%d_d_%d" % (c_value, d_value),
                            )
                        )
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_custom_path_string_modify_tree_tree(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        self.project.open_job(
                            dict(a=a_value, b=b_value, c=c_value, d=d_value)
                        ).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        with pytest.raises(RuntimeError):
            self.project.export_to(target=prefix_data, path="non_unique")

        self.project.export_to(
            target=prefix_data, path=os.path.join("c", "{c}", "b", "{b}", "{{auto}}")
        )

        assert len(self.project) == 80
        assert len(os.listdir(prefix_data)) == 1
        # self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        assert os.path.isdir(
                            os.path.join(
                                prefix_data,
                                "c",
                                str(c_value),
                                "b",
                                str(b_value),
                                "d",
                                str(d_value),
                                "a",
                                str(a_value),
                            )
                        )
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_custom_path_string_modify_flat_flat(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        self.project.open_job(
                            dict(a=a_value, b=b_value, c=c_value, d=d_value)
                        ).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        with pytest.raises(RuntimeError):
            self.project.export_to(target=prefix_data, path="non_unique")

        self.project.export_to(target=prefix_data, path="c_{c}_b_{b}/{{auto:_}}")

        assert len(self.project) == 80
        assert len(os.listdir(prefix_data)) == 4
        # self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        assert os.path.isdir(
                            os.path.join(
                                prefix_data,
                                "c_%d_b_%d" % (c_value, b_value),
                                "d_%d_a_%d" % (d_value, a_value),
                            )
                        )
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_custom_path_string_modify_flat_tree(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        self.project.open_job(
                            dict(a=a_value, b=b_value, c=c_value, d=d_value)
                        ).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        with pytest.raises(RuntimeError):
            self.project.export_to(target=prefix_data, path="non_unique")

        self.project.export_to(target=prefix_data, path="c_{c}_b_{b}/{{auto}}")

        assert len(self.project) == 80
        assert len(os.listdir(prefix_data)) == 4
        # self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for a_value in range(10):
            for b_value in range(2):
                for c_value in range(2):
                    for d_value in range(2):
                        assert os.path.isdir(
                            os.path.join(
                                prefix_data,
                                "c_%d_b_%d/d/%d/a/%d"
                                % (c_value, b_value, d_value, a_value),
                            )
                        )
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_custom_path_string(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        with pytest.raises(RuntimeError):
            self.project.export_to(target=prefix_data, path="non_unique")

        self.project.export_to(
            target=prefix_data, path="my_a/{job.sp.a}"
        )  # why not jus {a}

        assert len(self.project) == 10
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "my_a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "my_a", str(i)))
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_move(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "a", str(i)))
        assert len(self.project.import_from(origin=prefix_data)) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_custom_path_function_move(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        with pytest.raises(RuntimeError):
            self.project.export_to(
                target=prefix_data, path=lambda job: "non_unique", copytree=os.replace
            )

        self.project.export_to(
            target=prefix_data,
            path=lambda job: os.path.join("my_a", str(job.sp.a)),
            copytree=os.replace,
        )

        assert len(self.project) == 0
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "my_a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "my_a", str(i)))
        assert len(self.project.import_from(origin=prefix_data)) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_tarfile(self):
        target = os.path.join(self._tmp_dir.name, "data.tar")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}

        self.project.export_to(target=target)
        assert len(self.project) == 10
        with TarFile(name=target) as tarfile:
            for i in range(10):
                assert f"a/{i}" in tarfile.getnames()
        os.replace(self.project.workspace, self.project.workspace + "~")
        assert len(self.project) == 0
        self.project.import_from(origin=target)
        assert len(self.project) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_tarfile_zipped_longname(self):
        """Test the behavior of tarfile export when the path is >100 chars."""
        target = os.path.join(self._tmp_dir.name, "data.tar.gz")
        val_length = 100
        self.project.open_job(dict(a="1" * val_length)).init()
        self.project.open_job(dict(a="2" * val_length)).init()
        self.project.export_to(target=target)
        # Jobs are always copied, not moved, when writing to a tarfile, so we
        # must remove them manually to ensure that they're regenerated.
        for job in self.project:
            job.remove()
        self.project.import_from(origin=target)
        assert len(self.project) == 2

    def test_export_import_tarfile_zipped(self):
        target = os.path.join(self._tmp_dir.name, "data.tar.gz")
        for i in range(10):
            with self.project.open_job(dict(a=i)) as job:
                os.makedirs(job.fn("sub-dir"))
                with open(
                    job.fn(os.path.join("sub-dir", "signac_statepoint.json")), "w"
                ) as file:
                    file.write(json.dumps({"foo": 0}))
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=target)
        assert len(self.project) == 10
        with TarFile.open(name=target, mode="r:gz") as tarfile:
            for i in range(10):
                assert f"a/{i}" in tarfile.getnames()
                assert f"a/{i}/sub-dir/signac_statepoint.json" in tarfile.getnames()
        os.replace(self.project.workspace, self.project.workspace + "~")
        assert len(self.project) == 0
        self.project.import_from(origin=target)
        assert len(self.project) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

        for job in self.project:
            assert job.isfile(os.path.join("sub-dir", "signac_statepoint.json"))

    def test_export_import_zipfile(self):
        target = os.path.join(self._tmp_dir.name, "data.zip")
        for i in range(10):
            with self.project.open_job(dict(a=i)) as job:
                os.makedirs(job.fn("sub-dir"))
                with open(
                    job.fn(os.path.join("sub-dir", "signac_statepoint.json")), "w"
                ) as file:
                    file.write(json.dumps({"foo": 0}))
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=target)
        assert len(self.project) == 10
        with ZipFile(target) as zipfile:
            for i in range(10):
                assert f"a/{i}/signac_statepoint.json" in zipfile.namelist()
                assert f"a/{i}/sub-dir/signac_statepoint.json" in zipfile.namelist()
        os.replace(self.project.workspace, self.project.workspace + "~")
        assert len(self.project) == 0
        self.project.import_from(origin=target)
        assert len(self.project) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}
        for job in self.project:
            assert job.isfile(os.path.join("sub-dir", "signac_statepoint.json"))

    def test_export_import(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project.import_from(prefix_data)) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_conflict(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data)
        with pytest.raises(DestinationExistsError):
            assert len(self.project.import_from(prefix_data)) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_conflict_synced(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data)
        with pytest.raises(DestinationExistsError):
            assert len(self.project.import_from(prefix_data)) == 10
        with self.project.temporary_project() as tmp_project:
            assert len(tmp_project.import_from(prefix_data)) == 10
            assert len(tmp_project) == 10
            self.project.sync(tmp_project)
        assert ids_before_export == {job.id for job in self.project.find_jobs()}
        assert len(self.project.import_from(prefix_data, sync=True)) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_conflict_synced_with_args(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data)
        with pytest.raises(DestinationExistsError):
            assert len(self.project.import_from(prefix_data)) == 10

        selection = list(self.project.find_jobs(dict(a=0)))
        os.replace(self.project.workspace, self.project.workspace + "~")
        assert len(self.project) == 0
        assert (
            len(self.project.import_from(prefix_data, sync=dict(selection=selection)))
            == 10
        )
        assert len(self.project) == 1
        assert len(self.project.find_jobs(dict(a=0))) == 1
        assert next(iter(self.project.find_jobs())).id in ids_before_export

    def test_export_import_schema_callable(self):
        def my_schema(path):
            re_sep = re.escape(os.path.sep)
            m = re.match(r".*" + re_sep + "a" + re_sep + r"(?P<a>\d+)$", path)
            if m:
                return dict(a=int(m.groupdict()["a"]))

        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project.import_from(prefix_data, schema=my_schema)) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_schema_callable_non_unique(self):
        def my_schema_non_unique(path):
            re_sep = re.escape(os.path.sep)
            m = re.match(r".*" + re_sep + "a" + re_sep + r"(?P<a>\d+)$", path)
            if m:
                return dict(a=0)

        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        self.project.export_to(target=prefix_data, copytree=os.replace)
        with pytest.raises(RuntimeError):
            self.project.import_from(prefix_data, schema=my_schema_non_unique)

    def test_export_import_simple_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "a", str(i)))
        with pytest.raises(StatepointParsingError):
            self.project.import_from(origin=prefix_data, schema="a/{b:int}")
        assert len(self.project.import_from(prefix_data)) == 10
        assert len(self.project) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_simple_path_nested_with_schema(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=dict(b=dict(c=i)))).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a.b.c"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "a.b.c", str(i)))
        with pytest.raises(StatepointParsingError):
            self.project.import_from(origin=prefix_data, schema="a.b.c/{a.b:int}")
        assert (
            len(
                self.project.import_from(origin=prefix_data, schema="a.b.c/{a.b.c:int}")
            )
            == 10
        )
        assert len(self.project) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_simple_path_with_float(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=float(i))).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "a", str(float(i))))
        assert len(self.project.import_from(prefix_data)) == 10
        assert len(self.project) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_complex_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        sp_0 = [{"a": i, "b": i % 3} for i in range(5)]
        sp_1 = [{"a": i, "b": i % 3, "c": {"a": i, "b": 0}} for i in range(5)]
        sp_2 = [
            {"a": i, "b": i % 3, "c": {"a": i, "b": 0, "c": {"a": i, "b": 0}}}
            for i in range(5)
        ]
        statepoints = sp_0 + sp_1 + sp_2
        for sp in statepoints:
            self.project.open_job(sp).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        self.project.import_from(prefix_data)
        assert len(self.project) == len(statepoints)
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_simple_path_schema_from_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "a", str(i)))
        ret = self.project.import_from(origin=prefix_data, schema="a/{a:int}")
        assert len(ret) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_simple_path_schema_from_path_float(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        for i in range(10):
            self.project.open_job(dict(a=float(i))).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, "a"))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, "a", str(float(i))))
        ret = self.project.import_from(origin=prefix_data, schema="a/{a:int}")
        assert len(ret) == 0  # should not match
        ret = self.project.import_from(origin=prefix_data, schema="a/{a:float}")
        assert len(ret) == 10
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_export_import_complex_path_nested_schema_from_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, "data")
        statepoints = [{"a": i, "b": {"c": i % 3}} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.export_to(target=prefix_data, copytree=os.replace)
        assert len(self.project) == 0
        self.project.import_from(origin=prefix_data, schema="b.c/{b.c:int}/a/{a:int}")
        assert len(self.project) == len(statepoints)
        assert ids_before_export == {job.id for job in self.project.find_jobs()}

    def test_import_own_project(self):
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = {job.id for job in self.project.find_jobs()}
        self.project.import_from(origin=self.project.workspace)
        assert ids_before_export == {job.id for job in self.project.find_jobs()}
        with self.project.temporary_project() as tmp_project:
            tmp_project.import_from(origin=self.project.workspace)
            assert ids_before_export == {job.id for job in self.project.find_jobs()}
            assert len(tmp_project) == len(self.project)


VALID_SP_VALUES = [None, 0, 1, 0.0, 1.0, True, False, [0, 1, 2], [0, 1.0, False]]


def add_jobs_homogeneous(project, num_jobs):
    # Add jobs with many different state points
    for i in range(num_jobs):
        project.open_job({f"{i}_{j}": v for j, v in enumerate(VALID_SP_VALUES)}).init()


def add_jobs_heterogeneous(project, num_jobs):
    # Add jobs with many different state points
    for i in range(num_jobs):
        for v in VALID_SP_VALUES:
            project.open_job(dict(a=v)).init()


project_repr_generators = [
    (add_jobs_homogeneous, 0),
    (add_jobs_homogeneous, 10),
    (add_jobs_homogeneous, 200),
    (add_jobs_heterogeneous, 0),
    (add_jobs_heterogeneous, 10),
    (add_jobs_heterogeneous, 200),
]


class TestProjectRepresentation(TestProjectBase):
    num_few_jobs = 10
    num_many_jobs = 200

    @pytest.mark.parametrize("project_generator,num_jobs", project_repr_generators)
    def test_project_repr_methods(self, project_generator, num_jobs):
        project_generator(self.project, num_jobs)
        assert len(str(self.project)) > 0
        assert len(repr(self.project)) > 0
        assert eval(repr(self.project)) == self.project
        for use_pandas in (True, False):
            type(self.project)._use_pandas_for_html_repr = use_pandas
            if use_pandas and not PANDAS:
                raise pytest.skip("requires use_pandas")
            self.project._repr_html_()

    @pytest.mark.parametrize("project_generator,num_jobs", project_repr_generators)
    def test_JobsCursor_repr_methods(self, project_generator, num_jobs):
        project_generator(self.project, num_jobs)
        for filter_ in (None,):
            assert len(str(self.project.find_jobs(filter_))) > 0
            assert len(repr(self.project.find_jobs(filter_))) > 0
            q = self.project.find_jobs(filter_)
            print(q)
            assert eval(repr(q)) == q
            for use_pandas in (True, False):
                type(self.project)._use_pandas_for_html_repr = use_pandas
                if use_pandas and not PANDAS:
                    raise pytest.skip("requires use_pandas")
                self.project.find_jobs(filter_)._repr_html_()

    @pytest.mark.parametrize("project_generator,num_jobs", project_repr_generators)
    def test_Schema_repr_methods(self, project_generator, num_jobs):
        project_generator(self.project, num_jobs)
        schema = self.project.detect_schema()
        assert len(str(schema)) > 0
        assert len(repr(schema)) > 0
        schema._repr_html_()


class TestLinkedViewProject(TestProjectBase):
    @skip_windows_without_symlinks
    def test_create_linked_view(self):
        def clean(filter=None):
            """Helper function for wiping out views"""
            for job in self.project.find_jobs(filter):
                job.remove()
            self.project.create_linked_view(prefix=view_prefix)

        sp_0 = [{"a": i, "b": i % 3} for i in range(5)]
        sp_1 = [{"a": i, "b": i % 3, "c": {"a": i, "b": 0}} for i in range(5)]
        sp_2 = [
            {"a": i, "b": i % 3, "c": {"a": i, "b": 0, "c": {"a": i, "b": 0}}}
            for i in range(5)
        ]
        statepoints = sp_0 + sp_1 + sp_2
        view_prefix = os.path.join(self._tmp_pr, "view")
        # empty project
        self.project.create_linked_view(prefix=view_prefix)
        # one job
        self.project.open_job(statepoints[0]).init()
        self.project.create_linked_view(prefix=view_prefix)
        # more jobs
        for sp in statepoints:
            self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)
        assert os.path.isdir(view_prefix)
        all_links = list(_find_all_links(view_prefix))
        dst = set(
            map(
                lambda link: os.path.realpath(os.path.join(view_prefix, link, "job")),
                all_links,
            )
        )
        src = set(map(lambda j: os.path.realpath(j.path), self.project.find_jobs()))
        assert len(all_links) == len(self.project)
        self.project.create_linked_view(prefix=view_prefix)
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == len(self.project)
        dst = set(
            map(
                lambda link: os.path.realpath(os.path.join(view_prefix, link, "job")),
                all_links,
            )
        )
        src = set(map(lambda j: os.path.realpath(j.path), self.project.find_jobs()))
        assert src == dst
        # update with subset
        job_subset = self.project.find_jobs({"b": 0})
        id_subset = [job.id for job in job_subset]

        self.project.create_linked_view(prefix=view_prefix, job_ids=id_subset)
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == len(id_subset)
        dst = set(
            map(
                lambda link: os.path.realpath(os.path.join(view_prefix, link, "job")),
                all_links,
            )
        )
        src = set(map(lambda j: os.path.realpath(j.path), job_subset))
        assert src == dst
        # some jobs removed
        clean({"b": 0})
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == len(self.project)
        dst = set(
            map(
                lambda link: os.path.realpath(os.path.join(view_prefix, link, "job")),
                all_links,
            )
        )
        src = set(map(lambda j: os.path.realpath(j.path), self.project.find_jobs()))
        assert src == dst
        # all jobs removed
        clean()
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == len(self.project)
        dst = set(
            map(
                lambda link: os.path.realpath(os.path.join(view_prefix, link, "job")),
                all_links,
            )
        )
        src = set(map(lambda j: os.path.realpath(j.path), self.project.find_jobs()))
        assert src == dst

    @skip_windows_without_symlinks
    def test_create_linked_view_homogeneous_schema_tree(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "c",
                            str(sp["c"]),
                            "b",
                            str(sp["b"]),
                            "a",
                            str(sp["a"]),
                            "job",
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_homogeneous_schema_tree_tree(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path="a/{a}/{{auto}}")

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "a",
                            str(sp["a"]),
                            "c",
                            str(sp["c"]),
                            "b",
                            str(sp["b"]),
                            "job",
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_homogeneous_schema_tree_flat(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path="a/{a}/{{auto:_}}")

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "a",
                            str(sp["a"]),
                            "c_{}_b_{}".format(str(sp["c"]), str(sp["b"])),
                            "job",
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_homogeneous_schema_flat_flat(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path="a_{a}/{{auto:_}}")

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "b": b, "c": c}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "a_{}/c_{}_b_{}".format(
                                str(sp["a"]), str(sp["c"]), str(sp["b"])
                            ),
                            "job",
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_homogeneous_schema_flat_tree(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        d_vals = ["rock", "paper", "scissors"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    for d in d_vals:
                        sp = {"a": a, "b": b, "c": c, "d": d}
                        self.project.open_job(sp).init()

        self.project.create_linked_view(prefix=view_prefix, path="a_{a}/{{auto}}")

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    for d in d_vals:
                        sp = {"a": a, "b": b, "c": c, "d": d}
                        assert os.path.isdir(
                            os.path.join(
                                view_prefix,
                                "a_%s" % str(sp["a"]),
                                "c",
                                str(sp["c"]),
                                "d",
                                str(sp["d"]),
                                "b",
                                str(sp["b"]),
                                "job",
                            )
                        )

    @skip_windows_without_symlinks
    def test_create_linked_view_homogeneous_schema_nested(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(2)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "d": {"b": b, "c": c}}
                    self.project.open_job(sp).init()

        self.project.create_linked_view(prefix=view_prefix)

        # check all dir:
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "d": {"b": b, "c": c}}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "a",
                            str(sp["a"]),
                            "d.c",
                            str(sp["d"]["c"]),
                            "d.b",
                            str(sp["d"]["b"]),
                            "job",
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_homogeneous_schema_nested_provide_partial_path(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(2)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "d": {"b": b, "c": c}}
                    self.project.open_job(sp).init()

        # Should error if user-provided path doesn't make 1-1 mapping
        with pytest.raises(RuntimeError):
            self.project.create_linked_view(
                prefix=view_prefix, path=os.path.join("a", "{a}")
            )

        self.project.create_linked_view(
            prefix=view_prefix, path="a/{a}/d.c/{d.c}/{{auto}}"
        )

        # check all dir:
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {"a": a, "d": {"b": b, "c": c}}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "a",
                            str(sp["a"]),
                            "d.c",
                            str(sp["d"]["c"]),
                            "d.b",
                            str(sp["d"]["b"]),
                            "job",
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_heterogeneous_disjoint_schema(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(5)
        b_vals = range(3, 13)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                sp = {"a": a, "b": b}
                self.project.open_job(sp).init()
            for c in c_vals:
                sp = {"a": a, "c": c}
                self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        # test each directory
        for a in a_vals:
            for b in b_vals:
                sp = {"a": a, "b": b}
                assert os.path.isdir(
                    os.path.join(
                        view_prefix, "a", str(sp["a"]), "b", str(sp["b"]), "job"
                    )
                )
            for c in c_vals:
                sp = {"a": a, "c": c}
                assert os.path.isdir(
                    os.path.join(view_prefix, "c", sp["c"], "a", str(sp["a"]), "job")
                )

    @skip_windows_without_symlinks
    def test_create_linked_view_heterogeneous_disjoint_schema_nested(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(2)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                sp = {"a": a, "d": {"b": b}}
                self.project.open_job(sp).init()
            for c in c_vals:
                sp = {"a": a, "d": {"c": c}}
                self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                sp = {"a": a, "d": {"b": b}}
                assert os.path.isdir(
                    os.path.join(
                        view_prefix, "a", str(sp["a"]), "d.b", str(sp["d"]["b"]), "job"
                    )
                )
            for c in c_vals:
                sp = {"a": a, "d": {"c": c}}
                assert os.path.isdir(
                    os.path.join(
                        view_prefix, "a", str(sp["a"]), "d.c", sp["d"]["c"], "job"
                    )
                )

    @skip_windows_without_symlinks
    def test_create_linked_view_heterogeneous_fizz_schema_flat(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(5)
        b_vals = range(5)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    if a % 3 == 0:
                        sp = {"a": a, "b": b}
                    else:
                        sp = {"a": a, "b": b, "c": c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    if a % 3 == 0:
                        sp = {"a": a, "b": b}
                        assert os.path.isdir(
                            os.path.join(
                                view_prefix, "a", str(sp["a"]), "b", str(sp["b"]), "job"
                            )
                        )
                    else:
                        sp = {"a": a, "b": b, "c": c}
                        assert os.path.isdir(
                            os.path.join(
                                view_prefix,
                                "c",
                                sp["c"],
                                "a",
                                str(sp["a"]),
                                "b",
                                str(sp["b"]),
                                "job",
                            )
                        )

    @skip_windows_without_symlinks
    def test_create_linked_view_heterogeneous_schema_nested(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(5)
        b_vals = range(10)
        for a in a_vals:
            for b in b_vals:
                if a % 3 == 0:
                    sp = {"a": a, "b": {"c": b}}
                else:
                    sp = {"a": a, "b": b}
                self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                if a % 3 == 0:
                    sp = {"a": a, "b": {"c": b}}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "a",
                            str(sp["a"]),
                            "b.c",
                            str(sp["b"]["c"]),
                            "job",
                        )
                    )
                else:
                    sp = {"a": a, "b": b}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix, "a", str(sp["a"]), "b", str(sp["b"]), "job"
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_heterogeneous_schema_nested_partial_homogenous_path_provide(
        self,
    ):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(5)
        b_vals = range(10)
        d_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for d in d_vals:
                for b in b_vals:
                    if a % 3 == 0:
                        sp = {"a": a, "b": {"c": b}, "d": d}
                    else:
                        sp = {"a": a, "b": b, "d": d}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path="d/{d}/{{auto}}")

        for a in a_vals:
            for b in b_vals:
                if a % 3 == 0:
                    sp = {"a": a, "b": {"c": b}, "d": d}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "d",
                            sp["d"],
                            "a",
                            str(sp["a"]),
                            "b.c",
                            str(sp["b"]["c"]),
                            "job",
                        )
                    )
                else:
                    sp = {"a": a, "b": b, "d": d}
                    assert os.path.isdir(
                        os.path.join(
                            view_prefix,
                            "d",
                            sp["d"],
                            "a",
                            str(sp["a"]),
                            "b",
                            str(sp["b"]),
                            "job",
                        )
                    )

    @skip_windows_without_symlinks
    def test_create_linked_view_heterogeneous_schema_problematic(self):
        self.project.open_job(dict(a=1)).init()
        self.project.open_job(dict(a=1, b=1)).init()
        view_prefix = os.path.join(self._tmp_pr, "view")
        with pytest.raises(RuntimeError):
            self.project.create_linked_view(view_prefix)

    @skip_windows_without_symlinks
    def test_create_linked_view_with_slash_raises_error(self):
        statepoint = {"b": f"bad{os.sep}val"}
        view_prefix = os.path.join(self._tmp_pr, "view")
        self.project.open_job(statepoint).init()
        with pytest.raises(RuntimeError):
            self.project.create_linked_view(prefix=view_prefix)

    @skip_windows_without_symlinks
    def test_create_linked_view_weird_chars_in_file_name(self):
        shell_escaped_chars = [" ", "~"]
        if not WINDOWS:
            shell_escaped_chars.append("*")
        statepoints = [
            {f"a{i}b": 0, "b": f"escaped{i}val"} for i in shell_escaped_chars
        ]
        view_prefix = os.path.join(self._tmp_pr, "view")
        for sp in statepoints:
            self.project.open_job(sp).init()
            self.project.create_linked_view(prefix=view_prefix)

    @skip_windows_without_symlinks
    def test_create_linked_view_duplicate_paths(self):
        view_prefix = os.path.join(self._tmp_pr, "view")
        a_vals = range(2)
        b_vals = range(3, 8)
        for a in a_vals:
            for b in b_vals:
                sp = {"a": a, "b": b}
                self.project.open_job(sp).init()

        # An error should be raised if the user-provided path function doesn't
        # make a 1-1 mapping.
        with pytest.raises(RuntimeError):
            self.project.create_linked_view(
                prefix=view_prefix, path=os.path.join("a", "{a}")
            )


class UpdateCacheAfterInitJob(signac.job.Job):
    """Test job class that updates the project cache on job init."""

    def init(self, *args, **kwargs):
        job = super().init(*args, **kwargs)
        self._project.update_cache()
        return job


class UpdateCacheAfterInitJobProject(signac.Project):
    """Test project class that updates the project cache on job init."""

    def open_job(self, *args, **kwargs):
        job = super().open_job(*args, **kwargs)
        cache_updating_job = UpdateCacheAfterInitJob(
            job._project, job.statepoint(), job._id
        )
        return cache_updating_job


class TestCachedProject(TestProject):
    project_class = UpdateCacheAfterInitJobProject

    def test_repr(self):
        repr(self)


class TestProjectInit:
    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)

    def test_get_project(self):
        path = self._tmp_dir.name
        with pytest.raises(LookupError):
            signac.get_project(path="a/path/that/does/not/exist")
        with pytest.raises(LookupError):
            signac.get_project(path=path)
        project = signac.init_project(path=path)
        assert project.workspace == os.path.join(path, "workspace")
        assert project.path == path
        project = signac.Project.init_project(path=path)
        assert project.workspace == os.path.join(path, "workspace")
        assert project.path == path
        project = signac.get_project(path=path)
        assert project.workspace == os.path.join(path, "workspace")
        assert project.path == path
        project = signac.Project.get_project(path=path)
        assert project.workspace == os.path.join(path, "workspace")
        assert project.path == path

    def test_get_project_non_local(self):
        path = self._tmp_dir.name
        subdir = os.path.join(path, "subdir")
        os.mkdir(subdir)
        project = signac.init_project(path=path)
        assert project == project.get_project(path=path)
        assert project == signac.get_project(path=path)
        assert project == project.get_project(path=path, search=False)
        assert project == signac.get_project(path=path, search=False)
        try:
            assert project == project.get_project(
                path=os.path.relpath(path), search=False
            )
        except ValueError:
            # A relative path may not exist on Windows if it crosses drives.
            pass
        try:
            assert project == signac.get_project(
                path=os.path.relpath(path), search=False
            )
        except ValueError:
            # A relative path may not exist on Windows if it crosses drives.
            pass
        with pytest.raises(LookupError):
            assert project == project.get_project(path=subdir, search=False)
        with pytest.raises(LookupError):
            assert project == signac.get_project(path=subdir, search=False)
        assert project == project.get_project(path=subdir, search=True)
        assert project == signac.get_project(path=subdir, search=True)

    def test_init(self):
        path = self._tmp_dir.name
        with pytest.raises(LookupError):
            signac.get_project(path=path)
        project = signac.init_project(path=path)
        assert project.workspace == os.path.join(path, "workspace")
        assert project.path == path
        # Second initialization should not make any difference.
        project = signac.init_project(path=path)
        project = signac.get_project(path=path)
        assert project.workspace == os.path.join(path, "workspace")
        assert project.path == path
        project = signac.Project.get_project(path=path)
        assert project.workspace == os.path.join(path, "workspace")
        assert project.path == path

    def test_nested_project(self):
        def check_path(path=None):
            if path is None:
                path = os.getcwd()
            assert os.path.realpath(
                signac.get_project(path=path).path
            ) == os.path.realpath(path)

        path = self._tmp_dir.name
        path_a = os.path.join(path, "project_a")
        path_b = os.path.join(path_a, "project_b")
        signac.init_project(path=path_a)
        check_path(path_a)
        signac.init_project(path=path_b)
        check_path(path_b)
        cwd = os.getcwd()
        try:
            os.chdir(path_a)
            check_path()
        finally:
            os.chdir(cwd)
        try:
            os.chdir(path_b)
            check_path()
        finally:
            os.chdir(cwd)

    def test_get_job_valid_workspace(self):
        # Test case: The path is the job workspace path.
        path = self._tmp_dir.name
        project = signac.init_project(path=path)
        job = project.open_job({"a": 1})
        job.init()
        with job:
            # The context manager enters the working directory of the job
            assert project.get_job() == job
            assert signac.get_job() == job

    def test_get_job_invalid_workspace(self):
        # Test case: The path is not the job workspace path.
        path = self._tmp_dir.name
        project = signac.init_project(path=path)
        job = project.open_job({"a": 1})
        job.init()
        # We shouldn't be able to find a job while in the workspace directory,
        # since no signac_statepoint.json exists.
        cwd = os.getcwd()
        try:
            os.chdir(project.workspace)
            with pytest.raises(LookupError):
                project.get_job()
            with pytest.raises(LookupError):
                signac.get_job()
        finally:
            os.chdir(cwd)

    def test_get_job_nested_project(self):
        # Test case: The job workspace dir is also a project dir.
        path = self._tmp_dir.name
        project = signac.init_project(path=path)
        job = project.open_job({"a": 1})
        job.init()
        with job:
            nestedproject = signac.init_project()
            nestedproject.open_job({"b": 2}).init()
            assert project.get_job() == job
            assert signac.get_job() == job

    def test_get_job_subdir(self):
        # Test case: Get a job from a sub-directory of the job workspace dir.
        path = self._tmp_dir.name
        project = signac.init_project(path=path)
        job = project.open_job({"a": 1})
        job.init()
        with job:
            os.mkdir("test_subdir")
            assert project.get_job("test_subdir") == job
            assert signac.get_job("test_subdir") == job
        assert project.get_job(job.fn("test_subdir")) == job
        assert signac.get_job(job.fn("test_subdir")) == job

    def test_get_job_nested_project_subdir(self):
        # Test case: Get a job from a sub-directory of the job workspace dir
        # when the job directory is also a project directory
        path = self._tmp_dir.name
        project = signac.init_project(path=path)
        job = project.open_job({"a": 1})
        job.init()
        with job:
            nestedproject = signac.init_project()
            nestedproject.open_job({"b": 2}).init()
            os.mkdir("test_subdir")
            assert project.get_job("test_subdir") == job
            assert signac.get_job("test_subdir") == job
        assert project.get_job(job.fn("test_subdir")) == job
        assert signac.get_job(job.fn("test_subdir")) == job

    @skip_windows_without_symlinks
    def test_get_job_symlink_other_project(self):
        # Test case: Get a job from a symlink in another project workspace
        path = self._tmp_dir.name
        project_a_dir = os.path.join(path, "project_a")
        project_b_dir = os.path.join(path, "project_b")
        os.mkdir(project_a_dir)
        os.mkdir(project_b_dir)
        project_a = signac.init_project(path=project_a_dir)
        project_b = signac.init_project(path=project_b_dir)
        job_a = project_a.open_job({"a": 1})
        job_a.init()
        job_b = project_b.open_job({"b": 1})
        job_b.init()
        symlink_path = os.path.join(project_b.workspace, job_a._id)
        os.symlink(job_a.path, symlink_path)
        assert project_a.get_job(symlink_path) == job_a
        assert project_b.get_job(symlink_path) == job_a
        assert signac.get_job(symlink_path) == job_a


class TestProjectSchema(TestProjectBase):
    def test_project_schema_versions(self):
        from signac.migration import apply_migrations

        # Ensure that project initialization fails on an unsupported version.
        impossibly_high_schema_version = "9999"
        assert version.parse(self.project.config["schema_version"]) < version.parse(
            impossibly_high_schema_version
        )
        config = _read_config_file(_get_project_config_fn(self.project.path))
        config["schema_version"] = impossibly_high_schema_version
        config.write()
        with pytest.raises(IncompatibleSchemaVersion):
            signac.init_project(path=self.project.path)

        # Ensure that migration fails on an unsupported version.
        with pytest.raises(RuntimeError):
            apply_migrations(self.project.path)

    @pytest.mark.skip(reason="Fails when test system has no config file..")
    def test_no_migration(self):
        # This unit test should fail as long as there are no schema migrations
        # implemented within the signac.migration package.
        #
        # Once migrations are implemented:
        #
        # 1. Ensure to enable the 'migrate' sub-command within the __main__ module.
        # 2. Either update or remove this unit test.
        from signac.migration import _collect_migrations

        migrations = list(_collect_migrations(self.project.path))
        assert len(migrations) == 0


def _initialize_v1_project(dirname, with_workspace=True, with_other_files=True):
    # Create v1 config file.
    cfg_fn = os.path.join(dirname, "signac.rc")
    workspace_dir = "workspace_dir"
    with open(cfg_fn, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                project = project
                workspace_dir = {workspace_dir}
                schema_version = 0"""
            )
        )

    # Create a custom workspace
    os.makedirs(os.path.join(dirname, workspace_dir))
    if with_workspace:
        os.makedirs(os.path.join(dirname, "workspace"))

    if with_other_files:
        # Create a shell history file.
        history_fn = os.path.join(dirname, ".signac_shell_history")
        with open(history_fn, "w") as f:
            f.write("print(project)")

        # Create a statepoint cache. Note that this cache does not
        # correspond to actual statepoints since we don't currently have
        # any in this project, but that's fine for migration testing.
        sp_cache = os.path.join(dirname, ".signac_sp_cache.json.gz")
        sp = {"a": 1}
        with gzip.open(sp_cache, "wb") as f:
            f.write(json.dumps({calc_id(sp): sp}).encode())

    return cfg_fn


class TestSchemaMigration:
    @pytest.mark.parametrize("implicit_version", [True, False])
    @pytest.mark.parametrize("workspace_exists", [True, False])
    @pytest.mark.parametrize("with_other_files", [True, False])
    def test_project_schema_version_migration(
        self, implicit_version, workspace_exists, with_other_files
    ):
        from signac.migration import apply_migrations

        with TemporaryDirectory() as dirname:
            cfg_fn = _initialize_v1_project(dirname, workspace_exists, with_other_files)

            # If no schema version is present in the config it is equivalent to
            # version 0, so we test both explicit and implicit versions.
            config = _read_config_file(cfg_fn)
            if implicit_version:
                del config["schema_version"]
                assert "schema_version" not in config
            else:
                assert config["schema_version"] == "0"
            config.write()

            # If the 'workspace' directory already exists the migration should fail.
            if workspace_exists:
                with pytest.raises(RuntimeError):
                    apply_migrations(dirname)
                return

            err = io.StringIO()
            with redirect_stderr(err):
                apply_migrations(dirname)
            config = _load_config(dirname)
            assert config["schema_version"] == "2"
            project = signac.get_project(path=dirname)
            assert project.config["schema_version"] == "2"
            assert "OK" in err.getvalue()
            assert "0 to 1" in err.getvalue()
            assert "1 to 2" in err.getvalue()
            assert os.path.isfile(project.fn(PROJECT_CONFIG_FN))
            if with_other_files:
                assert os.path.isfile(
                    project.fn(os.sep.join((".signac", "shell_history")))
                )
                assert os.path.isfile(project.fn(Project.FN_CACHE))

    def test_project_init_old_schema(self):
        with TemporaryDirectory() as dirname:
            _initialize_v1_project(dirname)

            # Initializing a project should detect the incompatible schema.
            with pytest.raises(IncompatibleSchemaVersion):
                signac.get_project(dirname)

            with pytest.raises(IncompatibleSchemaVersion):
                signac.init_project(dirname)

            with pytest.raises(IncompatibleSchemaVersion):
                signac.Project(dirname)


class TestProjectPickling(TestProjectBase):
    def test_pickle_project_empty(self):
        blob = pickle.dumps(self.project)
        assert pickle.loads(blob) == self.project

    def test_pickle_project_with_jobs(self):
        for i in range(3):
            self.project.open_job(
                dict(a=i, b=dict(c=i), d=list(range(i, i + 3)))
            ).init()
        blob = pickle.dumps(self.project)
        assert pickle.loads(blob) == self.project

    def test_pickle_jobs_directly(self):
        for i in range(3):
            self.project.open_job(
                dict(a=i, b=dict(c=i), d=list(range(i, i + 3)))
            ).init()
        for job in self.project:
            assert pickle.loads(pickle.dumps(job)) == job


class TestProjectStoreBase(test_h5store.TestH5StoreBase):
    project_class = signac.Project

    @pytest.fixture(autouse=True)
    def setUp_base_h5Store(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, "pr")
        os.mkdir(self._tmp_pr)
        self.config = _load_config()
        self.project = self.project_class.init_project(path=self._tmp_pr)

        self._fn_store = os.path.join(self._tmp_dir.name, "signac_data.h5")
        self._fn_store_other = os.path.join(self._tmp_dir.name, "other.h5")

    def get_h5store(self):
        return self.project.data

    @contextmanager
    def open_h5store(self, **kwargs):
        with self.get_h5store().open(**kwargs) as h5s:
            yield h5s

    def get_other_h5store(self):
        return self.project.stores["other"]

    @contextmanager
    def open_other_h5store(self, **kwargs):
        with self.get_other_h5store().open(**kwargs) as h5s:
            yield h5s


class TestProjectStore(TestProjectStoreBase, test_h5store.TestH5Store):

    """
    This test opens multiple instances of H5Store, but
    the project data interface opens one instance of H5Store.
    This test will (and should) fail using the project data interface.
    """

    def test_assign_valid_types_within_same_file(self):
        pass


class TestProjectStoreOpen(TestProjectStoreBase, test_h5store.TestH5StoreOpen):

    """
    This test opens multiple instances of H5Store, but
    the project data interface opens one instance of H5Store.
    This test will (and should) fail using the project data interface.
    """

    def test_open_write_and_read_only(self):
        pass


class TestProjectStoreNestedData(TestProjectStore, test_h5store.TestH5StoreNestedData):
    pass


class TestProjectStoreBytes(TestProjectStore, test_h5store.TestH5StoreBytesData):
    pass


class TestProjectStoreClosed(TestProjectStore, test_h5store.TestH5StoreClosed):
    pass


class TestProjectStoreNestedDataClosed(
    TestProjectStoreNestedData, test_h5store.TestH5StoreNestedDataClosed
):
    pass


class TestProjectStorePandasData(TestProjectStore, test_h5store.TestH5StorePandasData):
    pass


class TestProjectStoreNestedPandasData(
    TestProjectStorePandasData, test_h5store.TestH5StoreNestedPandasData
):
    pass


class TestProjectStoreMultiThreading(
    TestProjectStoreBase, test_h5store.TestH5StoreMultiThreading
):
    pass


class TestProjectStoreMultiProcessing(
    TestProjectStoreBase, test_h5store.TestH5StoreMultiProcessing
):

    """
    These tests open multiple instances of H5Store, but
    the project data interface opens one instance of H5Store.
    Theses tests will (and should) fail using the project data interface.
    """

    @contextmanager
    def open_h5store(self, **kwargs):
        with signac.H5Store(self.project.fn("signac_data.h5")) as h5:
            yield h5

    def test_single_writer_multiple_reader_same_instance(self):
        pass

    def test_multiple_reader_different_process_no_swmr(self):
        pass

    def test_single_writer_multiple_reader_different_process_no_swmr(self):
        pass

    def test_single_writer_multiple_reader_different_process_swmr(self):
        pass


class TestProjectStorePerformance(
    TestProjectStoreBase, test_h5store.TestH5StorePerformance
):
    @pytest.fixture
    def setUp(self, setUp_base_h5Store):
        value = TestProjectStorePerformance.get_testdata(self)
        times = numpy.zeros(200)
        for i in range(len(times)):
            start = time()
            with h5py.File(self._fn_store, mode="a") as h5file:
                if i:
                    del h5file["_basegroup"]
                h5file.create_group("_basegroup").create_dataset(
                    "_baseline", data=value, shape=None
                )
            times[i] = time() - start
        self.baseline_time = times


class TestProjectStorePerformanceNestedData(
    TestProjectStorePerformance, test_h5store.TestH5StorePerformance
):
    pass
