# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import io
import itertools
import json
import logging
import os
import pickle
import re
import string
import sys
import uuid
import warnings
from contextlib import contextmanager, redirect_stderr
from tarfile import TarFile
from tempfile import TemporaryDirectory
from time import time
from zipfile import ZipFile

import pytest
import test_h5store
from conftest import deprecated_in_version
from packaging import version
from test_job import TestJobBase

import signac
from signac.common.config import get_config
from signac.contrib.errors import (
    IncompatibleSchemaVersion,
    JobsCorruptedError,
    StatepointParsingError,
    WorkspaceError,
)
from signac.contrib.linked_view import _find_all_links
from signac.contrib.project import JobsCursor, Project  # noqa: F401
from signac.contrib.schema import ProjectSchema
from signac.errors import DestinationExistsError

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

# Skip linked view tests on Windows
WINDOWS = sys.platform == "win32"


# Make sure the jobs created for this test are unique.
test_token = {"test_token": str(uuid.uuid4())}


S_FORMAT1 = """{
 'a': 'int([0, 1, 2, ..., 8, 9], 10)',
 'b.b2': 'int([0, 1, 2, ..., 8, 9], 10)',
 'c.c2.c3.c4.c5': 'tuple([((0, 0, 0),), ((1, 0, 0),), ((2, 0, 0),), ..., ((8, 0, 0),), ((9, 0, 0),)], 10)',
 'const': 'int([0], 1)',
}"""  # noqa: E501


S_FORMAT2 = """{'a': 'int([0, 1, 2, ..., 8, 9], 10)',
 'b': {'b2': 'int([0, 1, 2, ..., 8, 9], 10)'},
 'c': {'c2': {...}},
 'const': 'int([0], 1)'}"""


class TestProjectBase(TestJobBase):
    pass


class TestProject(TestProjectBase):
    def test_get(self):
        pass

    def test_get_id(self):
        with pytest.deprecated_call():
            assert self.project.get_id() == "testing_test_project"
            assert str(self.project) == self.project.get_id()

    def test_property_id(self):
        assert self.project.id == "testing_test_project"
        assert str(self.project) == self.project.id

    def test_repr(self):
        repr(self.project)
        p = eval(repr(self.project))
        assert repr(p) == repr(self.project)
        assert p == self.project

    def test_str(self):
        str(self.project) == self.project.id

    def test_root_directory(self):
        assert self._tmp_pr == self.project.root_directory()

    def test_workspace_directory(self):
        assert self._tmp_wd == self.project.workspace()

    def test_config_modification(self):
        # In-memory modification of the project configuration is
        # deprecated as of 1.3, and will be removed in version 2.0.
        # This unit test should reflect that change beginning 2.0,
        # and check that the project configuration is immutable.
        self.project.config["foo"] = "bar"

    def test_workspace_directory_with_env_variable(self):
        os.environ["SIGNAC_ENV_DIR_TEST"] = self._tmp_wd
        self.project.config["workspace_dir"] = "${SIGNAC_ENV_DIR_TEST}"
        assert self._tmp_wd == self.project.workspace()

    def test_workspace_directory_exists(self):
        assert os.path.exists(self.project.workspace())

    def test_fn(self):
        assert self.project.fn("test/abc") == os.path.join(
            self.project.root_directory(), "test/abc"
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
        prj2 = type(self.project).get_project(root=self.project.root_directory())
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
        prj2 = type(self.project).get_project(root=self.project.root_directory())
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
        prj2 = type(self.project).get_project(root=self.project.root_directory())
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

    def test_write_read_statepoint(self):
        statepoints = [{"a": i} for i in range(5)]
        self.project.dump_statepoints(statepoints)
        self.project.write_statepoints(statepoints)
        read = list(self.project.read_statepoints().values())
        assert len(read) == len(statepoints)
        more_statepoints = [{"b": i} for i in range(5, 10)]
        self.project.write_statepoints(more_statepoints)
        read2 = list(self.project.read_statepoints())
        assert len(read2) == len(statepoints) + len(more_statepoints)
        for id_ in self.project.read_statepoints().keys():
            with pytest.deprecated_call():
                self.project.get_statepoint(id_)

    def test_workspace_path_normalization(self):
        def norm_path(p):
            return os.path.abspath(os.path.expandvars(p))

        def root_path():
            # Returns 'C:\\' on Windows, '/' on other platforms
            return os.path.abspath(os.sep)

        assert self.project.workspace() == norm_path(self._tmp_wd)

        abs_path = os.path.join(root_path(), "path", "to", "workspace")
        self.project.config["workspace_dir"] = abs_path
        assert self.project.workspace() == norm_path(abs_path)

        rel_path = norm_path(os.path.join("path", "to", "workspace"))
        self.project.config["workspace_dir"] = rel_path
        assert self.project.workspace() == norm_path(
            os.path.join(self.project.root_directory(), self.project.workspace())
        )

    def test_no_workspace_warn_on_find(self, caplog):
        if os.path.exists(self.project.workspace()):
            os.rmdir(self.project.workspace())
        with caplog.at_level(logging.INFO):
            list(self.project.find_jobs())
            # Python < 3.8 will return 2 messages.
            # Python >= 3.8 will return 3 messages, because it determines the
            # length of the project one additional time during the list
            # constructor: https://bugs.python.org/issue33234
            assert len(caplog.records) in (2, 3)

    @pytest.mark.skipif(WINDOWS, reason="Symbolic links are unsupported on Windows.")
    def test_workspace_broken_link_error_on_find(self):
        wd = self.project.workspace()
        os.symlink(wd + "~", self.project.fn("workspace-link"))
        self.project.config["workspace_dir"] = "workspace-link"
        with pytest.raises(WorkspaceError):
            list(self.project.find_jobs())

    def test_workspace_read_only_path(self):
        # Create file where workspace would be, thus preventing the creation
        # of the workspace directory.
        if os.path.exists(self.project.workspace()):
            os.rmdir(self.project.workspace())
        with open(os.path.join(self.project.workspace()), "w"):
            pass

        with pytest.raises(OSError):  # Ensure that the file is in place.
            os.mkdir(self.project.workspace())

        assert issubclass(WorkspaceError, OSError)

        try:
            logging.disable(logging.ERROR)
            with pytest.raises(WorkspaceError):
                list(self.project.find_jobs())
        finally:
            logging.disable(logging.NOTSET)

        assert not os.path.isdir(self._tmp_wd)
        assert not os.path.isdir(self.project.workspace())

    def test_find_job_ids(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document["b"] = sp["a"]
        with pytest.deprecated_call():
            assert len(statepoints) == len(list(self.project.find_job_ids()))
            assert 1 == len(list(self.project.find_job_ids({"a": 0})))
            assert 0 == len(list(self.project.find_job_ids({"a": 5})))
            assert 1 == len(list(self.project.find_job_ids(doc_filter={"b": 0})))
            assert 0 == len(list(self.project.find_job_ids(doc_filter={"b": 5})))
            for job_id in self.project.find_job_ids():
                assert self.project.open_job(id=job_id).id == job_id
            index = list(self.project.index())
            for job_id in self.project.find_job_ids(index=index):
                assert self.project.open_job(id=job_id).id == job_id

    def test_find_jobs(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document["test"] = True
        assert len(self.project) == len(self.project.find_jobs())
        assert len(self.project) == len(self.project.find_jobs({}))
        assert 1 == len(list(self.project.find_jobs({"a": 0})))
        assert 0 == len(list(self.project.find_jobs({"a": 5})))

    def test_find_jobs_next(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        jobs = self.project.find_jobs()
        for i in range(2):  # run this twice
            jobs_ = set()
            for i in range(len(self.project)):
                job = jobs.next()
                assert job in self.project
                jobs_.add(job)
            with pytest.raises(StopIteration):
                job = jobs.next()
            assert jobs_ == set(self.project)

    def test_find_jobs_arithmetic_operators(self):
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        assert len(self.project) == 10
        assert len(self.project.find_jobs({"a": {"$lt": 5}})) == 5
        assert len(self.project.find_jobs({"a.$lt": 5})) == 5

    def test_find_jobs_logical_operators(self):
        for i in range(10):
            self.project.open_job({"a": i, "b": {"c": i}}).init()
        assert len(self.project) == 10
        with pytest.raises(ValueError):
            list(self.project.find_jobs({"$and": {"foo": "bar"}}))
        assert len(self.project.find_jobs({"$and": [{}, {"a": 0}]})) == 1
        assert len(self.project.find_jobs({"$or": [{}, {"a": 0}]})) == len(self.project)
        q = {"$and": [{"a": 0}, {"a": 1}]}
        assert len(self.project.find_jobs(q)) == 0
        q = {"$or": [{"a": 0}, {"a": 1}]}
        assert len(self.project.find_jobs(q)) == 2
        q = {"$and": [{"$and": [{"a": 0}, {"a": 1}]}]}
        assert len(self.project.find_jobs(q)) == 0
        q = {"$and": [{"$or": [{"a": 0}, {"a": 1}]}]}
        assert len(self.project.find_jobs(q)) == 2
        q = {"$or": [{"$or": [{"a": 0}, {"a": 1}]}]}
        assert len(self.project.find_jobs(q)) == 2
        q = {"$or": [{"$and": [{"a": 0}, {"a": 1}]}]}
        assert len(self.project.find_jobs(q)) == 0
        assert len(self.project.find_jobs({"$and": [{}, {"b": {"c": 0}}]})) == 1
        assert len(self.project.find_jobs({"$or": [{}, {"b": {"c": 0}}]})) == len(
            self.project
        )
        q = {"$and": [{"b": {"c": 0}}, {"b": {"c": 1}}]}
        assert len(self.project.find_jobs(q)) == 0
        q = {"$or": [{"b": {"c": 0}}, {"b": {"c": 1}}]}
        assert len(self.project.find_jobs(q)) == 2
        q = {"$and": [{"$and": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}
        assert len(self.project.find_jobs(q)) == 0
        q = {"$and": [{"$or": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}
        assert len(self.project.find_jobs(q)) == 2
        q = {"$or": [{"$or": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}
        assert len(self.project.find_jobs(q)) == 2
        q = {"$or": [{"$and": [{"b": {"c": 0}}, {"b": {"c": 1}}]}]}
        assert len(self.project.find_jobs(q)) == 0

    def test_num_jobs(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        assert len(statepoints) == self.project.num_jobs()
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

        os.remove(job.fn(job.FN_MANIFEST))

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

        # overwrite state point manifest file
        with open(job.fn(job.FN_MANIFEST), "w"):
            pass

        self.project._sp_cache.clear()
        self.project._remove_persistent_cache_file()
        try:
            logging.disable(logging.CRITICAL)
            with pytest.raises(JobsCorruptedError):
                self.project.open_job(id=job.id)
        finally:
            logging.disable(logging.NOTSET)

    def test_rename_workspace(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        # First, we move the job to the wrong directory.
        wd = job.workspace()
        wd_invalid = os.path.join(self.project.workspace(), "0" * 32)
        os.replace(wd, wd_invalid)  # Move to incorrect id.
        assert not os.path.exists(job.workspace())

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
            #  ... we reinitalize the initial job, ...
            job.init()
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

    def test_repair_corrupted_workspace(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()

        for i, job in enumerate(self.project):
            pass
        assert i == 4

        # no manifest file
        with self.project.open_job(statepoints[0]) as job:
            os.remove(job.FN_MANIFEST)
        # blank manifest file
        with self.project.open_job(statepoints[1]) as job:
            with open(job.FN_MANIFEST, "w"):
                pass

        # Need to clear internal and persistent cache to encounter error.
        self.project._sp_cache.clear()
        self.project._remove_persistent_cache_file()

        # Ensure that state point hash table does not exist.
        assert not os.path.isfile(self.project.fn(self.project.FN_STATEPOINTS))

        # disable logging temporarily
        try:
            logging.disable(logging.CRITICAL)

            # Iterating through the jobs should now result in an error.
            with pytest.raises(JobsCorruptedError):
                for job in self.project:
                    pass

            with pytest.raises(JobsCorruptedError):
                self.project.repair()

            self.project.write_statepoints(statepoints)
            self.project.repair()

            os.remove(self.project.fn(self.project.FN_STATEPOINTS))
            self.project._sp_cache.clear()
            for job in self.project:
                pass
        finally:
            logging.disable(logging.NOTSET)

    def test_index(self):
        docs = list(self.project.index(include_job_document=True))
        assert len(docs) == 0
        docs = list(self.project.index(include_job_document=False))
        assert len(docs) == 0
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document["test"] = True
        job_ids = {job.id for job in self.project.find_jobs()}
        docs = list(self.project.index())
        job_ids_cmp = {doc["_id"] for doc in docs}
        assert job_ids == job_ids_cmp
        assert len(docs) == len(statepoints)
        for sp in statepoints:
            with self.project.open_job(sp):
                with open("test.txt", "w"):
                    pass
        docs = list(
            self.project.index(
                {".*" + re.escape(os.path.sep) + r"test\.txt": "TextFile"}
            )
        )
        assert len(docs) == 2 * len(statepoints)
        assert len({doc["_id"] for doc in docs}) == len(docs)

    def test_signac_project_crawler(self):
        statepoints = [{"a": i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document["test"] = True
        job_ids = {job.id for job in self.project.find_jobs()}
        index = {}
        for doc in self.project.index():
            index[doc["_id"]] = doc
        assert len(index) == len(job_ids)
        assert set(index.keys()) == set(job_ids)
        crawler = signac.contrib.SignacProjectCrawler(self.project.root_directory())
        index2 = {}
        for doc in crawler.crawl():
            index2[doc["_id"]] = doc
        for _id, _id2 in zip(index, index2):
            assert _id == _id2
            assert index[_id] == index2[_id]
        assert index == index2
        for job in self.project.find_jobs():
            with open(job.fn("test.txt"), "w") as file:
                file.write("test\n")
        formats = {r".*" + re.escape(os.path.sep) + r"test\.txt": "TextFile"}
        index = {}
        for doc in self.project.index(formats):
            index[doc["_id"]] = doc
        assert len(index) == 2 * len(job_ids)

        class Crawler(signac.contrib.SignacProjectCrawler):
            called = False

            def process(self_, doc, dirpath, fn):
                Crawler.called = True
                doc = super().process(doc=doc, dirpath=dirpath, fn=fn)
                if "format" in doc and doc["format"] is None:
                    assert doc["_id"] == doc["signac_id"]
                return doc

        for p, fmt in formats.items():
            with pytest.deprecated_call():
                Crawler.define(p, fmt)
        index2 = {}
        for doc in Crawler(root=self.project.root_directory()).crawl():
            index2[doc["_id"]] = doc
        assert index == index2
        assert Crawler.called

    def test_custom_project(self):
        class CustomProject(signac.Project):
            pass

        project = CustomProject.get_project(root=self.project.root_directory())
        assert isinstance(project, signac.Project)
        assert isinstance(project, CustomProject)

    def test_custom_job_class(self):
        class CustomJob(signac.contrib.job.Job):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

        class CustomProject(signac.Project):
            Job = CustomJob

        project = CustomProject.get_project(root=self.project.root_directory())
        assert isinstance(project, signac.Project)
        assert isinstance(project, CustomProject)
        job = project.open_job(dict(a=0))
        assert isinstance(job, CustomJob)
        assert isinstance(job, signac.contrib.job.Job)

    def test_project_contains(self):
        job = self.open_job(dict(a=0))
        assert job not in self.project
        job.init()
        assert job in self.project

    def test_job_move(self):
        root = self._tmp_dir.name
        project_a = signac.init_project("ProjectA", os.path.join(root, "a"))
        project_b = signac.init_project("ProjectB", os.path.join(root, "b"))
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
        with job:
            job.document["a"] = 0
            with open("hello.txt", "w") as file:
                file.write("world!")
        job_ = project_b.open_job(job.statepoint())
        assert job == job_
        assert job_ == job_b
        assert job_.isfile("hello.txt")
        assert job_.document["a"] == 0

    def test_job_clone(self):
        root = self._tmp_dir.name
        project_a = signac.init_project("ProjectA", os.path.join(root, "a"))
        project_b = signac.init_project("ProjectB", os.path.join(root, "b"))
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
                    "const": 0,
                    "const2": {"const3": 0},
                    "a": i,
                    "b": {"b2": i},
                    "c": [i if i % 2 else None, 0, 0],
                    "d": [[i, 0, 0]],
                    "e": {"e2": [i, 0, 0]} if i % 2 else 0,  # heterogeneous!
                    "f": {"f2": [[i, 0, 0]]},
                }
            ).init()

        s = self.project.detect_schema()
        assert len(s) == 9
        for k in "const", "const2.const3", "a", "b.b2", "c", "d", "e.e2", "f.f2":
            assert k in s
            assert k.split(".") in s
            # The following calls should not error out.
            s[k]
            s[k.split(".")]
        repr(s)
        assert s.format() == str(s)
        s = self.project.detect_schema(exclude_const=True)
        assert len(s) == 7
        assert "const" not in s
        assert ("const2", "const3") not in s
        assert "const2.const3" not in s
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

    def test_schema_eval(self):
        for i in range(10):
            for j in range(10):
                self.project.open_job(dict(a=i, b=j)).init()
        s = self.project.detect_schema()
        assert s == s(self.project)
        assert s == s([job.sp for job in self.project])
        # Check that it works with iterables that can only be consumed once
        assert s == s(job.sp for job in self.project)

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

        assert S_FORMAT1 == s_format1
        assert S_FORMAT2 == s_format2

    def test_jobs_groupby(self):
        def get_sp(i):
            return {"a": i, "b": i % 2, "c": i % 3}

        for i in range(12):
            self.project.open_job(get_sp(i)).init()

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

    def test_jobs_groupbydoc(self):
        def get_doc(i):
            return {"a": i, "b": i % 2, "c": i % 3}

        for i in range(12):
            job = self.project.open_job({"i": i})
            job.init()
            job.document = get_doc(i)

        for k, g in self.project.groupbydoc("a"):
            assert len(list(g)) == 1
            for job in list(g):
                assert job.document["a"] == k
        for k, g in self.project.groupbydoc("b"):
            assert len(list(g)) == 6
            for job in list(g):
                assert job.document["b"] == k
        with pytest.raises(KeyError):
            for k, g in self.project.groupbydoc("d"):
                pass
        for k, g in self.project.groupbydoc("d", default=-1):
            assert k == -1
            assert len(list(g)) == len(self.project)
        for k, g in self.project.groupbydoc(("b", "c")):
            assert len(list(g)) == 2
            for job in list(g):
                assert job.document["b"] == k[0]
                assert job.document["c"] == k[1]
        for k, g in self.project.groupbydoc(lambda doc: doc["a"] % 4):
            assert len(list(g)) == 3
            for job in list(g):
                assert job.document["a"] % 4 == k
        for k, g in self.project.groupbydoc(lambda doc: str(doc)):
            assert len(list(g)) == 1
            for job in list(g):
                assert str(job.document) == k
        group_count = 0
        for k, g in self.project.groupbydoc():
            assert len(list(g)) == 1
            group_count = group_count + 1
            for job in list(g):
                assert str(job) == k
        assert group_count == len(list(self.project.find_jobs()))

    def test_temp_project(self):
        with self.project.temporary_project() as tmp_project:
            assert len(tmp_project) == 0
            tmp_root_dir = tmp_project.root_directory()
            assert os.path.isdir(tmp_root_dir)
            for i in range(10):  # init some jobs
                tmp_project.open_job(dict(a=i)).init()
            assert len(tmp_project) == 10
        assert not os.path.isdir(tmp_root_dir)

    def test_access_module(self):
        with deprecated_in_version("1.5"):
            self.project.create_access_module()


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
        os.replace(self.project.workspace(), self.project.workspace() + "~")
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
        os.replace(self.project.workspace(), self.project.workspace() + "~")
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
        os.replace(self.project.workspace(), self.project.workspace() + "~")
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
        os.replace(self.project.workspace(), self.project.workspace() + "~")
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
        self.project.import_from(origin=self.project.workspace())
        assert ids_before_export == {job.id for job in self.project.find_jobs()}
        with self.project.temporary_project() as tmp_project:
            tmp_project.import_from(origin=self.project.workspace())
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
        assert "project" in str(self.project)
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
    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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
                lambda l: os.path.realpath(os.path.join(view_prefix, l, "job")),
                all_links,
            )
        )
        src = set(
            map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs())
        )
        assert len(all_links) == self.project.num_jobs()
        self.project.create_linked_view(prefix=view_prefix)
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == self.project.num_jobs()
        dst = set(
            map(
                lambda l: os.path.realpath(os.path.join(view_prefix, l, "job")),
                all_links,
            )
        )
        src = set(
            map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs())
        )
        assert src == dst
        # update with subset
        job_subset = self.project.find_jobs({"b": 0})
        id_subset = [job.id for job in job_subset]

        bad_index = [dict(_id=i) for i in range(3)]
        with pytest.raises(ValueError):
            self.project.create_linked_view(
                prefix=view_prefix, job_ids=id_subset, index=bad_index
            )

        self.project.create_linked_view(prefix=view_prefix, job_ids=id_subset)
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == len(id_subset)
        dst = set(
            map(
                lambda l: os.path.realpath(os.path.join(view_prefix, l, "job")),
                all_links,
            )
        )
        src = set(map(lambda j: os.path.realpath(j.workspace()), job_subset))
        assert src == dst
        # some jobs removed
        clean({"b": 0})
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == self.project.num_jobs()
        dst = set(
            map(
                lambda l: os.path.realpath(os.path.join(view_prefix, l, "job")),
                all_links,
            )
        )
        src = set(
            map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs())
        )
        assert src == dst
        # all jobs removed
        clean()
        all_links = list(_find_all_links(view_prefix))
        assert len(all_links) == self.project.num_jobs()
        dst = set(
            map(
                lambda l: os.path.realpath(os.path.join(view_prefix, l, "job")),
                all_links,
            )
        )
        src = set(
            map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs())
        )
        assert src == dst

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
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

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
    def test_create_linked_view_heterogeneous_schema_problematic(self):
        self.project.open_job(dict(a=1)).init()
        self.project.open_job(dict(a=1, b=1)).init()
        view_prefix = os.path.join(self._tmp_pr, "view")
        with pytest.raises(RuntimeError):
            self.project.create_linked_view(view_prefix)

    @pytest.mark.skipif(WINDOWS, reason="Linked views unsupported on Windows.")
    def test_create_linked_view_with_slash_raises_error(self):
        bad_chars = [os.sep, " ", "*"]
        statepoints = [{f"a{i}b": 0, "b": f"bad{i}val"} for i in bad_chars]
        view_prefix = os.path.join(self._tmp_pr, "view")
        for sp in statepoints:
            self.project.open_job(sp).init()
            with pytest.raises(RuntimeError):
                self.project.create_linked_view(prefix=view_prefix)


class UpdateCacheAfterInitJob(signac.contrib.job.Job):
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self._project.update_cache()


class UpdateCacheAfterInitJobProject(signac.Project):
    "This is a test class that regularly calls the update_cache() method."
    Job = UpdateCacheAfterInitJob


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
        root = self._tmp_dir.name
        with pytest.raises(LookupError):
            signac.get_project(root=root)
        project = signac.init_project(name="testproject", root=root)
        assert project.id == "testproject"
        assert project.workspace() == os.path.join(root, "workspace")
        assert project.root_directory() == root
        project = signac.Project.init_project(name="testproject", root=root)
        assert project.id == "testproject"
        assert project.workspace() == os.path.join(root, "workspace")
        assert project.root_directory() == root
        project = signac.get_project(root=root)
        assert project.id == "testproject"
        assert project.workspace() == os.path.join(root, "workspace")
        assert project.root_directory() == root
        project = signac.Project.get_project(root=root)
        assert project.id == "testproject"
        assert project.workspace() == os.path.join(root, "workspace")
        assert project.root_directory() == root

    def test_get_project_all_printable_characters(self):
        root = self._tmp_dir.name
        with pytest.raises(LookupError):
            signac.get_project(root=root)
        project_name = "testproject" + string.printable
        project = signac.init_project(name=project_name, root=root)
        with pytest.deprecated_call():
            assert project.get_id() == project_name

    def test_get_project_non_local(self):
        root = self._tmp_dir.name
        subdir = os.path.join(root, "subdir")
        os.mkdir(subdir)
        project = signac.init_project(root=root, name="testproject")
        assert project == project.get_project(root=root)
        assert project == signac.get_project(root=root)
        assert project == project.get_project(root=root, search=False)
        assert project == signac.get_project(root=root, search=False)
        assert project == project.get_project(root=os.path.relpath(root), search=False)
        assert project == signac.get_project(root=os.path.relpath(root), search=False)
        with pytest.raises(LookupError):
            assert project == project.get_project(root=subdir, search=False)
        with pytest.raises(LookupError):
            assert project == signac.get_project(root=subdir, search=False)
        assert project == project.get_project(root=subdir, search=True)
        assert project == signac.get_project(root=subdir, search=True)

    def test_init(self):
        root = self._tmp_dir.name
        with pytest.raises(LookupError):
            signac.get_project(root=root)
        project = signac.init_project(name="testproject", root=root)
        assert project.id == "testproject"
        assert project.workspace() == os.path.join(root, "workspace")
        assert project.root_directory() == root
        # Second initialization should not make any difference.
        project = signac.init_project(name="testproject", root=root)
        project = signac.get_project(root=root)
        assert project.id == "testproject"
        assert project.workspace() == os.path.join(root, "workspace")
        assert project.root_directory() == root
        project = signac.Project.get_project(root=root)
        assert project.id == "testproject"
        assert project.workspace() == os.path.join(root, "workspace")
        assert project.root_directory() == root
        # Deviating initialization parameters should result in errors.
        with pytest.raises(RuntimeError):
            signac.init_project(name="testproject2", root=root)
        with pytest.raises(RuntimeError):
            signac.init_project(name="testproject", root=root, workspace="workspace2")
        with pytest.raises(RuntimeError):
            signac.init_project(name="testproject2", root=root, workspace="workspace2")

    def test_nested_project(self):
        def check_root(root=None):
            if root is None:
                root = os.getcwd()
            assert os.path.realpath(
                signac.get_project(root=root).root_directory()
            ) == os.path.realpath(root)

        root = self._tmp_dir.name
        root_a = os.path.join(root, "project_a")
        root_b = os.path.join(root_a, "project_b")
        signac.init_project("testprojectA", root_a)
        assert signac.get_project(root=root_a).id == "testprojectA"
        check_root(root_a)
        signac.init_project("testprojectB", root_b)
        assert signac.get_project(root=root_b).id == "testprojectB"
        check_root(root_b)
        cwd = os.getcwd()
        try:
            os.chdir(root_a)
            check_root()
            assert signac.get_project().id == "testprojectA"
        finally:
            os.chdir(cwd)
        try:
            os.chdir(root_b)
            assert signac.get_project().id == "testprojectB"
            check_root()
        finally:
            os.chdir(cwd)

    def test_get_job_valid_workspace(self):
        # Test case: The root-path is the job workspace path.
        root = self._tmp_dir.name
        project = signac.init_project(name="testproject", root=root)
        job = project.open_job({"a": 1})
        job.init()
        with job:
            # The context manager enters the working directory of the job
            assert project.get_job() == job
            assert signac.get_job() == job

    def test_get_job_invalid_workspace(self):
        # Test case: The root-path is not the job workspace path.
        root = self._tmp_dir.name
        project = signac.init_project(name="testproject", root=root)
        job = project.open_job({"a": 1})
        job.init()
        # We shouldn't be able to find a job while in the workspace directory,
        # since no signac_statepoint.json exists.
        cwd = os.getcwd()
        try:
            os.chdir(project.workspace())
            with pytest.raises(LookupError):
                project.get_job()
            with pytest.raises(LookupError):
                signac.get_job()
        finally:
            os.chdir(cwd)

    def test_get_job_nested_project(self):
        # Test case: The job workspace dir is also a project root dir.
        root = self._tmp_dir.name
        project = signac.init_project(name="testproject", root=root)
        job = project.open_job({"a": 1})
        job.init()
        with job:
            nestedproject = signac.init_project("nestedproject")
            nestedproject.open_job({"b": 2}).init()
            assert project.get_job() == job
            assert signac.get_job() == job

    def test_get_job_subdir(self):
        # Test case: Get a job from a sub-directory of the job workspace dir.
        root = self._tmp_dir.name
        project = signac.init_project(name="testproject", root=root)
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
        # when the job workspace is also a project root dir
        root = self._tmp_dir.name
        project = signac.init_project(name="testproject", root=root)
        job = project.open_job({"a": 1})
        job.init()
        with job:
            nestedproject = signac.init_project("nestedproject")
            nestedproject.open_job({"b": 2}).init()
            os.mkdir("test_subdir")
            assert project.get_job("test_subdir") == job
            assert signac.get_job("test_subdir") == job
        assert project.get_job(job.fn("test_subdir")) == job
        assert signac.get_job(job.fn("test_subdir")) == job

    @pytest.mark.skipif(WINDOWS, reason="Symbolic links are unsupported on Windows.")
    def test_get_job_symlink_other_project(self):
        # Test case: Get a job from a symlink in another project workspace
        root = self._tmp_dir.name
        project_a_dir = os.path.join(root, "project_a")
        project_b_dir = os.path.join(root, "project_b")
        os.mkdir(project_a_dir)
        os.mkdir(project_b_dir)
        project_a = signac.init_project(name="project_a", root=project_a_dir)
        project_b = signac.init_project(name="project_b", root=project_b_dir)
        job_a = project_a.open_job({"a": 1})
        job_a.init()
        job_b = project_b.open_job({"b": 1})
        job_b.init()
        symlink_path = os.path.join(project_b.workspace(), job_a._id)
        os.symlink(job_a.ws, symlink_path)
        assert project_a.get_job(symlink_path) == job_a
        assert project_b.get_job(symlink_path) == job_a
        assert signac.get_job(symlink_path) == job_a


class TestProjectSchema(TestProjectBase):
    def test_project_schema_versions(self):
        impossibly_high_schema_version = "9999"
        assert version.parse(self.project.config["schema_version"]) < version.parse(
            impossibly_high_schema_version
        )
        config = get_config(self.project.fn("signac.rc"))
        config["schema_version"] = impossibly_high_schema_version
        config.write()
        with pytest.raises(IncompatibleSchemaVersion):
            signac.init_project(
                name=str(self.project), root=self.project.root_directory()
            )

    def test_project_schema_version_migration(self):
        from signac.contrib.migration import apply_migrations

        apply_migrations(self.project)
        self.project._config["schema_version"] = "0"
        assert self.project._config["schema_version"] == "0"
        err = io.StringIO()
        with redirect_stderr(err):
            for origin, destination in apply_migrations(self.project):
                assert self.project._config["schema_version"] == destination
                project = signac.get_project(root=self.project.root_directory())
                assert project._config["schema_version"] == destination
        assert self.project._config["schema_version"] == "1"
        assert "OK" in err.getvalue()
        assert "0 to 1" in err.getvalue()

    def test_no_migration(self):
        # This unit test should fail as long as there are no schema migrations
        # implemented within the signac.contrib.migration package.
        #
        # Once migrations are implemented:
        #
        # 1. Ensure to enable the 'migrate' sub-command within the __main__ module.
        # 2. Either update or remove this unit test.
        from signac.contrib.migration import _collect_migrations

        migrations = list(_collect_migrations(self.project))
        assert len(migrations) == 0


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


class TestTestingProjectInitialization(TestProjectBase):

    # Sanity check on all different combinations of inputs
    def test_input_args(self):
        for nested, listed, het in itertools.product([True, False], repeat=3):
            with self.project.temporary_project() as tmp_project:
                jobs = signac.testing.init_jobs(
                    tmp_project, nested=nested, listed=listed, heterogeneous=het
                )
                assert len(tmp_project) > 0
                assert len(tmp_project) == len(jobs)
                # check that call does not fail:
                tmp_project.detect_schema()


class TestProjectStoreBase(test_h5store.TestH5StoreBase):

    project_class = signac.Project

    @pytest.fixture(autouse=True)
    def setUp_base_h5Store(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, "pr")
        self._tmp_wd = os.path.join(self._tmp_dir.name, "wd")
        os.mkdir(self._tmp_pr)
        self.config = signac.common.config.load_config()
        self.project = self.project_class.init_project(
            name="testing_test_project", root=self._tmp_pr, workspace=self._tmp_wd
        )

        warnings.filterwarnings("ignore", category=DeprecationWarning, module="signac")
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
