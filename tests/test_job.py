# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import copy
import json
import logging
import os
import random
import uuid
from contextlib import contextmanager
from tempfile import TemporaryDirectory

import pytest

import signac
from signac._config import _load_config
from signac.errors import (
    DestinationExistsError,
    InvalidKeyError,
    JobsCorruptedError,
    KeyTypeError,
)
from signac.job import Job

try:
    import h5py  # noqa: F401

    H5PY = True
except ImportError:
    H5PY = False

# Make sure the jobs created for this test are unique.
test_token = {"test_token": str(uuid.uuid4())}

BUILTINS = [
    ({"e": [1.0, "1.0", 1, True]}, "4d8058a305b940005be419b30e99bb53"),
    ({"d": True}, "33cf9999de25a715a56339c6c1b28b41"),
    ({"f": (1.0, "1.0", 1, True)}, "e998db9b595e170bdff936f88ccdbf75"),
    ({"a": 1}, "42b7b4f2921788ea14dac5566e6f06d0"),
    ({"c": "1.0"}, "80fa45716dd3b83fa970877489beb42e"),
    ({"b": 1.0}, "0ba6c5a46111313f11c41a6642520451"),
]


def builtins_dict():
    random.shuffle(BUILTINS)
    d = {}
    for b in BUILTINS:
        d.update(b[0])
    return d


BUILTINS_HASH = "7a80b58db53bbc544fc27fcaaba2ce44"

NESTED_HASH = "bd6f5828f4410b665bffcec46abeb8f3"


class TestJobBase:
    project_class = signac.Project

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, "pr")
        os.mkdir(self._tmp_pr)
        self.config = _load_config()
        self.project = self.project_class.init_project(path=self._tmp_pr)

    def tearDown(self):
        pass

    def open_job(self, *args, **kwargs):
        project = self.project
        return project.open_job(*args, **kwargs)

    @classmethod
    def nested_dict(self):
        d = dict(builtins_dict())
        d["g"] = builtins_dict()
        return d


class TestJobID(TestJobBase):
    def test_builtins(self):
        for p, h in BUILTINS:
            assert str(self.project.open_job(p)) == h
        assert str(self.project.open_job(builtins_dict())) == BUILTINS_HASH

    def test_shuffle(self):
        for i in range(10):
            assert str(self.project.open_job(builtins_dict())) == BUILTINS_HASH

    def test_nested(self):
        for i in range(10):
            assert str(self.project.open_job(self.nested_dict())) == NESTED_HASH

    def test_sequences_identity(self):
        job1 = self.project.open_job({"a": [1.0, "1.0", 1, True]})
        job2 = self.project.open_job({"a": (1.0, "1.0", 1, True)})
        assert str(job1) == str(job2)
        assert job1.statepoint() == job2.statepoint()


class TestJob(TestJobBase):
    def test_repr(self):
        job = self.project.open_job({"a": 0})
        job2 = self.project.open_job({"a": 0})
        assert repr(job) == repr(job2)
        assert job == job2

    def test_str(self):
        job = self.project.open_job({"a": 0})
        assert str(job) == job.id

    def test_eq(self):
        job = self.project.open_job({"a": 0})

        # Make sure that Jobs can only be equal to other Job instances.
        class NonJob:
            """Minimal class that cannot be compared with Job objects."""

            def __init__(self, job):
                self.id = job.id
                self._workspace = job.path

        class JobSubclass(Job):
            """Minimal subclass that can be compared with Job objects."""

            def __init__(self, job):
                self._id = job.id
                self._path = job.path

            def workspace(self):
                return self._workspace

        non_job = NonJob(job)
        assert job != non_job
        assert non_job != job

        sub_job = JobSubclass(job)
        assert job == sub_job
        assert sub_job == job

        job2 = self.project.open_job({"a": 0})
        assert job == job2
        assert job2 == job

    def test_isfile(self):
        job = self.project.open_job({"a": 0})
        fn = "test.txt"
        fn_ = os.path.join(job.path, fn)
        assert not job.isfile(fn)
        job.init()
        assert not job.isfile(fn)
        with open(fn_, "w") as file:
            file.write("hello")
        assert job.isfile(fn)

    def test_copy(self):
        job = self.project.open_job({"a": 0}).init()
        assert job in self.project

        # Modify copy
        copied_job = copy.copy(job)
        assert job is not copied_job
        assert job == copied_job
        assert job.sp == copied_job.sp
        assert job in self.project
        assert copied_job in self.project
        copied_job.sp.a = 1
        assert job in self.project
        assert copied_job in self.project
        assert job == copied_job
        assert job.sp == copied_job.sp

        # Modify original
        copied_job = copy.copy(job)
        assert job is not copied_job
        assert job == copied_job
        assert job.sp == copied_job.sp
        assert job in self.project
        assert copied_job in self.project
        job.sp.a = 2
        assert job in self.project
        assert copied_job in self.project
        assert job == copied_job
        assert job.sp == copied_job.sp

        # Delete original
        del job
        assert copied_job in self.project
        copied_job.sp.a = 3
        assert copied_job in self.project

    def test_deepcopy(self):
        job = self.project.open_job({"a": 0}).init()
        assert job in self.project

        # Modify copy
        copied_job = copy.deepcopy(job)
        assert job is not copied_job
        assert job == copied_job
        assert job.sp == copied_job.sp
        assert job in self.project
        assert copied_job in self.project
        copied_job.sp.a = 1
        assert job not in self.project
        assert copied_job in self.project
        assert job != copied_job
        assert job.sp != copied_job.sp

        # Modify original
        job = self.project.open_job({"a": 0}).init()
        copied_job = copy.deepcopy(job)
        assert job is not copied_job
        assert job == copied_job
        assert job.sp == copied_job.sp
        assert job in self.project
        assert copied_job in self.project
        job.sp.a = 2
        assert job in self.project
        assert copied_job not in self.project
        assert job != copied_job
        assert job.sp != copied_job.sp

        # Delete original
        copied_job = copy.deepcopy(job)
        del job
        assert copied_job in self.project
        copied_job.sp.a = 3
        assert copied_job in self.project

    def test_project_access_from_job(self):
        job = self.project.open_job({"a": 0}).init()
        assert isinstance(job.project, signac.Project)
        assert job in job.project
        assert job.project.path == self._tmp_pr

    def test_custom_project_access_from_job(self):
        # Test a custom project subclass to ensure compatibility with signac-flow's FlowProject
        class CustomProject(signac.Project):
            pass

        custom_project = CustomProject.get_project(self._tmp_pr)
        job = custom_project.open_job({"a": 0}).init()
        assert isinstance(job.project, CustomProject)
        assert job in job.project
        assert job.project.path == self._tmp_pr


class TestJobSpInterface(TestJobBase):
    def test_interface_read_only(self):
        sp = self.nested_dict()
        job = self.open_job(sp)
        assert job.statepoint() == json.loads(json.dumps(sp))
        for x in ("a", "b", "c", "d", "e"):
            assert getattr(job.sp, x) == sp[x]
            assert job.sp[x] == sp[x]
        for x in ("a", "b", "c", "d", "e"):
            assert getattr(job.sp.g, x) == sp["g"][x]
            assert job.sp[x] == sp[x]
        for x in ("a", "b", "c", "d", "e"):
            assert job.sp.get(x) == sp[x]
            assert job.sp.get(x) == sp[x]
            assert job.sp.g.get(x) == sp["g"][x]
        assert job.sp.get("not_in_sp") is None
        assert job.sp.g.get("not_in_sp") is None
        assert job.sp.get("not_in_sp", None) is None
        assert job.sp.g.get("not_in_sp", None) is None
        assert job.sp.get("not_in_sp", 23) == 23
        assert job.sp.g.get("not_in_sp", 23) == 23

    def test_interface_contains(self):
        sp = self.nested_dict()
        job = self.open_job(sp)
        for x in ("a", "b", "c", "d", "e"):
            assert x in job.sp
            assert x in job.sp.g

    def test_interface_read_write(self):
        sp = self.nested_dict()
        job = self.open_job(sp)
        job.init()
        for x in ("a", "b", "c", "d", "e"):
            assert getattr(job.sp, x) == sp[x]
            assert job.sp[x] == sp[x]
        for x in ("a", "b", "c", "d", "e"):
            assert getattr(job.sp.g, x) == sp["g"][x]
            assert job.sp[x] == sp[x]
        a = [1, 1.0, "1.0", True, None]
        for x in ("a", "b", "c", "d", "e"):
            setattr(job.sp, x, a)
            assert getattr(job.sp, x) == a
            setattr(job.sp.g, x, a)
            assert getattr(job.sp.g, x) == a
        t = (1, 2, 3)  # tuple
        job.sp.t = t
        assert job.sp.t == list(t)  # implicit conversion
        job.sp.g.t = t
        assert job.sp.g.t == list(t)

    def test_interface_job_identity_change(self):
        job = self.open_job({"a": 0})
        old_id = job.id
        job.sp.a = 1
        assert old_id != job.id

    def test_interface_nested_kws(self):
        with pytest.raises(InvalidKeyError):
            job = self.open_job({"a.b.c": 0}).statepoint

        job = self.open_job(dict(a=dict(b=dict(c=2))))
        assert job.sp.a.b.c == 2
        assert job.sp["a"]["b"]["c"] == 2

    def test_interface_lists(self):
        job = self.open_job({"a": [1, 2, 3]})
        assert job.sp.a == [1, 2, 3]
        old_id = job.id
        job.sp.a.append(4)
        assert job.sp.a == [1, 2, 3, 4]
        assert old_id != job.id

    def test_interface_reserved_keywords(self):
        job = self.open_job({"with": 0, "pop": 1})
        assert job.sp["with"] == 0
        assert job.sp["pop"] == 1
        assert job.sp.pop("with") == 0
        assert "with" not in job.sp

    def test_interface_illegal_type(self):
        job = self.open_job(dict(a=0))
        assert job.sp.a == 0

        class Foo:
            pass

        with pytest.raises(TypeError):
            job.sp.a = Foo()

    def test_interface_rename(self):
        job = self.open_job(dict(a=0))
        job.init()
        assert job.sp.a == 0
        job.sp.b = job.sp.pop("a")
        assert "a" not in job.sp
        assert job.sp.b == 0

    def test_interface_copy(self):
        job = self.open_job(dict(a=0)).init()
        copy.copy(job.sp).a = 1
        assert job in self.project

    def test_interface_deepcopy(self):
        job = self.open_job(dict(a=0)).init()
        copy.deepcopy(job.sp).a = 1
        assert job not in self.project

    def test_interface_add(self):
        job = self.open_job(dict(a=0))
        job.init()
        with pytest.raises(AttributeError):
            job.sp.b
        job.sp.b = 1
        assert "b" in job.sp
        assert job.sp.b == 1

    def test_interface_delete(self):
        job = self.open_job(dict(a=0, b=0))
        job.init()
        assert "b" in job.sp
        assert job.sp.b == 0
        del job.sp["b"]
        assert "b" not in job.sp
        with pytest.raises(AttributeError):
            job.sp.b
        job.sp.b = 0
        assert "b" in job.sp
        assert job.sp.b == 0
        del job.sp.b
        assert "b" not in job.sp
        with pytest.raises(AttributeError):
            job.sp.b

    def test_interface_destination_conflict(self):
        job_a = self.open_job(dict(a=0))
        job_b = self.open_job(dict(b=0))
        job_a.init()
        id_a = job_a.id
        job_a.sp = dict(b=0)
        assert job_a.statepoint() == dict(b=0)
        assert job_a == job_b
        assert job_a.id != id_a
        job_a = self.open_job(dict(a=0))
        # Moving to existing job, no problem while empty:
        assert job_a != job_b
        job_a.sp = dict(b=0)
        job_a = self.open_job(dict(a=0))
        job_b.init()
        # Moving to an existing job with data leads
        # to an error:
        job_a.document["a"] = 0
        job_b.document["a"] = 0
        assert job_a != job_b
        with pytest.raises(RuntimeError):
            job_a.sp = dict(b=0)
        with pytest.raises(DestinationExistsError):
            job_a.sp = dict(b=0)

    def test_interface_multiple_changes(self):
        for i in range(1, 4):
            job = self.project.open_job(dict(a=i))
            job.init()
        for job in self.project:
            assert job.sp.a > 0

        for job in self.project:
            obj_id = id(job)
            id0 = job.id
            sp0 = job.statepoint()
            assert id(job) == obj_id
            assert job.sp.a > 0
            assert job.id == id0
            assert job.sp == sp0

            job.sp.a = -job.sp.a
            assert id(job) == obj_id
            assert job.sp.a < 0
            assert job.id != id0
            assert job.sp != sp0

            job.sp.a = -job.sp.a
            assert id(job) == obj_id
            assert job.sp.a > 0
            assert job.id == id0
            assert job.sp == sp0
            job2 = self.project.open_job(id=id0)
            assert job.sp == job2.sp
            assert job.id == job2.id

    def test_valid_sp_key_types(self):
        job = self.open_job(dict(valid_key=True)).init()

        # Only strings are permitted as keys
        for key in ("0", "1"):
            job.sp[key] = "test"
            assert str(key) in job.sp

    def test_invalid_sp_key_types(self):
        class A:
            pass

        with pytest.raises(KeyTypeError):
            self.open_job({A(): True}).init()

        job = self.open_job(dict(invalid_key=True)).init()

        for key in (1, True, False, None, 0.0, A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                job.sp[key] = "test"
            with pytest.raises(KeyTypeError):
                job.sp = {key: "test"}
        for key in ([], {}):
            with pytest.raises(TypeError):
                job.sp[key] = "test"
            with pytest.raises(TypeError):
                job.sp = {key: "test"}

    def test_valid_doc_key_types(self):
        job = self.open_job(dict(valid_key=True)).init()

        # Only strings are permitted as keys
        for key in ("0", "1"):
            job.doc[key] = "test"
            assert str(key) in job.doc

    def test_invalid_doc_key_types(self):
        job = self.open_job(dict(invalid_key=True)).init()

        class A:
            pass

        for key in (1, True, False, None, 0.0, A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                job.doc[key] = "test"
            with pytest.raises(KeyTypeError):
                job.doc = {key: "test"}
        for key in ([], {}):
            with pytest.raises(TypeError):
                job.doc[key] = "test"
            with pytest.raises(TypeError):
                job.doc = {key: "test"}

    def test_cached_statepoint_read_only(self):
        statepoint = {"a": 0, "b": 1, "dict": {"value": "string"}}
        job = self.open_job(statepoint=statepoint)
        job.init()

        assert "a" in job.cached_statepoint
        assert "b" in job.cached_statepoint
        assert "c" not in job.cached_statepoint
        assert "dict" in job.cached_statepoint
        assert job.cached_statepoint["a"] == 0
        assert job.cached_statepoint["b"] == 1
        assert job.cached_statepoint["dict"] == {"value": "string"}
        with pytest.raises(KeyError):
            job.cached_statepoint["c"]
        assert list(job.cached_statepoint.keys()) == ["a", "b", "dict"]

        with pytest.raises(TypeError):
            job.cached_statepoint["c"] = 2

    def test_cached_statepoint_lazy_init(self):
        statepoint = {"a": 0}
        job = self.project.open_job(statepoint=statepoint)
        job.init()
        id_ = job.id

        # Clear the cache to force a lazy load of the cached statepoint
        self.project._sp_cache.clear()
        job = self.project.open_job(id=id_)
        job.cached_statepoint

    def test_no_args_error(self):
        with pytest.raises(ValueError):
            self.project.open_job()

        with pytest.raises(ValueError):
            Job(project=self.project)


class TestConfig(TestJobBase):
    def test_config_str(self):
        str(self.project.config)


class TestJobOpenAndClosing(TestJobBase):
    def test_init(self):
        job = self.open_job(test_token)
        assert not os.path.isdir(job.path)
        job.init()
        assert os.path.isdir(job.path)
        assert os.path.exists(os.path.join(job.path, job.FN_STATE_POINT))

    def test_chained_init(self):
        job = self.open_job(test_token)
        assert not os.path.isdir(job.path)
        job = self.open_job(test_token).init()
        assert os.path.isdir(job.path)
        assert os.path.exists(os.path.join(job.path, job.FN_STATE_POINT))

    def test_construction(self):
        from signac import Project  # noqa: F401

        # The eval statement needs to have Project available
        job = self.open_job(test_token)
        job2 = eval(repr(job))
        assert job == job2

    def test_open_job_close(self):
        with self.open_job(test_token) as job:
            pass
        job.remove()

    def test_open_job_close_manual(self):
        job = self.open_job(test_token)
        job.open()
        job.close()
        job.remove()

    def test_open_job_close_with_error(self):
        job = self.open_job(test_token)

        class TestError(Exception):
            pass

        with pytest.raises(TestError):
            with job:
                raise TestError()
        job.remove()

    def test_reopen_job(self):
        with self.open_job(test_token) as job:
            job_id = job.id
            assert str(job_id) == str(job)

        with self.open_job(test_token) as job:
            assert job.id == job_id
        job.remove()

    def test_close_nonopen_job(self):
        job = self.open_job(test_token)
        job.close()
        with job:
            pass

    def test_close_job_while_open(self):
        rp = os.path.realpath
        cwd = rp(os.getcwd())
        job = self.open_job(test_token)
        with job:
            job.close()
            assert cwd == rp(os.getcwd())

    def test_open_job_recursive(self):
        rp = os.path.realpath
        cwd = rp(os.getcwd())
        job = self.open_job(test_token)
        with job:
            assert rp(job.path) == rp(os.getcwd())
        assert cwd == rp(os.getcwd())
        with job:
            assert rp(job.path) == rp(os.getcwd())
            os.chdir(self.project.path)
        assert cwd == rp(os.getcwd())
        with job:
            assert rp(job.path) == rp(os.getcwd())
            with job:
                assert rp(job.path) == rp(os.getcwd())
            assert rp(job.path) == rp(os.getcwd())
        assert cwd == rp(os.getcwd())
        with job:
            assert rp(job.path) == rp(os.getcwd())
            os.chdir(self.project.path)
            with job:
                assert rp(job.path) == rp(os.getcwd())
            assert rp(os.getcwd()) == rp(self.project.path)
        assert cwd == rp(os.getcwd())
        with job:
            job.close()
            assert cwd == rp(os.getcwd())
            with job:
                assert rp(job.path) == rp(os.getcwd())
            assert cwd == rp(os.getcwd())
        assert cwd == rp(os.getcwd())

    def test_corrupt_workspace(self):
        job = self.open_job(test_token)
        job.init()
        fn_statepoint = os.path.join(job.path, job.FN_STATE_POINT)
        with open(fn_statepoint, "w") as file:
            file.write("corrupted")
        job2 = self.open_job(test_token)
        try:
            logging.disable(logging.ERROR)
            with pytest.raises(JobsCorruptedError):
                job2.init()
        finally:
            logging.disable(logging.NOTSET)
        job2.init(force=True)
        job2.init()


class TestJobDocument(TestJobBase):
    def test_get_set(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        assert not bool(job.document)
        assert len(job.document) == 0
        assert key not in job.document
        job.document[key] = d
        assert bool(job.document)
        assert len(job.document) == 1
        assert key in job.document
        assert job.document[key] == d
        assert job.document.get(key) == d
        assert job.document.get("non-existent-key", d) == d

    def test_del(self):
        key0 = "del0"
        key1 = "del1"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        assert len(job.document) == 0
        assert key0 not in job.document
        job.document[key0] = d0
        assert len(job.document) == 1
        assert key0 in job.document
        job.document[key1] = d1
        assert len(job.document) == 2
        assert key0 in job.document
        assert key1 in job.document
        assert job.document[key0] == d0
        assert job.document[key1] == d1
        del job.document[key0]
        assert len(job.document) == 1
        assert key1 in job.document
        assert key0 not in job.document

    def test_get_set_doc(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        assert not bool(job.doc)
        assert len(job.doc) == 0
        assert key not in job.doc
        job.doc[key] = d
        assert bool(job.doc)
        assert len(job.doc) == 1
        assert key in job.doc
        assert job.doc[key] == d
        assert job.doc.get(key) == d
        assert job.doc.get("non-existent-key", d) == d

    def test_set_set_doc(self):
        key0 = "set_set0"
        key1 = "set_set1"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        assert not bool(job.doc)
        assert len(job.doc) == 0
        assert key0 not in job.doc
        job.doc[key0] = d0
        assert bool(job.doc)
        assert len(job.doc) == 1
        assert key0 in job.doc
        assert job.doc[key0] == d0
        job = self.open_job(test_token)
        assert bool(job.doc)
        assert len(job.doc) == 1
        assert key0 in job.doc
        assert job.doc[key0] == d0
        job = self.open_job(test_token)
        job.document[key1] = d1
        assert bool(job.doc)
        assert len(job.doc) == 2
        assert key0 in job.doc
        assert key1 in job.doc
        assert job.doc[key0] == d0
        assert job.doc[key1] == d1

    def test_get_set_nested(self):
        d0 = "test_data0"
        d1 = "test_data1"
        d2 = "test_data2"
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        assert len(job.document) == 0
        assert "key0" not in job.document
        job.document["key0"] = d0
        assert len(job.document) == 1
        assert "key0" in job.document
        assert job.document["key0"] == d0
        with pytest.raises(AttributeError):
            job.document.key0.key1
        job.document.key0 = {"key1": d0}
        assert len(job.document) == 1
        assert "key0" in job.document
        assert job.document() == {"key0": {"key1": d0}}
        assert job.document["key0"] == {"key1": d0}
        assert job.document["key0"]["key1"] == d0
        assert job.document.key0 == {"key1": d0}
        assert job.document.key0.key1 == d0
        job.document.key0.key1 = d1
        assert job.document == {"key0": {"key1": d1}}
        assert job.document["key0"] == {"key1": d1}
        assert job.document["key0"]["key1"] == d1
        assert job.document.key0 == {"key1": d1}
        assert job.document.key0.key1 == d1
        job.document["key0"]["key1"] = d2
        assert job.document == {"key0": {"key1": d2}}
        assert job.document["key0"] == {"key1": d2}
        assert job.document["key0"]["key1"] == d2
        assert job.document.key0 == {"key1": d2}
        assert job.document.key0.key1 == d2

    def test_get_set_nested_doc(self):
        d0 = "test_data0"
        d1 = "test_data1"
        d2 = "test_data2"
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        assert len(job.doc) == 0
        assert "key0" not in job.doc
        job.doc["key0"] = d0
        assert len(job.doc) == 1
        assert "key0" in job.doc
        assert job.doc["key0"] == d0
        with pytest.raises(AttributeError):
            job.doc.key0.key1
        job.doc.key0 = {"key1": d0}
        assert len(job.doc) == 1
        assert "key0" in job.doc
        assert job.doc() == {"key0": {"key1": d0}}
        assert job.doc["key0"] == {"key1": d0}
        assert job.doc["key0"]["key1"] == d0
        assert job.doc.key0 == {"key1": d0}
        assert job.doc.key0.key1 == d0
        job.doc.key0.key1 = d1
        assert job.doc == {"key0": {"key1": d1}}
        assert job.doc["key0"] == {"key1": d1}
        assert job.doc["key0"]["key1"] == d1
        assert job.doc.key0 == {"key1": d1}
        assert job.doc.key0.key1 == d1
        job.doc["key0"]["key1"] = d2
        assert job.doc == {"key0": {"key1": d2}}
        assert job.doc["key0"] == {"key1": d2}
        assert job.doc["key0"]["key1"] == d2
        assert job.doc.key0 == {"key1": d2}
        assert job.doc.key0.key1 == d2

    def test_assign(self):
        key = "assign"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        assert len(job.document) == 0
        job.document[key] = d0
        assert len(job.document) == 1
        assert job.document() == {key: d0}
        with pytest.raises(ValueError):
            job.document = d1
        job.document = {key: d1}
        assert len(job.document) == 1
        assert job.document() == {key: d1}

    def test_assign_doc(self):
        key = "assign"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        assert len(job.doc) == 0
        job.doc[key] = d0
        assert len(job.doc) == 1
        assert job.doc() == {key: d0}
        with pytest.raises(ValueError):
            job.doc = d1
        job.doc = {key: d1}
        assert len(job.doc) == 1
        assert job.doc() == {key: d1}

    def test_copy_document(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        job.document[key] = d
        assert bool(job.document)
        assert len(job.document) == 1
        assert key in job.document
        assert job.document[key] == d
        assert job.document.get(key) == d
        assert job.document.get("non-existent-key", d) == d
        copy = dict(job.document)
        assert bool(copy)
        assert len(copy) == 1
        assert key in copy
        assert copy[key] == d
        assert copy.get(key) == d
        assert copy.get("non-existent-key", d) == d

    def test_update(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        job.document.update({key: d})
        assert key in job.document

    def test_clear_document(self):
        key = "clear"
        d = "test_data"
        job = self.open_job(test_token)
        job.document[key] = d
        assert key in job.document
        assert len(job.document) == 1
        job.document.clear()
        assert key not in job.document
        assert len(job.document) == 0

    def test_reopen(self):
        key = "clear"
        d = "test_data"
        job = self.open_job(test_token)
        job.document[key] = d
        assert key in job.document
        assert len(job.document) == 1
        job2 = self.open_job(test_token)
        assert key in job2.document
        assert len(job2.document) == 1

    def test_concurrency(self):
        key = "concurrent"
        d = "test_data"
        job = self.open_job(test_token)
        job2 = self.open_job(test_token)
        assert key not in job.document
        assert key not in job2.document
        job.document[key] = d
        assert key in job.document
        assert key in job2.document

    def test_remove(self):
        key = "remove"
        job = self.open_job(test_token)
        job.remove()
        d = "test_data"
        job.document[key] = d
        assert key in job.document
        assert len(job.document) == 1
        fn_test = os.path.join(job.path, "test")
        with open(fn_test, "w") as file:
            file.write("test")
        assert os.path.isfile(fn_test)
        job.remove()
        assert key not in job.document
        assert not os.path.isfile(fn_test)

    def test_clear_job(self):
        key = "clear"
        job = self.open_job(test_token)
        assert job not in self.project
        job.clear()
        assert job not in self.project
        job.clear()
        assert job not in self.project
        job.init()
        assert job in self.project
        job.clear()
        assert job in self.project
        job.clear()
        job.clear()
        assert job in self.project
        d = "test_data"
        job.document[key] = d
        assert job in self.project
        assert key in job.document
        assert len(job.document) == 1
        job.clear()
        assert len(job.document) == 0
        with open(job.fn("test"), "w") as file:
            file.write("test")
        assert job.isfile("test")
        assert job in self.project
        job.clear()
        assert not job.isfile("test")
        assert len(job.document) == 0

    def test_reset(self):
        key = "reset"
        job = self.open_job(test_token)
        assert job not in self.project
        job.reset()
        assert job in self.project
        assert len(job.document) == 0
        job.document[key] = "test_data"
        assert len(job.document) == 1
        job.reset()
        assert job in self.project
        assert len(job.document) == 0

    def test_doc(self):
        key = "test_doc"
        job = self.open_job(test_token)

        def check_content(key, d):
            assert job.doc[key] == d
            assert getattr(job.doc, key) == d
            assert job.doc()[key] == d
            assert job.document[key] == d
            assert getattr(job.document, key) == d
            assert job.document()[key] == d

        d0 = "test_data0"
        job.doc[key] = d0
        check_content(key, d0)
        d1 = "test_data1"
        job.doc[key] = d1
        check_content(key, d1)
        d2 = "test_data2"
        job.document[key] = d2
        check_content(key, d2)
        d3 = "test_data3"
        setattr(job.doc, key, d3)
        check_content(key, d3)

    def test_sp_formatting(self):
        job = self.open_job({"a": 0})
        assert f"{job.statepoint.a}" == str(job.sp.a)
        assert f"{job.sp.a}" == str(job.sp.a)
        assert "{job.statepoint[a]}".format(job=job) == str(job.sp.a)
        assert "{job.sp[a]}".format(job=job) == str(job.sp.a)
        job.sp.a = dict(b=0)
        assert f"{job.statepoint.a.b}" == str(job.sp.a.b)
        assert f"{job.sp.a.b}" == str(job.sp.a.b)
        assert "{job.statepoint[a][b]}".format(job=job) == str(job.sp.a.b)
        assert "{job.sp[a][b]}".format(job=job) == str(job.sp.a.b)

    def test_doc_formatting(self):
        job = self.open_job(test_token)
        job.doc.a = 0
        assert f"{job.doc.a}" == str(job.doc.a)
        assert "{job.doc[a]}".format(job=job) == str(job.doc.a)
        assert f"{job.document.a}" == str(job.doc.a)
        assert "{job.document[a]}".format(job=job) == str(job.doc.a)
        job.doc.a = dict(b=0)
        assert f"{job.doc.a.b}" == str(job.doc.a.b)
        assert f"{job.doc.a.b}" == str(job.doc.a.b)
        assert f"{job.document.a.b}" == str(job.doc.a.b)
        assert "{job.document[a][b]}".format(job=job) == str(job.doc.a.b)

    @pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
    def test_reset_statepoint_job(self):
        key = "move_job"
        d = "test_data"
        src = test_token
        dst = dict(test_token)
        dst["dst"] = True
        src_job = self.open_job(src)
        src_job.document[key] = d
        assert key in src_job.document
        assert len(src_job.document) == 1
        src_job.data[key] = d
        assert key in src_job.data
        assert len(src_job.data) == 1
        src_job.statepoint = dst
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        assert key in dst_job.document
        assert len(dst_job.document) == 1
        assert key not in src_job.document
        assert key in dst_job.data
        assert len(dst_job.data) == 1
        assert key not in src_job.data
        with pytest.raises(RuntimeError):
            src_job.statepoint = dst
        with pytest.raises(DestinationExistsError):
            src_job.statepoint = dst

    @pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
    def test_reset_statepoint_job_lazy_access(self):
        key = "move_job"
        d = "test_data"
        src = test_token
        dst = dict(test_token)
        dst["dst"] = True
        src_job = self.open_job(src)
        src_job.document[key] = d
        assert key in src_job.document
        assert len(src_job.document) == 1
        src_job.data[key] = d
        assert key in src_job.data
        assert len(src_job.data) == 1
        # Clear the project's state point cache to force lazy load
        self.project._sp_cache.clear()
        src_job_by_id = self.open_job(id=src_job.id)
        # Check that the state point will be instantiated lazily during the
        # call to reset_statepoint
        assert src_job_by_id._statepoint_requires_init
        src_job_by_id.statepoint = dst
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        assert key in dst_job.document
        assert len(dst_job.document) == 1
        assert key not in src_job.document
        assert key in dst_job.data
        assert len(dst_job.data) == 1
        assert key not in src_job.data
        with pytest.raises(RuntimeError):
            src_job.statepoint = dst
        with pytest.raises(DestinationExistsError):
            src_job.statepoint = dst

    @pytest.mark.filterwarnings("ignore:reset_statepoint")
    @pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
    def test_update_statepoint(self):
        key = "move_job"
        d = "test_data"
        src = test_token
        extension = {"dst": True}
        dst = dict(src)
        dst.update(extension)
        extension2 = {"dst": False}
        dst2 = dict(src)
        dst2.update(extension2)
        src_job = self.open_job(src)
        src_job.document[key] = d
        assert key in src_job.document
        assert len(src_job.document) == 1
        src_job.data[key] = d
        assert key in src_job.data
        assert len(src_job.data) == 1
        src_job.update_statepoint(extension)
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        assert dst_job.statepoint() == dst
        assert key in dst_job.document
        assert len(dst_job.document) == 1
        assert key not in src_job.document
        assert key in dst_job.data
        assert len(dst_job.data) == 1
        assert key not in src_job.data
        with pytest.raises(RuntimeError):
            src_job.statepoint = dst
        with pytest.raises(DestinationExistsError):
            src_job.statepoint = dst
        with pytest.raises(KeyError):
            dst_job.update_statepoint(extension2)
        dst_job.update_statepoint(extension2, overwrite=True)
        dst2_job = self.open_job(dst2)
        assert dst2_job.statepoint() == dst2
        assert key in dst2_job.document
        assert len(dst2_job.document) == 1
        assert key in dst2_job.data
        assert len(dst2_job.data) == 1


@pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
class TestJobOpenData(TestJobBase):
    @staticmethod
    @contextmanager
    def open_data(job):
        with job.data:
            yield

    def test_get_set(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert not bool(job.data)
            assert len(job.data) == 0
            assert key not in job.data
            job.data[key] = d
            assert bool(job.data)
            assert len(job.data) == 1
            assert key in job.data
            assert job.data[key] == d
            assert job.data.get(key) == d
            assert job.data.get("non-existent-key", d) == d

    def test_del(self):
        key0 = "del0"
        key1 = "del1"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.data) == 0
            assert key0 not in job.data
            job.data[key0] = d0
            assert len(job.data) == 1
            assert key0 in job.data
            job.data[key1] = d1
            assert len(job.data) == 2
            assert key0 in job.data
            assert key1 in job.data
            assert job.data[key0] == d0
            assert job.data[key1] == d1
            del job.data[key0]
            assert len(job.data) == 1
            assert key1 in job.data
            assert key0 not in job.data

    def test_get_set_data(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert not bool(job.data)
            assert len(job.data) == 0
            assert key not in job.data
            job.data[key] = d
            assert bool(job.data)
            assert len(job.data) == 1
            assert key in job.data
            assert job.data[key] == d
            assert job.data.get(key) == d
            assert job.data.get("non-existent-key", d) == d

    def test_set_set_data(self):
        key0 = "set_set0"
        key1 = "set_set1"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert not bool(job.data)
            assert len(job.data) == 0
            assert key0 not in job.data
            job.data[key0] = d0
            assert bool(job.data)
            assert len(job.data) == 1
            assert key0 in job.data
            assert job.data[key0] == d0
        job = self.open_job(test_token)
        with self.open_data(job):
            assert bool(job.data)
            assert len(job.data) == 1
            assert key0 in job.data
            assert job.data[key0] == d0
        job = self.open_job(test_token)
        with self.open_data(job):
            job.data[key1] = d1
            assert bool(job.data)
            assert len(job.data) == 2
            assert key0 in job.data
            assert key1 in job.data
            assert job.data[key0] == d0
            assert job.data[key1] == d1

    def test_get_set_nested(self):
        d0 = "test_data0"
        d1 = "test_data1"
        d2 = "test_data2"
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.data) == 0
            assert "key0" not in job.data
            job.data["key0"] = d0
            assert len(job.data) == 1
            assert "key0" in job.data
            assert job.data["key0"] == d0
            with pytest.raises(AttributeError):
                job.data.key0.key1
            job.data.key0 = {"key1": d0}
            assert len(job.data) == 1
            assert "key0" in job.data
            assert dict(job.data) == {"key0": {"key1": d0}}
            assert job.data["key0"] == {"key1": d0}
            assert job.data["key0"]["key1"] == d0
            assert job.data.key0 == {"key1": d0}
            assert job.data.key0.key1 == d0
            job.data.key0.key1 = d1
            assert job.data == {"key0": {"key1": d1}}
            assert job.data["key0"] == {"key1": d1}
            assert job.data["key0"]["key1"] == d1
            assert job.data.key0 == {"key1": d1}
            assert job.data.key0.key1 == d1
            job.data["key0"]["key1"] = d2
            assert job.data == {"key0": {"key1": d2}}
            assert job.data["key0"] == {"key1": d2}
            assert job.data["key0"]["key1"] == d2
            assert job.data.key0 == {"key1": d2}
            assert job.data.key0.key1 == d2

    def test_get_set_nested_data(self):
        d0 = "test_data0"
        d1 = "test_data1"
        d2 = "test_data2"
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.data) == 0
            assert "key0" not in job.data
            job.data["key0"] = d0
            assert len(job.data) == 1
            assert "key0" in job.data
            assert job.data["key0"] == d0
            with pytest.raises(AttributeError):
                job.data.key0.key1
            job.data.key0 = {"key1": d0}
            assert len(job.data) == 1
            assert "key0" in job.data
            assert dict(job.data) == {"key0": {"key1": d0}}
            assert job.data["key0"] == {"key1": d0}
            assert job.data["key0"]["key1"] == d0
            assert job.data.key0 == {"key1": d0}
            assert job.data.key0.key1 == d0
            job.data.key0.key1 = d1
            assert job.data == {"key0": {"key1": d1}}
            assert job.data["key0"] == {"key1": d1}
            assert job.data["key0"]["key1"] == d1
            assert job.data.key0 == {"key1": d1}
            assert job.data.key0.key1 == d1
            job.data["key0"]["key1"] = d2
            assert job.data == {"key0": {"key1": d2}}
            assert job.data["key0"] == {"key1": d2}
            assert job.data["key0"]["key1"] == d2
            assert job.data.key0 == {"key1": d2}
            assert job.data.key0.key1 == d2

    def test_assign(self):
        key = "assign"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.data) == 0
            job.data[key] = d0
            assert len(job.data) == 1
            assert dict(job.data) == {key: d0}
            with pytest.raises(ValueError):
                job.data = d1
        job.data = {key: d1}
        assert len(job.data) == 1
        assert dict(job.data) == {key: d1}

    def test_assign_data(self):
        key = "assign"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.data) == 0
            job.data[key] = d0
            assert len(job.data) == 1
            assert dict(job.data) == {key: d0}
            with pytest.raises(ValueError):
                job.data = d1
        job.data = {key: d1}
        assert len(job.data) == 1
        assert dict(job.data) == {key: d1}

    def test_copy_data(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.data[key] = d
            assert bool(job.data)
            assert len(job.data) == 1
            assert key in job.data
            assert job.data[key] == d
            assert job.data.get(key) == d
            assert job.data.get("non-existent-key", d) == d
            copy = dict(job.data)
            assert bool(copy)
            assert len(copy) == 1
            assert key in copy
            assert copy[key] == d
            assert copy.get(key) == d
            assert copy.get("non-existent-key", d) == d

    def test_update(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.data.update({key: d})
            assert key in job.data

    def test_clear_data(self):
        key = "clear"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.data[key] = d
            assert key in job.data
            assert len(job.data) == 1
            job.data.clear()
            assert key not in job.data
            assert len(job.data) == 0

    def test_reopen(self):
        key = "clear"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.data[key] = d
            assert key in job.data
            assert len(job.data) == 1
        job2 = self.open_job(test_token)
        with self.open_data(job2):
            assert key in job2.data
            assert len(job2.data) == 1

    def test_concurrency(self):
        key = "concurrent"
        d = "test_data"
        job = self.open_job(test_token)
        job2 = self.open_job(test_token)
        with self.open_data(job):
            with self.open_data(job2):
                assert key not in job.data
                assert key not in job2.data
                job.data[key] = d
                assert key in job.data
                assert key in job2.data

    def test_move_not_initialized(self):
        job = self.open_job(test_token)
        with pytest.raises(RuntimeError):
            job.move(job._project)

    def test_move_intra_project(self):
        job = self.open_job(test_token).init()
        job.move(self.project)  # no-op

    def test_move_inter_project(self):
        job = self.open_job(test_token).init()
        project_a = self.project
        project_b = self.project_class.init_project(
            path=os.path.join(self._tmp_pr, "project_b")
        )
        job.move(project_b)
        job.move(project_a)
        project_b.clone(job)
        with pytest.raises(DestinationExistsError):
            job.move(project_b)

    def test_remove(self):
        key = "remove"
        job = self.open_job(test_token)
        job.remove()
        d = "test_data"
        with self.open_data(job):
            job.data[key] = d
            assert key in job.data
            assert len(job.data) == 1
        fn_test = os.path.join(job.path, "test")
        with open(fn_test, "w") as file:
            file.write("test")
        assert os.path.isfile(fn_test)
        job.remove()
        with self.open_data(job):
            assert key not in job.data
        assert not os.path.isfile(fn_test)

    def test_clear_job(self):
        key = "clear"
        job = self.open_job(test_token)
        assert job not in self.project
        job.clear()
        assert job not in self.project
        job.clear()
        assert job not in self.project
        job.init()
        assert job in self.project
        job.clear()
        assert job in self.project
        job.clear()
        job.clear()
        assert job in self.project
        d = "test_data"
        with self.open_data(job):
            job.data[key] = d
            assert job in self.project
            assert key in job.data
            assert len(job.data) == 1
        job.clear()
        with self.open_data(job):
            assert len(job.data) == 0
        with open(job.fn("test"), "w") as file:
            file.write("test")
        assert job.isfile("test")
        assert job in self.project
        job.clear()
        assert not job.isfile("test")
        with self.open_data(job):
            assert len(job.data) == 0

    def test_reset(self):
        key = "reset"
        job = self.open_job(test_token)
        assert job not in self.project
        job.reset()
        assert job in self.project
        with self.open_data(job):
            assert len(job.data) == 0
            job.data[key] = "test_data"
            assert len(job.data) == 1
        job.reset()
        assert job in self.project
        with self.open_data(job):
            assert len(job.data) == 0

    def test_data(self):
        key = "test_data"
        job = self.open_job(test_token)

        def check_content(key, d):
            assert job.data[key] == d
            assert getattr(job.data, key) == d
            assert dict(job.data)[key] == d
            assert job.data[key] == d
            assert getattr(job.data, key) == d
            assert dict(job.data)[key] == d

        with self.open_data(job):
            d0 = "test_data0"
            job.data[key] = d0
            check_content(key, d0)
            d1 = "test_data1"
            job.data[key] = d1
            check_content(key, d1)
            d2 = "test_data2"
            job.data[key] = d2
            check_content(key, d2)
            d3 = "test_data3"
            setattr(job.data, key, d3)
            check_content(key, d3)

    def test_reset_statepoint_job(self):
        key = "move_job"
        d = "test_data"
        src = test_token
        dst = dict(test_token)
        dst["dst"] = True
        src_job = self.open_job(src)
        with self.open_data(src_job):
            src_job.data[key] = d
            assert key in src_job.data
            assert len(src_job.data) == 1
        src_job.statepoint = dst
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        with self.open_data(dst_job):
            assert key in dst_job.data
            assert len(dst_job.data) == 1
        with self.open_data(src_job):
            assert key not in src_job.data
        with pytest.raises(RuntimeError):
            src_job.statepoint = dst
        with pytest.raises(DestinationExistsError):
            src_job.statepoint = dst

    def test_update_statepoint(self):
        key = "move_job"
        d = "test_data"
        src = test_token
        extension = {"dst": True}
        dst = dict(src)
        dst.update(extension)
        extension2 = {"dst": False}
        dst2 = dict(src)
        dst2.update(extension2)
        src_job = self.open_job(src)
        with self.open_data(src_job):
            src_job.data[key] = d
            assert key in src_job.data
            assert len(src_job.data) == 1
        src_job.update_statepoint(extension)
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        assert dst_job.statepoint() == dst
        with self.open_data(dst_job):
            assert key in dst_job.data
            assert len(dst_job.data) == 1
        with self.open_data(src_job):
            assert key not in src_job.data
        with pytest.raises(RuntimeError):
            src_job.statepoint = dst
        with pytest.raises(DestinationExistsError):
            src_job.statepoint = dst
        with pytest.raises(KeyError):
            dst_job.update_statepoint(extension2)
        dst_job.update_statepoint(extension2, overwrite=True)
        dst2_job = self.open_job(dst2)
        assert dst2_job.statepoint() == dst2
        with self.open_data(dst2_job):
            assert key in dst2_job.data
            assert len(dst2_job.data) == 1

    def test_statepoint_copy(self):
        job = self.open_job(dict(a=test_token, b=test_token)).init()
        id_ = job.id
        sp_copy = copy.copy(job.sp)
        del sp_copy["b"]
        assert "a" in job.sp
        assert "b" not in job.sp
        assert job in self.project
        assert job.id != id_

    def test_statepoint_deepcopy(self):
        job = self.open_job(dict(a=test_token, b=test_token)).init()
        id_ = job.id
        sp_copy = copy.deepcopy(job.sp)
        del sp_copy["b"]
        assert "a" in job.sp
        assert "b" in job.sp
        assert job not in self.project
        assert job.id == id_


@pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
class TestJobClosedData(TestJobOpenData):
    @staticmethod
    @contextmanager
    def open_data(job):
        yield

    def test_implicit_initialization(self):
        job = self.open_job(test_token)
        assert "test" not in job.stores
        assert "foo" not in job.stores.test
        assert list(job.stores.keys()) == []
        assert list(job.stores) == []
        assert "test" not in job.stores
        job.stores.test.foo = True
        assert "test" in job.stores
        assert "foo" in job.stores.test
        assert list(job.stores.keys()) == ["test"]
        assert list(job.stores) == ["test"]


@pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
class TestJobOpenCustomData(TestJobBase):
    @staticmethod
    @contextmanager
    def open_data(job):
        with job.stores.test:
            yield

    def test_get_set(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert not bool(job.stores.test)
            assert len(job.stores.test) == 0
            assert key not in job.stores.test
            job.stores.test[key] = d
            assert bool(job.stores.test)
            assert len(job.stores.test) == 1
            assert key in job.stores.test
            assert job.stores.test[key] == d
            assert job.stores.test.get(key) == d
            assert job.stores.test.get("non-existent-key", d) == d

    def test_del(self):
        key0 = "del0"
        key1 = "del1"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.stores.test) == 0
            assert key0 not in job.stores.test
            job.stores.test[key0] = d0
            assert len(job.stores.test) == 1
            assert key0 in job.stores.test
            job.stores.test[key1] = d1
            assert len(job.stores.test) == 2
            assert key0 in job.stores.test
            assert key1 in job.stores.test
            assert job.stores.test[key0] == d0
            assert job.stores.test[key1] == d1
            del job.stores.test[key0]
            assert len(job.stores.test) == 1
            assert key1 in job.stores.test
            assert key0 not in job.stores.test

    def test_get_set_data(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert not bool(job.stores.test)
            assert len(job.stores.test) == 0
            assert key not in job.stores.test
            job.stores.test[key] = d
            assert bool(job.stores.test)
            assert len(job.stores.test) == 1
            assert key in job.stores.test
            assert job.stores.test[key] == d
            assert job.stores.test.get(key) == d
            assert job.stores.test.get("non-existent-key", d) == d

    def test_set_set_data(self):
        key0 = "set_set0"
        key1 = "set_set1"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert not bool(job.stores.test)
            assert len(job.stores.test) == 0
            assert key0 not in job.stores.test
            job.stores.test[key0] = d0
            assert bool(job.stores.test)
            assert len(job.stores.test) == 1
            assert key0 in job.stores.test
            assert job.stores.test[key0] == d0
        job = self.open_job(test_token)
        with self.open_data(job):
            assert bool(job.stores.test)
            assert len(job.stores.test) == 1
            assert key0 in job.stores.test
            assert job.stores.test[key0] == d0
        job = self.open_job(test_token)
        with self.open_data(job):
            job.stores.test[key1] = d1
            assert bool(job.stores.test)
            assert len(job.stores.test) == 2
            assert key0 in job.stores.test
            assert key1 in job.stores.test
            assert job.stores.test[key0] == d0
            assert job.stores.test[key1] == d1

    def test_get_set_nested(self):
        d0 = "test_data0"
        d1 = "test_data1"
        d2 = "test_data2"
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.stores.test) == 0
            assert "key0" not in job.stores.test
            job.stores.test["key0"] = d0
            assert len(job.stores.test) == 1
            assert "key0" in job.stores.test
            assert job.stores.test["key0"] == d0
            with pytest.raises(AttributeError):
                job.stores.test.key0.key1
            job.stores.test.key0 = {"key1": d0}
            assert len(job.stores.test) == 1
            assert "key0" in job.stores.test
            assert dict(job.stores.test) == {"key0": {"key1": d0}}
            assert job.stores.test["key0"] == {"key1": d0}
            assert job.stores.test["key0"]["key1"] == d0
            assert job.stores.test.key0 == {"key1": d0}
            assert job.stores.test.key0.key1 == d0
            job.stores.test.key0.key1 = d1
            assert job.stores.test == {"key0": {"key1": d1}}
            assert job.stores.test["key0"] == {"key1": d1}
            assert job.stores.test["key0"]["key1"] == d1
            assert job.stores.test.key0 == {"key1": d1}
            assert job.stores.test.key0.key1 == d1
            job.stores.test["key0"]["key1"] = d2
            assert job.stores.test == {"key0": {"key1": d2}}
            assert job.stores.test["key0"] == {"key1": d2}
            assert job.stores.test["key0"]["key1"] == d2
            assert job.stores.test.key0 == {"key1": d2}
            assert job.stores.test.key0.key1 == d2

    def test_get_set_nested_data(self):
        d0 = "test_data0"
        d1 = "test_data1"
        d2 = "test_data2"
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.stores.test) == 0
            assert "key0" not in job.stores.test
            job.stores.test["key0"] = d0
            assert len(job.stores.test) == 1
            assert "key0" in job.stores.test
            assert job.stores.test["key0"] == d0
            with pytest.raises(AttributeError):
                job.stores.test.key0.key1
            job.stores.test.key0 = {"key1": d0}
            assert len(job.stores.test) == 1
            assert "key0" in job.stores.test
            assert dict(job.stores.test) == {"key0": {"key1": d0}}
            assert job.stores.test["key0"] == {"key1": d0}
            assert job.stores.test["key0"]["key1"] == d0
            assert job.stores.test.key0 == {"key1": d0}
            assert job.stores.test.key0.key1 == d0
            job.stores.test.key0.key1 = d1
            assert job.stores.test == {"key0": {"key1": d1}}
            assert job.stores.test["key0"] == {"key1": d1}
            assert job.stores.test["key0"]["key1"] == d1
            assert job.stores.test.key0 == {"key1": d1}
            assert job.stores.test.key0.key1 == d1
            job.stores.test["key0"]["key1"] = d2
            assert job.stores.test == {"key0": {"key1": d2}}
            assert job.stores.test["key0"] == {"key1": d2}
            assert job.stores.test["key0"]["key1"] == d2
            assert job.stores.test.key0 == {"key1": d2}
            assert job.stores.test.key0.key1 == d2

    def test_assign(self):
        key = "assign"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.stores.test) == 0
            job.stores.test[key] = d0
            assert len(job.stores.test) == 1
            assert dict(job.stores.test) == {key: d0}
            with pytest.raises(ValueError):
                job.stores.test = d1
        job.stores.test = {key: d1}
        assert len(job.stores.test) == 1
        assert dict(job.stores.test) == {key: d1}

    def test_assign_data(self):
        key = "assign"
        d0 = "test_data0"
        d1 = "test_data1"
        job = self.open_job(test_token)
        with self.open_data(job):
            assert len(job.stores.test) == 0
            job.stores.test[key] = d0
            assert len(job.stores.test) == 1
            assert dict(job.stores.test) == {key: d0}
            with pytest.raises(ValueError):
                job.stores.test = d1
        job.stores.test = {key: d1}
        assert len(job.stores.test) == 1
        assert dict(job.stores.test) == {key: d1}

    def test_copy_data(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.stores.test[key] = d
            assert bool(job.stores.test)
            assert len(job.stores.test) == 1
            assert key in job.stores.test
            assert job.stores.test[key] == d
            assert job.stores.test.get(key) == d
            assert job.stores.test.get("non-existent-key", d) == d
            copy = dict(job.stores.test)
            assert bool(copy)
            assert len(copy) == 1
            assert key in copy
            assert copy[key] == d
            assert copy.get(key) == d
            assert copy.get("non-existent-key", d) == d

    def test_update(self):
        key = "get_set"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.stores.test.update({key: d})
            assert key in job.stores.test

    def test_clear_data(self):
        key = "clear"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.stores.test[key] = d
            assert key in job.stores.test
            assert len(job.stores.test) == 1
            job.stores.test.clear()
            assert key not in job.stores.test
            assert len(job.stores.test) == 0

    def test_reopen(self):
        key = "reopen"
        d = "test_data"
        job = self.open_job(test_token)
        with self.open_data(job):
            job.stores.test[key] = d
            assert key in job.stores.test
            assert len(job.stores.test) == 1
        job2 = self.open_job(test_token)
        with self.open_data(job2):
            assert key in job2.stores.test
            assert len(job2.stores.test) == 1

    def test_concurrency(self):
        key = "concurrent"
        d = "test_data"
        job = self.open_job(test_token)
        job2 = self.open_job(test_token)
        with self.open_data(job):
            with self.open_data(job2):
                assert key not in job.stores.test
                assert key not in job2.stores.test
                job.stores.test[key] = d
                assert key in job.stores.test
                assert key in job2.stores.test

    def test_remove(self):
        key = "remove"
        job = self.open_job(test_token)
        job.remove()
        d = "test_data"
        with self.open_data(job):
            job.stores.test[key] = d
            assert key in job.stores.test
            assert len(job.stores.test) == 1
        fn_test = os.path.join(job.path, "test")
        with open(fn_test, "w") as file:
            file.write("test")
        assert os.path.isfile(fn_test)
        job.remove()
        with self.open_data(job):
            assert key not in job.stores.test
        assert not os.path.isfile(fn_test)

    def test_clear_job(self):
        key = "clear"
        job = self.open_job(test_token)
        assert job not in self.project
        job.clear()
        assert job not in self.project
        job.clear()
        assert job not in self.project
        job.init()
        assert job in self.project
        job.clear()
        assert job in self.project
        job.clear()
        job.clear()
        assert job in self.project
        d = "test_data"
        with self.open_data(job):
            job.stores.test[key] = d
            assert job in self.project
            assert key in job.stores.test
            assert len(job.stores.test) == 1
        job.clear()
        with self.open_data(job):
            assert len(job.stores.test) == 0
        with open(job.fn("test"), "w") as file:
            file.write("test")
        assert job.isfile("test")
        assert job in self.project
        job.clear()
        assert not job.isfile("test")
        with self.open_data(job):
            assert len(job.stores.test) == 0

    def test_reset(self):
        key = "reset"
        job = self.open_job(test_token)
        assert job not in self.project
        job.reset()
        assert job in self.project
        with self.open_data(job):
            assert len(job.stores.test) == 0
            job.stores.test[key] = "test_data"
            assert len(job.stores.test) == 1
        job.reset()
        assert job in self.project
        with self.open_data(job):
            assert len(job.stores.test) == 0

    def test_data(self):
        key = "test_data"
        job = self.open_job(test_token)

        def check_content(key, d):
            assert job.stores.test[key] == d
            assert getattr(job.stores.test, key) == d
            assert dict(job.stores.test)[key] == d
            assert job.stores.test[key] == d
            assert getattr(job.stores.test, key) == d
            assert dict(job.stores.test)[key] == d

        with self.open_data(job):
            d0 = "test_data0"
            job.stores.test[key] = d0
            check_content(key, d0)
            d1 = "test_data1"
            job.stores.test[key] = d1
            check_content(key, d1)
            d2 = "test_data2"
            job.stores.test[key] = d2
            check_content(key, d2)
            d3 = "test_data3"
            setattr(job.stores.test, key, d3)
            check_content(key, d3)


class TestJobClosedCustomData(TestJobOpenCustomData):
    @staticmethod
    @contextmanager
    def open_data(job):
        yield
