# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging
import os
from tempfile import TemporaryDirectory
from time import sleep

import pytest
from test_job import TestJobBase

import signac
from signac import JSONDict, sync
from signac._utility import _mkdir_p
from signac.errors import DocumentSyncConflict, FileSyncConflict, SchemaSyncConflict
from signac.sync import _DocProxy, _FileModifyProxy


def touch(fname, mode=0o666, dir_fd=None, **kwargs):
    """Utility function for updating a file time stamp.

    Source:
        https://stackoverflow.com/questions/1158076/implement-touch-using-python
    """
    flags = os.O_CREAT | os.O_APPEND
    with os.fdopen(os.open(fname, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(
            f.fileno() if os.utime in os.supports_fd else fname,
            dir_fd=None if os.supports_fd else dir_fd,
            **kwargs,
        )


class TestDocProxy:
    def test_basic(self):
        doc = dict(a=0)
        proxy = _DocProxy(doc)
        assert proxy == proxy
        assert proxy == doc
        str(proxy)
        repr(proxy)
        assert len(proxy) == len(doc)
        assert proxy["a"] == doc["a"]
        assert "a" in proxy
        for key in proxy:
            assert key == "a"
        for key in proxy.keys():
            assert key == "a"
        proxy["a"] = 1
        assert proxy["a"] == doc["a"]
        assert proxy == proxy
        assert proxy == doc
        doc["a"] = 2
        proxy.update(doc)
        assert proxy["a"] == doc["a"]
        assert proxy == proxy
        assert proxy == doc

    def test_dry_run(self):
        doc = dict(a=0)
        proxy = _DocProxy(doc, dry_run=True)
        assert proxy == proxy
        assert proxy == doc
        str(proxy)
        repr(proxy)
        assert len(proxy) == len(doc)
        assert proxy["a"] == doc["a"]
        assert "a" in proxy
        for key in proxy:
            assert key == "a"
        for key in proxy.keys():
            assert key == "a"
        proxy["a"] = 1
        assert proxy["a"] == 0
        assert proxy == proxy
        assert proxy == doc


class TestFileModifyProxy:
    def test_copy(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix="signac_") as tmp:
            fn_src = os.path.join(tmp, "src.txt")
            fn_dst = os.path.join(tmp, "dst.txt")
            touch(fn_src)
            assert os.path.isfile(fn_src)
            assert not os.path.isfile(fn_dst)
            proxy.copy(fn_src, fn_dst)
            assert os.path.isfile(fn_src)
            assert os.path.isfile(fn_dst)

    def test_copy_dry_run(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix="signac_") as tmp:
            fn_src = os.path.join(tmp, "src.txt")
            fn_dst = os.path.join(tmp, "dst.txt")
            with open(fn_src, "w") as file:
                file.write("test")
            assert os.path.isfile(fn_src)
            assert not os.path.isfile(fn_dst)
            proxy.copy(fn_src, fn_dst)
            assert os.path.isfile(fn_src)
            assert not os.path.isfile(fn_dst)

    def test_copytree(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix="signac_") as tmp:
            src = os.path.join(tmp, "src")
            dst = os.path.join(tmp, "dst")
            _mkdir_p(src)
            fn_src = os.path.join(src, "test.txt")
            fn_dst = os.path.join(dst, "test.txt")
            touch(fn_src)
            assert os.path.isfile(fn_src)
            assert not os.path.isfile(fn_dst)
            proxy.copytree(src, dst)
            assert os.path.isfile(fn_src)
            assert os.path.isfile(fn_dst)

    def test_copytree_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix="signac_") as tmp:
            src = os.path.join(tmp, "src")
            dst = os.path.join(tmp, "dst")
            _mkdir_p(src)
            fn_src = os.path.join(src, "test.txt")
            fn_dst = os.path.join(dst, "test.txt")
            touch(fn_src)
            assert os.path.isfile(fn_src)
            assert not os.path.isfile(fn_dst)
            proxy.copytree(src, dst)
            assert os.path.isfile(fn_src)
            assert not os.path.isfile(fn_dst)

    def test_remove(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix="signac_") as tmp:
            fn = os.path.join(tmp, "test.txt")
            assert not os.path.isfile(fn)
            touch(fn)
            assert os.path.isfile(fn)
            proxy.remove(fn)
            assert not os.path.isfile(fn)

    def test_remove_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix="signac_") as tmp:
            fn = os.path.join(tmp, "test.txt")
            assert not os.path.isfile(fn)
            touch(fn)
            assert os.path.isfile(fn)
            proxy.remove(fn)
            assert os.path.isfile(fn)

    def test_create_backup(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix="signac_") as tmp:
            fn = os.path.join(tmp, "test.txt")
            assert not os.path.isfile(fn)
            with open(fn, "w") as file:
                file.write("a")
            assert os.path.isfile(fn)
            with proxy.create_backup(fn) as fn_backup:
                assert os.path.isfile(fn_backup)
            assert os.path.isfile(fn)
            assert not os.path.isfile(fn_backup)
            with pytest.raises(RuntimeError):
                with proxy.create_backup(fn) as fn_backup:
                    assert os.path.isfile(fn_backup)
                    with open(fn, "w") as file:
                        file.write("b")
                    raise RuntimeError()
            assert os.path.isfile(fn)
            assert not os.path.isfile(fn_backup)
            with open(fn) as file:
                assert file.read() == "a"

    def test_create_backup_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix="signac_") as tmp:
            fn = os.path.join(tmp, "test.txt")
            assert not os.path.isfile(fn)
            with open(fn, "w") as file:
                file.write("a")
            assert os.path.isfile(fn)
            with proxy.create_backup(fn) as fn_backup:
                assert not os.path.isfile(fn_backup)
            assert os.path.isfile(fn)
            assert not os.path.isfile(fn_backup)
            with pytest.raises(RuntimeError):
                with proxy.create_backup(fn) as fn_backup:
                    assert not os.path.isfile(fn_backup)
                    with open(fn, "w") as file:
                        file.write("b")
                    raise RuntimeError()
            assert os.path.isfile(fn)
            assert not os.path.isfile(fn_backup)
            with open(fn) as file:
                assert file.read() == "b"


class TestFileModifyProxyDocBackup:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.doc = {}

    def test_create_doc_dict(self):
        proxy = _FileModifyProxy()
        with proxy.create_doc_backup(self.doc) as p:
            pass
        with proxy.create_doc_backup(self.doc) as p:
            p["a"] = 0
        assert len(self.doc) == 1
        assert self.doc["a"] == 0

    def test_create_doc_dict_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with proxy.create_doc_backup(self.doc) as p:
            pass
        with proxy.create_doc_backup(self.doc) as p:
            p["a"] = 0
        assert len(self.doc) == 0

    def test_create_doc_dict_with_error(self):
        proxy = _FileModifyProxy()
        with pytest.raises(RuntimeError):
            with proxy.create_doc_backup(self.doc) as p:
                p["a"] = 0
                raise RuntimeError()
        assert len(self.doc) == 0

    def test_create_doc_dict_with_error_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with pytest.raises(RuntimeError):
            with proxy.create_doc_backup(self.doc) as p:
                p["a"] = 0
                raise RuntimeError()
        assert len(self.doc) == 0


class TestFileModifyProxyJSONDocBackup(TestFileModifyProxyDocBackup):
    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self.doc = JSONDict(filename=os.path.join(self._tmp_dir.name, "doc.json"))


class TestJobSync(TestJobBase):
    def test_sync_no_implicit_init(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})
        assert job_dst not in self.project
        assert job_src not in self.project
        job_dst.sync(job_src)
        assert job_dst not in self.project
        assert job_src not in self.project
        job_src.init()
        assert job_src in self.project
        job_dst.sync(job_src)
        assert job_dst in self.project

    def test_file_sync(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})
        with job_src:
            with open("test", "w") as file:
                file.write("test")
            os.makedirs("subdir")
            with open("subdir/test2", "w") as file:
                file.write("test2")
        assert job_src.isfile("test")
        try:
            logging.disable(logging.WARNING)
            job_dst.sync(job_src)
        finally:
            logging.disable(logging.NOTSET)
        assert job_dst in self.project
        assert job_dst.isfile("test")
        assert not job_dst.isfile("subdir/test2")
        with open(job_dst.fn("test")) as file:
            assert file.read() == "test"

    def test_file_sync_recursive(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})
        with job_src:
            with open("test", "w") as file:
                file.write("test")
            os.makedirs("subdir")
            with open("subdir/test2", "w") as file:
                file.write("test2")
        assert job_src.isfile("test")
        job_dst.sync(job_src, recursive=True)
        assert job_dst in self.project
        assert job_dst.isfile("test")
        assert job_dst.isfile("subdir/test2")
        with open(job_dst.fn("test")) as file:
            assert file.read() == "test"
        with open(job_dst.fn("subdir/test2")) as file:
            assert file.read() == "test2"

    def test_file_sync_deep(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})
        with job_src:
            with open("test", "w") as file:
                file.write("test")
            os.makedirs("subdir")
            with open("subdir/test2", "w") as file:
                file.write("test2")
        assert job_src.isfile("test")
        job_dst.sync(job_src, deep=True, recursive=True)
        assert job_dst in self.project
        assert job_dst.isfile("test")
        assert job_dst.isfile("subdir/test2")
        with open(job_dst.fn("test")) as file:
            assert file.read() == "test"
        with open(job_dst.fn("subdir/test2")) as file:
            assert file.read() == "test2"

    def _reset_differing_jobs(self, jobs):
        for i, job in enumerate(jobs):
            with job:
                with open("test", "w") as file:
                    file.write("x" * i)
                _mkdir_p("subdir")
                with open("subdir/test2", "w") as file:
                    file.write("x" * i)

        def differs(fn):
            x = set()
            for job in jobs:
                with open(job.fn(fn)) as file:
                    x.add(file.read())
            return len(x) > 1

        return differs

    def test_file_sync_with_conflict(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})
        differs = self._reset_differing_jobs((job_dst, job_src))
        assert differs("test")
        assert differs("subdir/test2")
        with pytest.raises(FileSyncConflict):
            job_dst.sync(job_src, recursive=True)
        job_dst.sync(job_src, sync.FileSync.never, recursive=True)
        assert differs("test")
        assert differs("subdir/test2")
        job_dst.sync(job_src, sync.FileSync.always, exclude="test", recursive=True)
        assert differs("test")
        assert differs("subdir/test2")
        job_dst.sync(
            job_src,
            sync.FileSync.always,
            exclude=["test", "non-existent-key"],
            recursive=True,
        )
        assert differs("test")
        assert differs("subdir/test2")
        sleep(1)
        touch(job_src.fn("test"))
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        assert not differs("test")
        touch(job_src.fn("subdir/test2"))
        job_dst.sync(job_src, sync.FileSync.update, exclude="test2", recursive=True)
        assert not differs("test")
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        assert not differs("subdir/test2")

    def test_file_sync_strategies(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})

        def reset():
            return self._reset_differing_jobs((job_dst, job_src))

        differs = reset()
        assert differs("test")
        assert differs("subdir/test2")
        with pytest.raises(FileSyncConflict):
            job_dst.sync(job_src, recursive=True)
        assert differs("test")
        assert differs("subdir/test2")
        job_dst.sync(job_src, sync.FileSync.never, recursive=True)
        assert differs("test")
        assert differs("subdir/test2")
        job_dst.sync(job_src, sync.FileSync.always, recursive=True)
        assert not differs("test")
        assert not differs("subdir/test2")
        reset()
        assert differs("test")
        assert differs("subdir/test2")
        sleep(1)
        touch(job_src.fn("test"))
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        assert not differs("test")
        touch(job_src.fn("subdir/test2"))
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        assert not differs("test")
        assert not differs("subdir/test2")

    def _reset_document_sync(self):
        job_src = self.open_job({"a": 0})
        job_dst = self.open_job({"a": 1})
        job_src.document["a"] = 0
        job_src.document["nested"] = dict(a=1)
        assert job_src.document != job_dst.document
        return job_dst, job_src

    def test_document_sync(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.sync(job_src)
        assert len(job_dst.document) == len(job_src.document)
        assert job_src.document == job_dst.document
        assert job_src.document["a"] == job_dst.document["a"]
        assert job_src.document["nested"]["a"] == job_dst.document["nested"]["a"]
        job_dst.sync(job_src)
        assert job_src.document == job_dst.document

    def test_document_sync_nested(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.document["nested"] = dict(a=0)
        with pytest.raises(DocumentSyncConflict):
            job_dst.sync(job_src)
        assert job_src.document != job_dst.document

    def test_document_sync_explicit_overwrit(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.sync(job_src, doc_sync=sync.DocSync.update)
        assert job_src.document == job_dst.document

    def test_document_sync_overwrite_specific(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey("nested.a"))
        assert job_src.document == job_dst.document

    def test_document_sync_partially_differing(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.document["a"] = 0
        job_dst.sync(job_src)
        assert job_src.document == job_dst.document

    def test_document_sync_differing_keys(self):
        job_dst, job_src = self._reset_document_sync()
        job_src.document["b"] = 1
        job_src.document["nested"]["b"] = 1
        job_dst.sync(job_src)
        assert job_src.document == job_dst.document

    def test_document_sync_no_sync(self):
        job_dst, job_src = self._reset_document_sync()
        assert sync.DocSync.NO_SYNC is False
        job_dst.sync(job_src, doc_sync=False)
        assert job_src.document != job_dst.document
        assert len(job_dst.document) == 0

    def test_document_sync_dst_has_extra_key(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.document["b"] = 2
        assert "b" not in job_src.document
        assert "b" in job_dst.document
        job_dst.sync(job_src)
        assert "b" not in job_src.document
        assert "b" in job_dst.document
        assert job_dst.document != job_src.document
        assert job_dst.document["nested"] == job_src.document["nested"]
        assert job_dst.document["a"] == job_src.document["a"]

    def test_document_sync_with_error(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})
        job_dst.document["a"] = 0
        job_src.document["a"] = 1

        def raise_error(src, dst):
            raise RuntimeError()

        with pytest.raises(RuntimeError):
            job_dst.sync(job_src, doc_sync=raise_error)

    def test_document_sync_with_conflict(self):
        job_dst = self.open_job({"a": 0})
        job_src = self.open_job({"a": 1})

        def reset():
            job_src.document["a"] = 0
            job_src.document["nested"] = dict(a=1)
            job_dst.document["a"] = 1
            job_dst.document["nested"] = dict(a=2)

        reset()
        assert job_dst.document != job_src.document
        with pytest.raises(DocumentSyncConflict):
            job_dst.sync(job_src)
        assert job_dst.document != job_src.document
        job_dst.sync(job_src, doc_sync=sync.DocSync.NO_SYNC)
        assert job_dst.document != job_src.document
        assert job_dst.document["a"] != job_src.document["a"]
        assert job_dst.document["nested"] != job_src.document["nested"]
        reset()  # only sync a
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey("a"))
        assert job_dst.document != job_src.document
        assert job_dst.document["nested"] != job_src.document["nested"]
        assert job_dst.document["a"] == job_src.document["a"]
        reset()  # only sync nested
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey("nested"))
        assert job_dst.document != job_src.document
        assert job_dst.document["a"] != job_src.document["a"]
        assert job_dst.document["nested"] == job_src.document["nested"]
        reset()
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey(r"(nested\.)?a"))
        assert job_dst.document == job_src.document
        assert job_dst.document["nested"] == job_src.document["nested"]
        assert job_dst.document["a"] == job_src.document["a"]
        reset()
        job_dst.sync(
            job_src, doc_sync=sync.DocSync.ByKey(lambda key: key.startswith("a"))
        )
        assert job_dst.document != job_src.document
        assert job_dst.document["nested"] != job_src.document["nested"]
        assert job_dst.document["a"] == job_src.document["a"]
        reset()
        job_dst.sync(
            job_src, doc_sync=sync.DocSync.ByKey(lambda key: key.startswith("nested"))
        )
        assert job_dst.document != job_src.document
        assert job_dst.document["a"] != job_src.document["a"]
        assert job_dst.document["nested"] == job_src.document["nested"]
        reset()
        job_dst.sync(job_src, doc_sync=sync.DocSync.update)
        assert job_dst.document == job_src.document
        assert job_dst.document["nested"] == job_src.document["nested"]
        assert job_dst.document["a"] == job_src.document["a"]


class TestProjectSync:
    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self._tmp_pr_a = os.path.join(self._tmp_dir.name, "pr_a")
        self._tmp_pr_b = os.path.join(self._tmp_dir.name, "pr_b")
        os.mkdir(self._tmp_pr_a)
        os.mkdir(self._tmp_pr_b)
        self.project_a = signac.Project.init_project(path=self._tmp_pr_a)
        self.project_b = signac.Project.init_project(path=self._tmp_pr_b)

    def _init_job(self, job, data="data"):
        with job:
            with open("test.txt", "w") as file:
                file.write(str(data))

    def test_src_and_dst_identical(self):
        with pytest.raises(ValueError):
            self.project_a.sync(self.project_a)

    def test_src_and_dst_empty(self):
        self.project_a.sync(self.project_b)
        assert len(self.project_a) == len(self.project_b)

    def test_src_empty(self):
        for i in range(4):
            self._init_job(self.project_b.open_job({"a": i}))
        self.project_a.sync(self.project_b)
        assert len(self.project_a) == len(self.project_b)

    def test_dst_empty(self):
        for i in range(4):
            self._init_job(self.project_a.open_job({"a": i}))
        self.project_a.sync(self.project_b)
        assert len(self.project_a) == 4
        assert len(self.project_b) == 0

    def test_doc_sync(self):
        self.project_a.document["a"] = 0
        assert "a" in self.project_a.document
        assert "a" not in self.project_b.document
        self.project_a.sync(self.project_b)
        assert "a" in self.project_a.document
        assert "a" not in self.project_b.document
        self.project_b.document["b"] = 1
        self.project_a.sync(self.project_b)
        assert "b" in self.project_a.document
        self.project_a.document["b"] = 2
        with pytest.raises(DocumentSyncConflict):
            self.project_a.sync(self.project_b)
        self.project_a.sync(self.project_b, doc_sync=sync.DocSync.ByKey("b"))

    def _setup_mixed(self):
        for i in range(4):
            if i % 2 == 0:
                self._init_job(self.project_a.open_job({"a": i}))
            if i % 3 == 0:
                self._init_job(self.project_b.open_job({"a": i}))

    def test_mixed(self):
        self._setup_mixed()
        with pytest.raises(SchemaSyncConflict):
            self.project_a.sync(self.project_b)
        assert len(self.project_a) == 2
        assert len(self.project_b) == 2
        self.project_a.sync(self.project_b, check_schema=False)
        assert len(self.project_a) == 3

    def _setup_jobs(self):
        for i in range(4):
            self._init_job(self.project_a.open_job({"a": i}))
            self._init_job(self.project_b.open_job({"a": i}))

    def test_with_conflict(self):
        self._setup_jobs()
        assert len(self.project_a) == len(self.project_b)
        job_a0 = self.project_a.open_job({"a": 0})
        with open(job_a0.fn("test.txt"), "w") as file:
            file.write("newdata")
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(self.project_b)

    def test_with_conflict_never(self):
        self._setup_jobs()
        job_a0 = self.project_a.open_job({"a": 0})
        with open(job_a0.fn("text.txt"), "w") as file:
            file.write("otherdata")
        self.project_a.sync(self.project_b, sync.FileSync.never)
        with open(job_a0.fn("text.txt")) as file:
            assert file.read() == "otherdata"

    def test_selection(self):
        self._setup_jobs()
        assert len(self.project_a) == len(self.project_b)
        job_a0 = self.project_a.open_job({"a": 0})
        with open(job_a0.fn("test.txt"), "w") as file:
            file.write("newdata")
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(self.project_b)
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_a)
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_b)
        assert len(self.project_a.find_jobs({"a": 0})) == 1
        assert len(self.project_b.find_jobs({"a": 0})) == 1
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(
                self.project_b, selection=self.project_a.find_jobs({"a": 0})
            )
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(
                self.project_b, selection=self.project_b.find_jobs({"a": 0})
            )
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(
                self.project_b,
                selection=[job.id for job in self.project_a.find_jobs({"a": 0})],
            )
        with pytest.raises(FileSyncConflict):
            self.project_a.sync(
                self.project_b,
                selection=[job.id for job in self.project_b.find_jobs({"a": 0})],
            )
        f = {"a": {"$ne": 0}}
        self.project_a.sync(self.project_b, selection=self.project_a.find_jobs(f))
        self.project_a.sync(self.project_b, selection=self.project_b.find_jobs(f))
        self.project_a.sync(
            self.project_b, selection=[job.id for job in self.project_a.find_jobs(f)]
        )
        self.project_a.sync(
            self.project_b, selection=[job.id for job in self.project_b.find_jobs(f)]
        )
