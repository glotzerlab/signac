# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import logging
import os
import platform
from stat import S_IREAD
from tempfile import TemporaryDirectory
from time import sleep

import pytest
from test_project import TestProjectBase

import signac
from signac.core.synced_collections.errors import BufferedError
from signac.errors import BufferedFileError, Error

PYPY = "PyPy" in platform.python_implementation()


# Determine if we can run permission error tests
with TemporaryDirectory() as tmp_dir:
    path = os.path.join(tmp_dir, "subdir")
    os.mkdir(path)
    mode = os.stat(path).st_mode
    try:
        os.chmod(path, S_IREAD)
        with open(os.path.join(path, "testfile.txt"), "w") as file:
            pass
    except OSError:
        ABLE_TO_PREVENT_WRITE = True
    else:
        ABLE_TO_PREVENT_WRITE = False
    finally:
        os.chmod(path, mode)


@pytest.mark.skipif(PYPY, reason="Buffered mode not supported for PyPy.")
class TestBufferedMode(TestProjectBase):
    def test_enter_exit_buffered_mode(self):
        assert not signac.is_buffered()
        with signac.buffered():
            assert signac.is_buffered()
        assert not signac.is_buffered()

        assert not signac.is_buffered()
        with signac.buffered():
            assert signac.is_buffered()
            with signac.buffered():
                assert signac.is_buffered()
            assert signac.is_buffered()
        assert not signac.is_buffered()

    def test_basic_and_nested(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        assert "a" not in job.doc
        with signac.buffered():
            assert "a" not in job.doc
            job.doc.a = 0
            assert job.doc.a == 0
        assert job.doc.a == 0

        with signac.buffered():
            assert job.doc.a == 0
            job.doc.a = 1
            assert job.doc.a == 1
            with signac.buffered():
                assert job.doc.a == 1
                job.doc.a = 2
                assert job.doc.a == 2
            assert job.doc.a == 2
        assert job.doc.a == 2

    @pytest.mark.xfail(
        reason="The new SyncedCollection does not implement force_write."
    )
    def test_buffered_mode_force_write(self):
        with signac.buffered(force_write=False):
            with signac.buffered(force_write=False):
                pass
        assert not signac.is_buffered()

        with signac.buffered(force_write=True):
            with signac.buffered(force_write=True):
                pass

        with pytest.raises(Error):
            with signac.buffered():
                with signac.buffered(force_write=True):
                    pass
        assert not signac.is_buffered()

    @pytest.mark.xfail(
        reason="The new SyncedCollection does not implement force_write."
    )
    def test_buffered_mode_force_write_with_file_modification(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        job.doc.a = True
        x = job.doc.a
        assert job.doc.a == x
        with pytest.raises(BufferedFileError):
            with signac.buffered():
                assert job.doc.a == x
                job.doc.a = not x
                assert job.doc.a == (not x)
                sleep(1.0)
                with open(job.doc._filename, "wb") as file:
                    file.write(json.dumps({"a": x}).encode())
        assert not signac.is_buffered()
        assert job.doc.a == x

        with signac.buffered(force_write=True):
            assert job.doc.a == x
            job.doc.a = not x
            assert job.doc.a == (not x)
            sleep(1.0)
            with open(job.doc._filename, "wb") as file:
                file.write(json.dumps({"a": x}).encode())
        assert job.doc.a == (not x)

    @pytest.mark.xfail(
        reason="The new SyncedCollection does not implement force_write."
    )
    def test_force_write_mode_with_permission_error(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        job.doc.a = True
        x = job.doc.a
        path = os.path.dirname(job.doc._filename)
        mode = os.stat(path).st_mode
        logging.disable(logging.CRITICAL)
        try:
            assert job.doc.a == x
            with pytest.raises(BufferedFileError):
                with signac.buffered():
                    assert job.doc.a == x
                    job.doc.a = not x
                    assert job.doc.a == (not x)
                    os.chmod(path, S_IREAD)  # Trigger permissions error
        finally:
            logging.disable(logging.NOTSET)
            os.chmod(path, mode)
        assert job.doc.a == x

    def test_buffered_mode_change_buffer_size(self):
        assert not signac.is_buffered()
        signac.set_buffer_size(12)
        with signac.buffered():
            assert signac.buffered()
            assert signac.get_buffer_size() == 12

        assert not signac.is_buffered()

        assert not signac.is_buffered()
        with signac.buffered():
            assert signac.buffered()
            assert signac.get_buffer_size() == 12
            with signac.buffered():
                assert signac.buffered()
                assert signac.get_buffer_size() == 12

        assert not signac.is_buffered()

    def test_integration(self):
        def routine():
            for i in range(1, 4):
                job = self.project.open_job(dict(a=i))
                job.doc.a = True

            for job in self.project:
                assert job.sp.a > 0
                job.sp.a = -job.sp.a
                assert job.sp.a < 0
                with pytest.deprecated_call():
                    job2 = self.project.open_job(id=job.get_id())
                assert job2.sp.a < 0
                job.sp.a = -job.sp.a
                assert job.sp.a > 0
                with pytest.deprecated_call():
                    job2 = self.project.open_job(id=job.get_id())
                assert job2.sp.a > 0

            for job in self.project:
                assert job.doc.a
                job.doc.a = not job.doc.a
                assert not job.doc.a
                job.doc.a = not job.doc.a
                assert job.doc.a

        routine()
        assert signac.get_buffer_load() == 0
        with signac.buffered():
            assert signac.is_buffered()
            routine()
            assert signac.get_buffer_load() > 0
        assert signac.get_buffer_load() == 0

        for job in self.project:
            x = job.doc.a
            with signac.buffered():
                assert signac.get_buffer_load() == 0
                assert job.doc.a == x
                assert signac.get_buffer_load() > 0
                job.doc.a = not job.doc.a
                assert job.doc.a == (not x)
                with pytest.deprecated_call():
                    job2 = self.project.open_job(id=job.get_id())
                assert job2.doc.a == (not x)
            assert signac.get_buffer_load() == 0
            assert job.doc.a == (not x)
            assert job2.doc.a == (not x)

            job.doc.a = x
            with signac.buffered():
                assert job.doc.a == x
                job.doc.a = not x
                assert job.doc.a == (not x)
                job2.doc.a = x
                assert job.doc.a == x
                assert job2.doc.a == x
            assert job.doc.a == x
            assert job2.doc.a == x

            job.doc.a = x
            with signac.buffered():
                assert job.doc.a == x
                job.doc.a = not x
                assert job.doc.a == (not x)
                job2.doc.a = x
                assert job.doc.a == x
                assert job2.doc.a == x
                job.doc.a = not x
                assert job.doc.a == (not x)
                assert job2.doc.a == (not x)
            assert job.doc.a == (not x)
            assert job2.doc.a == (not x)

            assert job.doc.a == (not x)
            with pytest.raises(BufferedError) as cm:
                with signac.buffered():
                    assert job.doc.a == (not x)
                    job.doc.a = x
                    assert job.doc.a == x
                    sleep(1.0)
                    with open(job.doc._filename, "wb") as file:
                        file.write(json.dumps({"a": not x}).encode())
            assert job.doc._filename in cm.value.files

            break  # only test for one job
