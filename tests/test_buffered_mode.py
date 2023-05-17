# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import os
import platform
from stat import S_IREAD
from tempfile import TemporaryDirectory
from time import sleep

import pytest
from synced_collections.errors import BufferedError
from test_project import TestProjectBase

import signac

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

    def test_buffered_mode_change_buffer_capacity(self):
        assert not signac.is_buffered()
        with signac.buffered(buffer_capacity=12):
            assert signac.buffered()
            assert signac.get_buffer_capacity() == 12

        assert not signac.is_buffered()

        assert not signac.is_buffered()
        with signac.buffered(buffer_capacity=12):
            assert signac.buffered()
            assert signac.get_buffer_capacity() == 12
            with signac.buffered():
                assert signac.buffered()
                assert signac.get_buffer_capacity() == 12

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
                job2 = self.project.open_job(id=job.id)
                assert job2.sp.a < 0
                job.sp.a = -job.sp.a
                assert job.sp.a > 0
                job2 = self.project.open_job(id=job.id)
                assert job2.sp.a > 0

            for job in self.project:
                assert job.doc.a
                job.doc.a = not job.doc.a
                assert not job.doc.a
                job.doc.a = not job.doc.a
                assert job.doc.a

        routine()
        assert signac.get_current_buffer_size() == 0
        with signac.buffered():
            assert signac.is_buffered()
            routine()
            assert signac.get_current_buffer_size() > 0
        assert signac.get_current_buffer_size() == 0

        for job in self.project:
            x = job.doc.a
            with signac.buffered():
                assert signac.get_current_buffer_size() == 0
                assert job.doc.a == x
                assert signac.get_current_buffer_size() > 0
                job.doc.a = not job.doc.a
                assert job.doc.a == (not x)
                job2 = self.project.open_job(id=job.id)
                assert job2.doc.a == (not x)
            assert signac.get_current_buffer_size() == 0
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
