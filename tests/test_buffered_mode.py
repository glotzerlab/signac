# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import json
import logging
import platform
from time import sleep
from stat import S_IREAD

import signac
from signac.errors import Error
from signac.errors import BufferException
from signac.errors import BufferedFileError
from signac.common import six

from test_project import BaseProjectTest

if six.PY2:
    from ..signac.common.tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


PYPY = 'PyPy' in platform.python_implementation()


# Determine if we can run permission error tests
with TemporaryDirectory() as tmp_dir:
    path = os.path.join(tmp_dir, 'subdir')
    os.mkdir(path)
    mode = os.stat(path).st_mode
    try:
        os.chmod(path, S_IREAD)
        with open(os.path.join(path, 'testfile.txt'), 'w') as file:
            pass
    except (IOError, OSError):
        ABLE_TO_PREVENT_WRITE = True
    else:
        ABLE_TO_PREVENT_WRITE = False
    finally:
        os.chmod(path, mode)


@unittest.skipIf(PYPY, "Buffered mode not supported for PyPy.")
class BufferedModeTest(BaseProjectTest):

    def test_enter_exit_buffered_mode(self):
        self.assertFalse(signac.is_buffered())
        with signac.buffered():
            self.assertTrue(signac.is_buffered())
        self.assertFalse(signac.is_buffered())

        self.assertFalse(signac.is_buffered())
        with signac.buffered():
            self.assertTrue(signac.is_buffered())
            with signac.buffered():
                self.assertTrue(signac.is_buffered())
            self.assertTrue(signac.is_buffered())
        self.assertFalse(signac.is_buffered())

    def test_basic_and_nested(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        self.assertNotIn('a', job.doc)
        with signac.buffered():
            self.assertNotIn('a', job.doc)
            job.doc.a = 0
            self.assertEqual(job.doc.a, 0)
        self.assertEqual(job.doc.a, 0)

        with signac.buffered():
            self.assertEqual(job.doc.a, 0)
            job.doc.a = 1
            self.assertEqual(job.doc.a, 1)
            with signac.buffered():
                self.assertEqual(job.doc.a, 1)
                job.doc.a = 2
                self.assertEqual(job.doc.a, 2)
            self.assertEqual(job.doc.a, 2)
        self.assertEqual(job.doc.a, 2)

    def test_buffered_mode_force_write(self):
        with signac.buffered(force_write=False):
            with signac.buffered(force_write=False):
                pass
        self.assertFalse(signac.is_buffered())

        with signac.buffered(force_write=True):
            with signac.buffered(force_write=True):
                pass

        with self.assertRaises(Error):
            with signac.buffered():
                with signac.buffered(force_write=True):
                    pass
        self.assertFalse(signac.is_buffered())

    def test_buffered_mode_force_write_with_file_modification(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        job.doc.a = True
        x = job.doc.a
        self.assertEqual(job.doc.a, x)
        with self.assertRaises(BufferedFileError):
            with signac.buffered():
                self.assertEqual(job.doc.a, x)
                job.doc.a = not x
                self.assertEqual(job.doc.a, not x)
                sleep(1.0)
                with open(job.doc._filename, 'wb') as file:
                    file.write(json.dumps({'a': x}).encode())
        self.assertFalse(signac.is_buffered())
        self.assertEqual(job.doc.a, x)

        with signac.buffered(force_write=True):
            self.assertEqual(job.doc.a, x)
            job.doc.a = not x
            self.assertEqual(job.doc.a, not x)
            sleep(1.0)
            with open(job.doc._filename, 'wb') as file:
                file.write(json.dumps({'a': x}).encode())
        self.assertEqual(job.doc.a, not x)

    @unittest.skipIf(not ABLE_TO_PREVENT_WRITE, 'unable to trigger permission error')
    def test_force_write_mode_with_permission_error(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        job.doc.a = True
        x = job.doc.a
        path = os.path.dirname(job.doc._filename)
        mode = os.stat(path).st_mode
        logging.disable(logging.CRITICAL)
        try:
            self.assertEqual(job.doc.a, x)
            with self.assertRaises(BufferedFileError):
                with signac.buffered():
                    self.assertEqual(job.doc.a, x)
                    job.doc.a = not x
                    self.assertEqual(job.doc.a, not x)
                    os.chmod(path, S_IREAD)  # Trigger permissions error
        finally:
            logging.disable(logging.NOTSET)
            os.chmod(path, mode)
        self.assertEqual(job.doc.a, x)

    def test_buffered_mode_change_buffer_size(self):
        self.assertFalse(signac.is_buffered())
        with signac.buffered(buffer_size=12):
            self.assertTrue(signac.buffered())
            self.assertEqual(signac.get_buffer_size(), 12)

        self.assertFalse(signac.is_buffered())
        with self.assertRaises(TypeError):
            with signac.buffered(buffer_size=True):
                pass

        self.assertFalse(signac.is_buffered())
        with signac.buffered(buffer_size=12):
            self.assertTrue(signac.buffered())
            self.assertEqual(signac.get_buffer_size(), 12)
            with signac.buffered(buffer_size=12):
                self.assertTrue(signac.buffered())
                self.assertEqual(signac.get_buffer_size(), 12)

        self.assertFalse(signac.is_buffered())
        with self.assertRaises(BufferException):
            with signac.buffered(buffer_size=12):
                self.assertTrue(signac.buffered())
                self.assertEqual(signac.get_buffer_size(), 12)
                with signac.buffered(buffer_size=14):
                    pass

    def test_integration(self):

        def routine():
            for i in range(1, 4):
                job = self.project.open_job(dict(a=i))
                job.doc.a = True

            for job in self.project:
                self.assertTrue(job.sp.a > 0)
                job.sp.a = - job.sp.a
                self.assertTrue(job.sp.a < 0)
                job2 = self.project.open_job(id=job.get_id())
                self.assertTrue(job2.sp.a < 0)
                job.sp.a = - job.sp.a
                self.assertTrue(job.sp.a > 0)
                job2 = self.project.open_job(id=job.get_id())
                self.assertTrue(job2.sp.a > 0)

            for job in self.project:
                self.assertTrue(job.doc.a)
                job.doc.a = not job.doc.a
                self.assertFalse(job.doc.a)
                job.doc.a = not job.doc.a
                self.assertTrue(job.doc.a)

        routine()
        with signac.buffered():
            self.assertTrue(signac.is_buffered())
            routine()

        for job in self.project:
            x = job.doc.a
            with signac.buffered():
                self.assertEqual(job.doc.a, x)
                job.doc.a = not job.doc.a
                self.assertEqual(job.doc.a, not x)
                job2 = self.project.open_job(id=job.get_id())
                self.assertEqual(job2.doc.a, not x)
            self.assertEqual(job.doc.a, not x)
            self.assertEqual(job2.doc.a, not x)

            job.doc.a = x
            with signac.buffered():
                self.assertEqual(job.doc.a, x)
                job.doc.a = not x
                self.assertEqual(job.doc.a, not x)
                job2.doc.a = x
                self.assertEqual(job.doc.a, x)
                self.assertEqual(job2.doc.a, x)
            self.assertEqual(job.doc.a, x)
            self.assertEqual(job2.doc.a, x)

            job.doc.a = x
            with signac.buffered():
                self.assertEqual(job.doc.a, x)
                job.doc.a = not x
                self.assertEqual(job.doc.a, not x)
                job2.doc.a = x
                self.assertEqual(job.doc.a, x)
                self.assertEqual(job2.doc.a, x)
                job.doc.a = not x
                self.assertEqual(job.doc.a, not x)
                self.assertEqual(job2.doc.a, not x)
            self.assertEqual(job.doc.a, not x)
            self.assertEqual(job2.doc.a, not x)

            self.assertEqual(job.doc.a, not x)
            with self.assertRaises(BufferedFileError) as cm:
                with signac.buffered():
                    self.assertEqual(job.doc.a, not x)
                    job.doc.a = x
                    self.assertEqual(job.doc.a, x)
                    sleep(1.0)
                    with open(job.doc._filename, 'wb') as file:
                        file.write(json.dumps({'a': not x}).encode())
            self.assertIn(job.doc._filename, cm.exception.files)

            break    # only test for one job


if __name__ == '__main__':
    unittest.main()
