# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import json
import logging
from time import sleep
from stat import S_IREAD

import signac
from signac.errors import Error
from signac.errors import BufferException
from signac.errors import BufferedFileError

from test_project import BaseProjectTest


class BufferedModeTest(BaseProjectTest):

    def test_enter_exit_buffered_mode(self):
        with signac.buffered():
            self.assertTrue(signac.is_buffered())
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
        with self.assertRaises(Error):
            with signac.buffered():
                with signac.buffered(force_write=True):
                    pass

        job = self.project.open_job(dict(a=0))
        job.init()
        job.doc.a = 0

    def test_buffered_mode_change_buffer_size(self):
        with signac.buffered(buffer_size=12):
            self.assertTrue(signac.buffered())
            self.assertEqual(signac.get_buffer_size(), 12)

        with signac.buffered(buffer_size=12):
            self.assertTrue(signac.buffered())
            self.assertEqual(signac.get_buffer_size(), 12)
            with signac.buffered(buffer_size=12):
                self.assertTrue(signac.buffered())
                self.assertEqual(signac.get_buffer_size(), 12)

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
            with self.assertRaises(BufferedFileError):
                with signac.buffered():
                    self.assertEqual(job.doc.a, not x)
                    job.doc.a = x
                    self.assertEqual(job.doc.a, x)
                    sleep(1.0)
                    with open(job.doc._filename, 'wb') as file:
                        file.write(json.dumps({'a': not x}).encode())

            path = os.path.dirname(job.doc._filename)
            mode = os.stat(path).st_mode
            logging.disable(logging.CRITICAL)
            try:
                self.assertEqual(job.doc.a, not x)
                with self.assertRaises(BufferedFileError):
                    with signac.buffered():
                        self.assertEqual(job.doc.a, not x)
                        job.doc.a = x
                        self.assertEqual(job.doc.a, x)
                        os.chmod(path, S_IREAD)  # Trigger permissions error
            finally:
                logging.disable(logging.NOTSET)
                os.chmod(path, mode)

            path = os.path.dirname(job.doc._filename)
            mode = os.stat(path).st_mode
            logging.disable(logging.CRITICAL)
            try:
                self.assertEqual(job.doc.a, not x)
                with self.assertRaises(BufferedFileError):
                    with signac.buffered():
                        self.assertEqual(job.doc.a, not x)
                        job.doc.a = x
                        self.assertEqual(job.doc.a, x)
                        os.chmod(path, S_IREAD)  # Trigger permissions error
            finally:
                logging.disable(logging.NOTSET)
                os.chmod(path, mode)

            break    # only test for one job


if __name__ == '__main__':
    unittest.main()
