# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import unittest

import signac


class TemporaryProjectTest(unittest.TestCase):

    def test_init_context_manager_constructor(self):
        with signac.TemporaryProject() as tmp_project:
            self.assertTrue(os.path.isdir(tmp_project.root_directory()))
            for i in range(10):
                tmp_project.open_job(dict(a=i)).init()
            self.assertEqual(len(tmp_project), 10)
        self.assertFalse(os.path.isdir(tmp_project.root_directory()))

    def test_init_project_method(self):
        with signac.TemporaryProject() as project:
            with project.temporary_project() as tmp_project:
                self.assertTrue(os.path.isdir(tmp_project.root_directory()))
                for i in range(10):
                    tmp_project.open_job(dict(a=i)).init()
                self.assertEqual(len(tmp_project), 10)
            self.assertFalse(os.path.isdir(tmp_project.root_directory()))


if __name__ == '__main__':
    unittest.main()
