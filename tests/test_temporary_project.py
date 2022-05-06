# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os

import signac


class TestTemporaryProject:
    def test_init_context_manager_constructor(self):
        with signac.TemporaryProject() as tmp_project:
            assert os.path.isdir(tmp_project.path)
            for i in range(10):
                tmp_project.open_job(dict(a=i)).init()
            assert len(tmp_project) == 10
        assert not os.path.isdir(tmp_project.path)

    def test_init_project_method(self):
        with signac.TemporaryProject() as project:
            with project.temporary_project() as tmp_project:
                assert os.path.isdir(tmp_project.path)
                for i in range(10):
                    tmp_project.open_job(dict(a=i)).init()
                assert len(tmp_project) == 10
            assert not os.path.isdir(tmp_project.path)
