# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import sys
import json
import pytest
import subprocess
from tempfile import TemporaryDirectory

import signac


# Skip linked view tests on Windows
WINDOWS = (sys.platform == 'win32')


class DummyFile(object):
    "We redirect sys stdout into this file during console tests."

    def __init__(self):
        self._x = ''

    def write(self, x):
        self._x += x

    def flush(self): pass

    def read(self):
        x = self._x
        self._x = ''
        return x


class ExitCodeError(RuntimeError):
    pass


class TestBasicShell():

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        pythonpath = os.environ.get('PYTHONPATH')
        if pythonpath is None:
            pythonpath = [os.getcwd()]
        else:
            pythonpath = [os.getcwd()] + pythonpath.split(':')
        os.environ['PYTHONPATH'] = ':'.join(pythonpath)
        self.tmpdir = TemporaryDirectory(prefix='signac_')
        request.addfinalizer(self.tmpdir.cleanup)
        self.cwd = os.getcwd()
        os.chdir(self.tmpdir.name)
        request.addfinalizer(self.return_to_cwd)

    def return_to_cwd(self):
        os.chdir(self.cwd)

    def call(self, command, input=None, shell=False):
        p = subprocess.Popen(
            command,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        if input:
            p.stdin.write(input.encode())
        out, err = p.communicate()
        if p.returncode != 0:
            raise ExitCodeError("STDOUT='{}' STDERR='{}'".format(out, err))
        return out.decode()

    def test_init_project(self):
        self.call('python -m signac init my_project'.split())
        assert str(signac.get_project()) == 'my_project'

    def test_init_project_in_project_root(self):
        self.call('python -m signac init my_project'.split())
        assert str(signac.get_project()) == 'my_project'
        with pytest.raises(ExitCodeError):
            self.call('python -m signac init second_project'.split())

    def test_project_id(self):
        self.call('python -m signac init my_project'.split())
        assert str(signac.get_project()) == 'my_project'
        assert self.call('python -m signac project'.split()).strip() == 'my_project'

    def test_project_workspace(self):
        self.call('python -m signac init my_project'.split())
        assert str(signac.get_project()) == 'my_project'
        assert os.path.realpath(self.call(
                'python -m signac project --workspace'.split()).strip()) == \
            os.path.realpath(os.path.join(self.tmpdir.name, 'workspace'))

    def test_job_with_argument(self):
        self.call('python -m signac init my_project'.split())
        assert self.call(['python', '-m', 'signac', 'job',
                          '{"a": 0}']).strip() == '9bfd29df07674bc4aa960cf661b5acd2'

    def test_job_with_argument_workspace(self):
        self.call('python -m signac init my_project'.split())
        wd_path = os.path.join(self.tmpdir.name, 'workspace',
                               '9bfd29df07674bc4aa960cf661b5acd2')
        assert os.path.realpath(self.call(
            ['python', '-m', 'signac', 'job', '--workspace', '{"a": 0}']).strip()) == \
            os.path.realpath(wd_path)

    def test_job_with_argument_create_workspace(self):
        self.call('python -m signac init my_project'.split())
        wd_path = os.path.join(self.tmpdir.name, 'workspace',
                               '9bfd29df07674bc4aa960cf661b5acd2')
        assert not os.path.isdir(wd_path)
        self.call(['python', '-m', 'signac', 'job', '--create', '{"a": 0}'])
        assert os.path.isdir(wd_path)

    def test_statepoint(self):
        self.call('python -m signac init my_project'.split())
        self.call(['python', '-m', 'signac', 'job', '--create', '{"a": 0}'])
        project = signac.Project()
        for job in project:
            sp = self.call('python -m signac statepoint {}'.format(job).split())
            assert project.open_job(json.loads(sp)) == job

    def test_index(self):
        self.call('python -m signac init my_project'.split())
        self.call('python -m signac project --access'.split())
        project = signac.Project()
        project.open_job({'a': 0}).init()
        assert len(project) == 1
        with pytest.deprecated_call():
            assert len(list(project.index())) == 1
            assert len(list(signac.index())) == 1
        doc = json.loads(self.call('python -m signac index'.split()))
        assert 'statepoint' in doc
        assert doc['statepoint'] == {'a': 0}
        project.open_job({'a': 0}).document['b'] = 0
        doc = json.loads(self.call('python -m signac index'.split()))
        assert 'statepoint' in doc
        assert doc['statepoint'] == {'a': 0}
        assert 'b' in doc
        assert doc['b'] == 0

    def test_document(self):
        self.call('python -m signac init my_project'.split())
        self.call('python -m signac project --access'.split())
        project = signac.Project()
        job_a = project.open_job({'a': 0})
        job_a.init()
        assert len(project) == 1
        job_a.document['data'] = 4
        doc = json.loads(self.call('python -m signac document'.split()))
        assert 'data' in doc
        assert doc['data'] == 4
        doc = json.loads(self.call('python -m signac document {}'.format(job_a.id).split()))
        assert 'data' in doc
        assert doc['data'] == 4

    @pytest.mark.skipif(WINDOWS, reason='Symbolic links are unsupported on Windows.')
    def test_view_single(self):
        """Check whether command line views work for single job workspaces."""
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        sps = [{'a': i} for i in range(1)]
        for sp in sps:
            project.open_job(sp).init()
        os.mkdir('view')
        self.call('python -m signac view'.split())
        for sp in sps:
            assert os.path.isdir('view/job')
            assert os.path.realpath('view/job') == \
                os.path.realpath(project.open_job(sp).workspace())

    @pytest.mark.skipif(WINDOWS, reason='Symbolic links are unsupported on Windows.')
    def test_view(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        sps = [{'a': i} for i in range(3)]
        for sp in sps:
            project.open_job(sp).init()
        os.mkdir('view')
        self.call('python -m signac view'.split())
        for sp in sps:
            assert os.path.isdir('view/a/{}'.format(sp['a']))
            assert os.path.isdir('view/a/{}/job'.format(sp['a']))
            assert os.path.realpath('view/a/{}/job'.format(sp['a'])) == \
                os.path.realpath(project.open_job(sp).workspace())

    def test_find(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        sps = [{'a': i} for i in range(3)]
        for sp in sps:
            project.open_job(sp).init()
        out = self.call('python -m signac find'.split())
        job_ids = out.split(os.linesep)[:-1]
        with pytest.deprecated_call():
            assert set(job_ids) == set(project.find_job_ids())
            assert self.call('python -m signac find'.split() + ['{"a": 0}']).strip() == \
                list(project.find_job_ids({'a': 0}))[0]

        # Test the doc_filter
        for job in project.find_jobs():
            job.document['a'] = job.statepoint()['a']
        with pytest.deprecated_call():
            for i in range(3):
                assert self.call('python -m signac find --doc-filter'.split() +
                                 ['{"a": ' + str(i) + '}']).strip() == \
                    list(project.find_job_ids(doc_filter={'a': i}))[0]

    def test_clone(self):
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        job = project_a.open_job({'a': 0})
        job.init()
        assert len(project_a) == 1
        assert len(project_b) == 0
        self.call("python -m signac clone {} {}"
                  .format(os.path.join(self.tmpdir.name, 'b'), job.id).split())
        assert len(project_a) == 1
        assert job in project_a
        assert len(project_b) == 1
        assert job in project_b

    def test_move(self):
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        job = project_a.open_job({'a': 0})
        job.init()
        assert len(project_a) == 1
        assert len(project_b) == 0
        self.call("python -m signac move {} {}"
                  .format(os.path.join(self.tmpdir.name, 'b'), job.id).split())
        assert len(project_a) == 0
        assert job not in project_a
        assert len(project_b) == 1
        assert job in project_b

    def test_remove(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        sps = [{'a': i} for i in range(3)]
        for sp in sps:
            project.open_job(sp).init()
        job_to_remove = project.open_job({'a': 1})
        job_to_remove.doc.a = 0
        assert job_to_remove in project
        assert job_to_remove.doc.a == 0
        assert len(job_to_remove.doc) == 1
        with pytest.deprecated_call():
            self.call('python -m signac rm --clear {}'.format(job_to_remove.get_id()).split())
        assert job_to_remove in project
        assert len(job_to_remove.doc) == 0
        with pytest.deprecated_call():
            self.call('python -m signac rm {}'.format(job_to_remove.get_id()).split())
        assert job_to_remove not in project

    def test_sync(self):
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        for i in range(4):
            project_a.open_job({'a': i}).init()
            project_b.open_job({'a': i}).init()
        assert len(project_a) == 4
        assert len(project_b) == 4
        project_b.document['a'] = 0
        self.call('python -m signac sync {} {}'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert len(project_a) == 4
        assert len(project_b) == 4
        assert 'a' in project_a.document
        assert 'a' in project_b.document

    def test_shell(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        out = self.call(
            'python -m signac shell',
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        assert out.strip() == '>>> {} None {}'.format(project, len(project))

    def test_shell_with_jobs(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        for i in range(3):
            project.open_job(dict(a=i)).init()
        assert len(project)
        out = self.call(
            'python -m signac shell',
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        assert out.strip() == '>>> {} None {}'.format(project, len(project))

    def test_shell_with_jobs_and_selection(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        for i in range(3):
            project.open_job(dict(a=i)).init()
        assert len(project)
        python_command = 'python -m signac shell -f a.{}gt 0'.format('$' if WINDOWS else r'\$')
        out = self.call(
            python_command,
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        n = len(project.find_jobs({'a': {'$gt': 0}}))
        assert out.strip() == '>>> {} None {}'.format(project, n)

    def test_shell_with_jobs_and_selection_only_one_job(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        for i in range(3):
            project.open_job(dict(a=i)).init()
        assert len(project)
        out = self.call(
            'python -m signac shell -f a 0',
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        job = list(project.find_jobs({'a': 0}))[0]
        assert out.strip() == '>>> {} {} 1'.format(project, job)
