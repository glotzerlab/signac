# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import sys
import json
import pytest
import subprocess
from tempfile import TemporaryDirectory
from signac.common import config

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

    def call(self, command, input=None, shell=False, error=False, raise_error=True):
        p = subprocess.Popen(
            command,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        if input:
            p.stdin.write(input.encode())
        out, err = p.communicate()
        if p.returncode != 0 and raise_error:
            raise ExitCodeError("STDOUT='{}' STDERR='{}'".format(out, err))
        return err.decode() if error else out.decode()

    def test_print_usage(self):
        with pytest.raises(ExitCodeError):
            self.call('python -m signac'.split())

        out = self.call('python -m signac'.split(), raise_error=False)
        assert 'usage:' in out

    def test_version(self):
        out = self.call('python -m signac --version'.split())
        assert 'signac {}'.format(signac.__version__) in out

    def test_help(self):
        out = self.call('python -m signac --help'.split())
        assert 'positional arguments:' in out
        assert 'optional arguments:' in out

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
        assert len(project) == 1
        job = project.open_job({'a': 0})
        sp = self.call('python -m signac statepoint {}'.format(job.id).split())
        assert project.open_job(json.loads(sp)) == job
        assert len(project) == 1
        sp = self.call('python -m signac statepoint'.split())
        assert project.open_job(json.loads(sp)) == job
        assert len(project) == 1
        sp = self.call('python -m signac statepoint --pretty'.split())
        assert "{'a': 0}" in sp
        assert len(project) == 1

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
        doc = json.loads(self.call('python -m signac project --index'.split()))
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
        out = self.call('python -m signac document --pretty'.split())
        for key, value in doc.items():
            assert str(key) in out
            assert str(value) in out

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

        job = project.open_job({'a': 0})
        out = self.call('python -m signac find a 0 --sp'.split()).strip()
        assert out.strip().split(os.linesep) == [str(job.id), str(job.statepoint())]
        out = self.call('python -m signac find a 0 --sp a'.split()).strip()
        assert out.strip().split(os.linesep) == [str(job.id), str(job.statepoint())]
        out = self.call('python -m signac find a 0 --sp b'.split()).strip()
        assert out.strip().split(os.linesep) == [str(job.id), '{}']

        job.document['a'] = 2
        out = self.call('python -m signac find a 0 --doc'.split()).strip()
        assert out.strip().split(os.linesep) == [str(job.id), str(job.document)]
        out = self.call('python -m signac find a 0 --doc a'.split()).strip()
        assert out.strip().split(os.linesep) == [str(job.id), str(job.document)]
        out = self.call('python -m signac find a 0 --doc b'.split()).strip()
        assert out.strip().split(os.linesep) == [str(job.id), '{}']
        out = self.call('python -m signac find a 0 --show --one-line'.split()).strip()
        assert str(job.id) in out
        assert '{"a": 0}' in out
        assert '{"a": 2}' in out

        # Test the doc_filter
        for job in project.find_jobs():
            job.document['a'] = job.statepoint()['a']
        with pytest.deprecated_call():
            for i in range(3):
                assert self.call('python -m signac find --doc-filter'.split() +
                                 ['{"a": ' + str(i) + '}']).strip() == \
                    list(project.find_job_ids(doc_filter={'a': i}))[0]

    def test_diff(self):
        self.call('python -m signac init ProjectA'.split())
        project = signac.Project()
        job_a = project.open_job({"a": 0, "b": 1})
        job_a.init()
        job_b = project.open_job({"a": 0, "b": 0})
        job_b.init()
        out = self.call('python -m signac diff {} {}'.format(job_a.id, job_b.id).split())
        expected = [str(job_a.id), "{'b': 1}", str(job_b.id), "{'b': 0}"]
        assert out.strip().split(os.linesep) == expected

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

        # cloning a job that exist at both source and destination
        err = self.call("python -m signac clone {} {}"
                        .format(os.path.join(self.tmpdir.name, 'b'), job.id).split(), error=True)
        assert 'Destination already exists' in err
        assert len(project_a) == 1
        assert job in project_a
        assert len(project_b) == 1
        assert job in project_b

        # checking for id that does not exit at source
        with pytest.raises(ExitCodeError):
            self.call("python -m signac clone {} 9bfd29df07674bc5"
                      .format(os.path.join(self.tmpdir.name, 'b')).split())
        assert len(project_a) == 1
        assert len(project_b) == 1

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

        # moving a job that already exists at destination
        project_a.open_job({'a': 0}).init()
        err = self.call("python -m signac move {} {}"
                        .format(os.path.join(self.tmpdir.name, 'b'), job.id).split(), error=True)
        assert 'Destination already exists' in err
        assert len(project_a) == 1
        assert job in project_a
        assert len(project_b) == 1
        assert job in project_b

        # moving a job that does not exits
        with pytest.raises(ExitCodeError):
            self.call("python -m signac move {} 9bfd29df07674bc5"
                      .format(os.path.join(self.tmpdir.name, 'b')).split())
        assert len(project_a) == 1
        assert len(project_b) == 1

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
        self.call('python -m signac rm --clear {}'.format(job_to_remove.id).split())
        assert job_to_remove in project
        assert len(job_to_remove.doc) == 0
        self.call('python -m signac -v rm {}'.format(job_to_remove.id).split())
        assert job_to_remove not in project

        # removing job that does not exist at source
        with pytest.raises(ExitCodeError):
            self.call('python -m signac -v rm {}'.format(job_to_remove.id).split())
        assert job_to_remove not in project

    def test_schema(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        for i in range(10):
            project.open_job({
                'a': i,
                'b': {'b2': i},
                'c': [i if i % 2 else None, 0, 0],
                'd': [[i, 0, 0]],
                'e': {'e2': [i, 0, 0]} if i % 2 else 0,  # heterogeneous!
                'f': {'f2': [[i, 0, 0]]},
            }).init()

        s = project.detect_schema()
        out = self.call('python -m signac schema'.split())
        assert s.format() == out.strip().replace(os.linesep, '\n')

    def test_sync(self):
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        for i in range(4):
            project_a.open_job({'a': i}).init()
            project_b.open_job({'a': i}).init()
        job_src = project_b.open_job({'a': 0})
        job_dst = project_a.open_job({'a': 0})
        with job_src:
            with open('test', 'w') as file:
                file.write('x')
        assert len(project_a) == 4
        assert len(project_b) == 4
        project_b.document['a'] = 0
        project_a.document['b'] = 0
        out = self.call('python -m signac sync {} {} --stats --human-readable'
                        .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert 'Number of files transferred: 1' in out
        assert len(project_a) == 4
        assert len(project_b) == 4
        assert 'a' in project_a.document
        assert 'a' in project_b.document
        assert 'b' in project_a.document
        assert 'b' not in project_b.document
        with job_dst:
            with open('test', 'r') as file:
                assert 'x' == file.read()
        # invalid cases
        with pytest.raises(ExitCodeError):
            self.call('python -m signac sync {} {} -s never -u'
                      .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        with pytest.raises(ExitCodeError):
            self.call('python -m signac sync {} {} -t'
                      .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())

    def test_sync_merge(self):
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        for i in range(4):
            project_a.open_job({'a': i}).init()
            project_b.open_job({'a': i}).init()
        project_a.open_job({'c': 1}).init()
        project_b.open_job({'b': 1}).init()
        project_b.open_job({'a': 4}).init()
        assert len(project_a) == 5
        assert len(project_b) == 6

        # sync with projects having diffent schema
        with pytest.raises(ExitCodeError):
            self.call('python -m signac sync {} {}'
                      .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert len(project_a) == 5
        assert len(project_b) == 6

        self.call('python -m signac sync {} {} --merge'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert len(project_a) == 7
        assert len(project_b) == 6

    def test_sync_document(self):
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        job_src = project_a.open_job({'a': 0})
        job_dst = project_b.open_job({'a': 0})

        def reset():
            job_src.document['a'] = 0
            job_src.document['nested'] = dict(a=1)
            job_dst.document['a'] = 1
            job_dst.document['nested'] = dict(a=2)

        # DocumentSyncConflict without any doc-strategy
        reset()
        assert job_dst.document != job_src.document
        with pytest.raises(ExitCodeError):
            self.call('python -m signac sync {} {}'
                      .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert job_dst.document != job_src.document
        # don't sync any key
        self.call('python -m signac sync {} {} --no-key'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert job_dst.document != job_src.document
        assert job_dst.document['a'] != job_src.document['a']
        assert job_dst.document['nested'] != job_src.document['nested']
        # only sync a
        reset()
        self.call('python -m signac sync {} {} --key a'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert job_dst.document != job_src.document
        assert job_dst.document['nested'] != job_src.document['nested']
        assert job_dst.document['a'] == job_src.document['a']
        # only sync nested
        reset()
        self.call('python -m signac sync {} {} --key nested'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert job_dst.document != job_src.document
        assert job_dst.document['a'] != job_src.document['a']
        assert job_dst.document['nested'] == job_src.document['nested']
        # sync both
        reset()
        self.call('python -m signac sync {} {} --all-key'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        assert job_dst.document == job_src.document
        assert job_dst.document['nested'] == job_src.document['nested']
        assert job_dst.document['a'] == job_src.document['a']
        # invalid input
        with pytest.raises(ExitCodeError):
            self.call('python -m signac sync {} {} --all-key --no-key'
                      .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())

    def test_sync_file(self):
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        job_src = project_a.open_job({'a': 0}).init()
        job_dst = project_b.open_job({'a': 0}).init()
        for i, job in enumerate([job_src, job_dst]):
            with open(job.fn('test'), 'w') as file:
                file.write('x'*(i+1))
        # FileSyncConflict
        with pytest.raises(ExitCodeError):
            self.call('python -m signac sync {} {}'
                      .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        self.call('python -m signac sync {} {} --strategy never'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        with open(job_dst.fn('test'), 'r') as file:
            assert file.read() == 'xx'

        with pytest.raises(ExitCodeError):
            self.call('python -m signac sync {} {}'
                      .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())
        self.call('python -m signac sync {} {} --update'
                  .format(os.path.join(self.tmpdir.name, 'b'), self.tmpdir.name).split())

    def test_export(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        prefix_data = os.path.join(self.tmpdir.name, 'data')

        err = self.call("python -m signac export {}"
                        .format(prefix_data).split(), error=True)
        assert 'No jobs to export' in err

        for i in range(10):
            project.open_job({'a': i}).init()
        assert len(project) == 10
        self.call("python -m signac export {}".format(prefix_data).split())
        assert len(project) == 10
        assert len(os.listdir(prefix_data)) == 1
        assert len(os.listdir(os.path.join(prefix_data, 'a'))) == 10
        for i in range(10):
            assert os.path.isdir(os.path.join(prefix_data, 'a', str(i)))

    def test_import(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        prefix_data = os.path.join(self.tmpdir.name, 'data')

        err = self.call("python -m signac import {}"
                        .format(self.tmpdir.name).split(), error=True)
        assert 'Nothing to import.' in err

        for i in range(10):
            project.open_job({'a': i}).init()
        job_ids = list(project.find_job_ids())
        assert len(project) == 10
        project.export_to(target=prefix_data, copytree=os.replace)
        assert len(project) == 0
        self.call("python -m signac import {}".format(prefix_data).split())
        assert len(project) == 10
        assert list(project.find_job_ids()) == job_ids

        # invalid combination
        with pytest.raises(ExitCodeError):
            self.call('python -m signac import {} --sync-interactive --move'
                      .format(prefix_data).split())

    def test_import_sync(self):
        project_b = signac.init_project('ProjectB', os.path.join(self.tmpdir.name, 'b'))
        self.call('python -m signac init ProjectA'.split())
        prefix_data = os.path.join(self.tmpdir.name, 'data')
        project_a = signac.Project()
        for i in range(4):
            project_a.open_job({'a': i}).init()
            project_b.open_job({'a': i}).init()
        job_dst = project_a.open_job({'a': 0})
        job_src = project_b.open_job({'a': 0})
        job_src.document['a'] = 0
        project_b.export_to(prefix_data)
        err = self.call("python -m signac import {}".format(prefix_data).split(), error=True)
        assert "Import failed" in err
        self.call("python -m signac import {} --sync".format(prefix_data).split(), error=True)
        assert len(project_a) == 4
        assert 'a' in job_dst.document
        assert job_dst.document['a'] == 0
        out = self.call(
            'python -m signac import {} --sync-interactive'.format(prefix_data),
            'print(str(tmp_project), len(tmp_project)); exit()', shell=True)
        assert 'ProjectA' in out
        assert '4' in out

    def test_shell(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        out = self.call(
            'python -m signac shell',
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        assert out.strip() == '>>> {} None {}'.format(project, len(project))

        cmd = 'python -m signac shell -c'.split() + ['print(str(project), len(list(jobs)))']
        out = self.call(cmd)
        assert out.strip() == '{} {}'.format(project, len(project))

    def test_shell_with_jobs(self):
        out = self.call('python -m signac shell', shell=True)
        assert 'No project within this directory' in out

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
        assert len(project) == 3
        out = self.call(
            'python -m signac shell -f a 0',
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        job = list(project.find_jobs({'a': 0}))[0]
        assert out.strip() == '>>> {} {} 1'.format(project, job)

    def test_config_show(self):
        err = self.call('python -m signac config --local show'.split(), error=True).strip()
        assert 'Did not find a local configuration file' in err

        self.call('python -m signac init my_project'.split())
        out = self.call('python -m signac config --local show'.split()).strip()
        cfg = config.read_config_file('signac.rc')
        expected = config.Config(cfg).write()
        assert out.split(os.linesep) == expected

        out = self.call('python -m signac config show'.split()).strip()
        cfg = config.load_config()
        expected = config.Config(cfg).write()
        assert out.split(os.linesep) == expected

        out = self.call('python -m signac config --global show'.split()).strip()
        cfg = config.read_config_file(config.FN_CONFIG)
        expected = config.Config(cfg).write()
        assert out.split(os.linesep) == expected

    def test_config_set(self):
        self.call('python -m signac init my_project'.split())
        self.call('python -m signac config set a b'.split())
        cfg = self.call('python -m signac config --local show'.split())
        assert 'a' in cfg
        assert 'a = b' in cfg

        self.call('python -m signac config --local set x.y z'.split())
        cfg = self.call('python -m signac config --local show'.split())
        assert '[x]' in cfg
        assert 'y = z' in cfg

        self.call('python -m signac config --global set b c'.split())
        cfg = self.call('python -m signac config --global show'.split())
        assert 'b' in cfg
        assert 'b = c' in cfg.split(os.linesep)

        # setting password with config set should fail
        with pytest.raises(ExitCodeError):
            self.call('python -m signac config --global set a.password b'.split())

    def test_config_host(self):
        self.call('python -m signac init my_project'.split())
        self.call('python -m signac config --local host Mongo -u abc -p 123'.split())
        cfg = self.call('python -m signac config --local show'.split())
        assert 'Mongo' in cfg
        assert 'username = abc' in cfg
        assert 'password = ***' in cfg

        self.call('python -m signac config --local host Mongo -u abcd'.split())
        cfg = self.call('python -m signac config --local show'.split())
        assert 'Mongo' in cfg
        assert 'username = abcd' in cfg
        assert 'password = ***' in cfg

        out = self.call('python -m signac config --local host Mongo --show-pw'.split())
        assert '123' in out

        self.call('python -m signac config --local host Mongo --remove'.split(),
                  input='y')
        cfg = self.call('python -m signac config --local show'.split())
        assert 'Mongo' not in cfg

    def test_config_verify(self):
        # no config file
        with pytest.raises(ExitCodeError):
            err = self.call('python -m signac config --local verify'.split(), error=True)
        err = self.call('python -m signac config --local verify'.split(),
                        error=True, raise_error=False)
        assert 'Did not find a local configuration file' in err
        self.call('python -m signac init my_project'.split())
        err = self.call('python -m signac config --local verify'.split(), error=True)
        assert 'Passed' in err

        self.call('python -m signac config --local host Mongo -u abc -p 123'.split())
        err = self.call('python -m signac config --local verify'.split(), error=True)
        assert 'hosts.Mongo.password_config.[missing section]' in err

    def test_update_cache(self):
        self.call('python -m signac init ProjectA'.split())
        project_a = signac.Project()
        assert not os.path.isfile(project_a.FN_CACHE)

        for i in range(4):
            project_a.open_job({'a': i}).init()
        err = self.call('python -m signac update-cache'.split(), error=True)
        assert os.path.isfile(project_a.FN_CACHE)
        assert 'Updated cache' in err

        err = self.call('python -m signac update-cache'.split(), error=True)
        assert 'Cache is up to date' in err
