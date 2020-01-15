# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import sys
import json
import unittest
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


class BasicShellTest(unittest.TestCase):

    def setUp(self):
        pythonpath = os.environ.get('PYTHONPATH')
        if pythonpath is None:
            pythonpath = [os.getcwd()]
        else:
            pythonpath = [os.getcwd()] + pythonpath.split(':')
        os.environ['PYTHONPATH'] = ':'.join(pythonpath)
        self.tmpdir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self.tmpdir.cleanup)
        self.cwd = os.getcwd()
        self.addCleanup(self.return_to_cwd)
        os.chdir(self.tmpdir.name)

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
        with self.assertRaises(ExitCodeError):
            self.call('python -m signac init second_project'.split())

    def test_project_id(self):
        self.call('python -m signac init my_project'.split())
        self.assertEqual(str(signac.get_project()), 'my_project')
        self.assertEqual(
            self.call('python -m signac project'.split()).strip(),
            'my_project')

    def test_project_workspace(self):
        self.call('python -m signac init my_project'.split())
        self.assertEqual(str(signac.get_project()), 'my_project')
        self.assertEqual(
            os.path.realpath(
                self.call('python -m signac project --workspace'.split()).strip()),
            os.path.realpath(os.path.join(self.tmpdir.name, 'workspace')))

    def test_job_with_argument(self):
        self.call('python -m signac init my_project'.split())
        self.assertEqual(
            self.call(['python', '-m', 'signac', 'job', '{"a": 0}']).strip(),
            '9bfd29df07674bc4aa960cf661b5acd2')

    def test_job_with_argument_workspace(self):
        self.call('python -m signac init my_project'.split())
        wd_path = os.path.join(self.tmpdir.name, 'workspace',
                               '9bfd29df07674bc4aa960cf661b5acd2')
        self.assertEqual(
            os.path.realpath(
                self.call(['python', '-m', 'signac', 'job', '--workspace', '{"a": 0}']).strip()),
            os.path.realpath(wd_path))

    def test_job_with_argument_create_workspace(self):
        self.call('python -m signac init my_project'.split())
        wd_path = os.path.join(self.tmpdir.name, 'workspace',
                               '9bfd29df07674bc4aa960cf661b5acd2')
        self.assertFalse(os.path.isdir(wd_path))
        self.call(['python', '-m', 'signac', 'job', '--create', '{"a": 0}'])
        self.assertTrue(os.path.isdir(wd_path))

    def test_statepoint(self):
        self.call('python -m signac init my_project'.split())
        self.call(['python', '-m', 'signac', 'job', '--create', '{"a": 0}'])
        project = signac.Project()
        for job in project:
            sp = self.call('python -m signac statepoint {}'.format(job).split())
            self.assertEqual(project.open_job(json.loads(sp)), job)

    def test_index(self):
        self.call('python -m signac init my_project'.split())
        self.call('python -m signac project --access'.split())
        project = signac.Project()
        project.open_job({'a': 0}).init()
        self.assertEqual(len(project), 1)
        self.assertEqual(len(list(project.index())), 1)
        self.assertEqual(len(list(signac.index())), 1)
        doc = json.loads(self.call('python -m signac index'.split()))
        self.assertIn('statepoint', doc)
        self.assertEqual(doc['statepoint'], {'a': 0})
        project.open_job({'a': 0}).document['b'] = 0
        doc = json.loads(self.call('python -m signac index'.split()))
        self.assertIn('statepoint', doc)
        self.assertEqual(doc['statepoint'], {'a': 0})
        self.assertIn('b', doc)
        self.assertEqual(doc['b'], 0)

    @unittest.skipIf(WINDOWS, 'Symbolic links are unsupported on Windows.')
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
            self.assertTrue(os.path.isdir('view/job'))
            self.assertEqual(
                os.path.realpath('view/job'),
                os.path.realpath(project.open_job(sp).workspace()))

    @unittest.skipIf(WINDOWS, 'Symbolic links are unsupported on Windows.')
    def test_view(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        sps = [{'a': i} for i in range(3)]
        for sp in sps:
            project.open_job(sp).init()
        os.mkdir('view')
        self.call('python -m signac view'.split())
        for sp in sps:
            self.assertTrue(os.path.isdir('view/a/{}'.format(sp['a'])))
            self.assertTrue(os.path.isdir('view/a/{}/job'.format(sp['a'])))
            self.assertEqual(
                os.path.realpath('view/a/{}/job'.format(sp['a'])),
                os.path.realpath(project.open_job(sp).workspace()))

    def test_find(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        sps = [{'a': i} for i in range(3)]
        for sp in sps:
            project.open_job(sp).init()
        out = self.call('python -m signac find'.split())
        job_ids = out.split(os.linesep)[:-1]
        self.assertEqual(set(job_ids), set(project.find_job_ids()))
        self.assertEqual(
            self.call('python -m signac find'.split() + ['{"a": 0}']).strip(),
            list(project.find_job_ids({'a': 0}))[0])

        # Test the doc_filter
        for job in project.find_jobs():
            job.document['a'] = job.statepoint()['a']

        for i in range(3):
            self.assertEqual(
                self.call('python -m signac find --doc-filter'.split() +
                          ['{"a": ' + str(i) + '}']).strip(),
                list(project.find_job_ids(doc_filter={'a': i}))[0])

    def test_remove(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        sps = [{'a': i} for i in range(3)]
        for sp in sps:
            project.open_job(sp).init()
        job_to_remove = project.open_job({'a': 1})
        job_to_remove.doc.a = 0
        self.assertIn(job_to_remove, project)
        self.assertEqual(job_to_remove.doc.a, 0)
        self.assertEqual(len(job_to_remove.doc), 1)
        self.call('python -m signac rm --clear {}'.format(job_to_remove.get_id()).split())
        self.assertIn(job_to_remove, project)
        self.assertEqual(len(job_to_remove.doc), 0)
        self.call('python -m signac rm {}'.format(job_to_remove.get_id()).split())
        self.assertNotIn(job_to_remove, project)

    def test_shell(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        out = self.call(
            'python -m signac shell',
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        self.assertEqual(out.strip(), '>>> {} None {}'.format(project, len(project)))

    def test_shell_with_jobs(self):
        self.call('python -m signac init my_project'.split())
        project = signac.Project()
        for i in range(3):
            project.open_job(dict(a=i)).init()
        assert len(project)
        out = self.call(
            'python -m signac shell',
            'print(str(project), job, len(list(jobs))); exit()', shell=True)
        self.assertEqual(out.strip(), '>>> {} None {}'.format(project, len(project)))

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
        self.assertEqual(out.strip(), '>>> {} None {}'.format(project, n))

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
        self.assertEqual(out.strip(), '>>> {} {} 1'.format(project, job))


if __name__ == '__main__':
    unittest.main()
