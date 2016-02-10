import os
import unittest
import subprocess

import signac
from signac.common import six

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


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
        self.tmpdir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self.tmpdir.cleanup)
        self.cwd = os.getcwd()
        self.addCleanup(self.return_to_cwd)
        os.chdir(self.tmpdir.name)

    def return_to_cwd(self):
        os.chdir(self.cwd)

    def call(self, command, input=None):
        p = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate(input=input)
        if p.returncode != 0:
            raise ExitCodeError()
        if six.PY2:
            return str(out)
        else:
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


if __name__ == '__main__':
    unittest.main()
