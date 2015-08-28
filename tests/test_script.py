import os
import sys
import unittest
import warnings
import tempfile

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

import signac
import signac.contrib.script

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

class ExitCodeError(RuntimeError): pass

class BaseScriptConsoleTest(unittest.TestCase):
    
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix = 'signac_')
        self._tmp_pr = os.path.join(self._tmp_dir.name, 'pr')
        self._tmp_wd = os.path.join(self._tmp_dir.name, 'wd')
        self._tmp_fs = os.path.join(self._tmp_dir.name, 'fs')
        os.environ['COMPDB_AUTHOR_NAME'] = 'signac_test_author'
        os.environ['COMPDB_AUTHOR_EMAIL'] = 'testauthor@example.com'
        os.environ['COMPDB_PROJECT'] = 'testing_signac_test_project'
        os.environ['COMPDB_PROJECT_DIR'] = self._tmp_pr
        os.environ['COMPDB_FILESTORAGE_DIR'] = self._tmp_fs
        os.environ['COMPDB_WORKING_DIR'] = self._tmp_wd
        os.environ['COMPDB_VERSION'] = '0.1.1'
        os.environ['COMPDB_DATABASE_AUTH_MECHANISM'] = 'none'
        os.environ['COMPDB_DATABASE_HOST'] = 'localhost'
        self._project = signac.contrib.get_project()
        # supress any output
        self.stdout = sys.stdout
        self.cwd = os.getcwd()
        os.chdir(self._tmp_dir.name)
        self.addCleanup(self._tmp_dir.cleanup)
        self.addCleanup(self._project.remove, force=True)

    def tearDown(self):
        sys.stdout = self.stdout
        os.chdir(self.cwd)

    def script(self, command):
        output = DummyFile()
        sys.stdout = output
        try:
            ret = signac.contrib.script.main(command.split(' '))
            if ret != 0:
                raise ExitCodeError(ret)
            return output.read()
        finally:
            sys.stdout = self.stdout

    def add_dummy_job(self):
        with self._project.open_job({'blub': 123}): 
            pass

class ScriptConsoleTest(BaseScriptConsoleTest):
    
    def test_info(self):
        self.script('info -a')

    def test_config(self): 
        self.script('config dump')
        self.script('config show')
        with self.assertRaises(ExitCodeError):
            self.script('config set bullshit abc')
        self.script('config set author_name impostor')
        config = signac.core.config.Config()
        config.read('signac.rc')
        self.assertEqual(config['author_name'], 'impostor')

    def test_clear(self):
        self.add_dummy_job()
        self.assertEqual(len(list(self._project.find_jobs())), 1)
        self.script('--yes clear')
        self.assertEqual(len(list(self._project.find_jobs())), 0)

    def test_remove_jobs(self):
        self.add_dummy_job()
        self.assertEqual(len(list(self._project.find_jobs())), 1)
        self.script('--yes remove --job all')
        self.assertEqual(len(list(self._project.find_jobs())), 0)

    def test_snapshot_restore(self):
        self.add_dummy_job()
        self.assertEqual(len(list(self._project.find_jobs())), 1)
        self.script('snapshot test.tar.gz')
        self.script('--yes clear')
        self.assertEqual(len(list(self._project.find_jobs())), 0)
        self.script('restore test.tar.gz')
        self.assertEqual(len(list(self._project.find_jobs())), 1)

    def test_check(self):
        self.script('check')

    @unittest.skip("This does not work right now.")
    def test_log(self):
        self._project.start_logging()
        self.add_dummy_job()
        for job in self._project.find_jobs():
            with self._project.open_job(job.parameters()):
                pass
        self.assertEqual(self.script('log'), 'No logs available.\n')
        self.assertEqual(self.script('log -l INFO'), 'No logs available.')

if __name__ == '__main__':
    unittest.main()
