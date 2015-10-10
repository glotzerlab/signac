import os
import sys
import unittest
import warnings
import tempfile

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
        self.addCleanup(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, 'pr')
        self._tmp_wd = os.path.join(self._tmp_dir.name, 'wd')
        self._tmp_fs = os.path.join(self._tmp_dir.name, 'fs')
        os.mkdir(self._tmp_pr)
        os.mkdir(self._tmp_wd)
        os.mkdir(self._tmp_fs)
        self.config = signac.common.config.load_config()
        self.config['default_host'] = 'testing'
        self.config['author'] = 'test_author'
        self.config['author_email'] = 'testauthor@example.com'
        self.config['project'] = 'testing_test_project'
        self.config['project_dir'] = self._tmp_pr
        self.config['workspace_dir'] = self._tmp_wd
        self.config['filestorage_dir'] = self._tmp_fs
        self.config['signac_version'] = signac.VERSION_TUPLE
        self.project = signac.contrib.Project(config=self.config)
        self.addCleanup(self.project.remove, force=True)
        # supress any output
        self.stdout = sys.stdout
        self.cwd = os.getcwd()
        os.chdir(self._tmp_dir.name)
        self.addCleanup(self.switch_back_to_cwd)
        signac.common.config.write_config(self.config, 'signac.rc')
        self.addCleanup(self.project.remove, force=True)

    def switch_back_to_cwd(self):
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
        with self.project.open_job({'blub': 123}): 
            pass

class ScriptConsoleTest(BaseScriptConsoleTest):
    
    def test_info(self):
        self.script('info -a')

    def test_config(self): 
        self.script('config show')
        self.script('config set author_name impostor')
        config = signac.common.config.read_config_file('signac.rc')
        self.assertEqual(config['author_name'], 'impostor')

    def test_clear(self):
        self.add_dummy_job()
        self.assertEqual(len(list(self.project.find_jobs())), 1)
        self.script('--yes clear')
        self.assertEqual(len(list(self.project.find_jobs())), 0)

    def test_remove_jobs(self):
        self.add_dummy_job()
        self.assertEqual(len(list(self.project.find_jobs())), 1)
        self.script('--yes remove --job all')
        self.assertEqual(len(list(self.project.find_jobs())), 0)

    def test_snapshot_restore(self):
        self.add_dummy_job()
        self.assertEqual(len(list(self.project.find_jobs())), 1)
        self.script('snapshot test.tar.gz')
        self.script('--yes clear')
        self.assertEqual(len(list(self.project.find_jobs())), 0)
        self.script('restore test.tar.gz')
        self.assertEqual(len(list(self.project.find_jobs())), 1)

    def test_check(self):
        self.script('check')

    @unittest.skip("This does not work right now.")
    def test_log(self):
        self.project.start_logging()
        self.add_dummy_job()
        for job in self.project.find_jobs():
            with self.project.open_job(job.parameters()):
                pass
        self.assertEqual(self.script('log'), 'No logs available.\n')
        self.assertEqual(self.script('log -l INFO'), 'No logs available.')

if __name__ == '__main__':
    unittest.main()
