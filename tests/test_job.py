import unittest
import os
import io
import warnings
import tempfile
import uuid
import copy

import signac.contrib
import signac.common.config

# Make sure the jobs created for this test are unique.
test_token = {'test_token': str(uuid.uuid4())}

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')
warnings.filterwarnings(
    'ignore', category=PendingDeprecationWarning, message=r'.*Cache API.*')


def config_from_cfg(cfg):
    cfile = io.StringIO('\n'.join(cfg))
    return signac.common.config.get_config(cfile)


def open_job(cfg, *args, **kwargs):
    config = config_from_cfg(cfg)
    project = signac.contrib.project.Project(config=config)
    return project.open_job(*args, **kwargs)


def open_offline_job(cfg, *args, **kwargs):
    config = config_from_cfg(cfg)
    project = signac.contrib.project.Project(config=config)
    return project.open_offline_job(*args, **kwargs)


def testdata():
    return str(uuid.uuid4())


class BaseJobTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, 'pr')
        self._tmp_wd = os.path.join(self._tmp_dir.name, 'wd')
        os.mkdir(self._tmp_pr)
        os.mkdir(self._tmp_wd)
        self.config = signac.common.config.load_config()
        self.config['default_host'] = 'testing'
        self.config['author'] = 'test_author'
        self.config['author_email'] = 'testauthor@example.com'
        self.config['project'] = 'testing_test_project'
        self.config['project_dir'] = self._tmp_pr
        self.config['workspace_dir'] = self._tmp_wd
        self.config['signac_version'] = signac.VERSION_TUPLE
        self.project = signac.contrib.Project(config=self.config)
        # self.addCleanup(self.project.remove, force=True)

    def tearDown(self):
        pass

    def open_job(self, *args, **kwargs):
        project = self.project
        return project.open_job(*args, **kwargs)


class ConfigTest(BaseJobTest):

    def test_set_get_delete(self):
        key, value = list(test_token.items())[0]
        key, value = 'author_name', list(test_token.values())[0]
        config = copy.deepcopy(self.project.config)
        config[key] = value
        self.assertEqual(config[key], value)
        self.assertIn(key, config)
        del config[key]
        self.assertNotIn(key, config)

    def test_update(self):
        key, value = 'author_name', list(test_token.values())[0]
        config = copy.deepcopy(self.project.config)
        config.update({key: value})
        self.assertEqual(config[key], value)
        self.assertIn(key, config)

    def test_set_and_retrieve_version(self):
        fake_version = 0, 0, 0
        self.project.config['signac_version'] = fake_version
        self.assertEqual(self.project.config['signac_version'], fake_version)

    def test_str(self):
        str(self.project.config)


class JobOpenAndClosingTest(BaseJobTest):

    def test_open_job_close(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with self.open_job(test_token) as job:
                pass
            try:
                job.remove()
            except AttributeError:  # not possible for offline jobs
                pass

    def test_open_job_close_manual(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            job = self.open_job(test_token)
            job.open()
            job.close()
            try:
                job.remove()
            except AttributeError:  # not possible for offline jobs
                pass

    def test_open_job_close_with_error(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            job = self.open_job(test_token)

            class TestError(Exception):
                pass
            with self.assertRaises(TestError):
                with job:
                    raise TestError()
            try:
                job.remove()
            except AttributeError:  # not possible for offline jobs
                pass

    def test_reopen_job(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with self.open_job(test_token) as job:
                job_id = job.get_id()
                self.assertEqual(str(job_id), str(job))

            with self.open_job(test_token) as job:
                self.assertEqual(job.get_id(), job_id)
            try:
                job.remove()
            except AttributeError:
                pass


class JobDocumentTest(BaseJobTest):

    def test_get_set(self):
        key = 'get_set'
        d = testdata()
        job = self.open_job(test_token)
        self.assertFalse(bool(job.document))
        self.assertEqual(len(job.document), 0)
        self.assertNotIn(key, job.document)
        job.document[key] = d
        self.assertTrue(bool(job.document))
        self.assertEqual(len(job.document), 1)
        self.assertIn(key, job.document)
        self.assertEqual(job.document[key], d)
        self.assertEqual(job.document.get(key), d)
        self.assertEqual(job.document.get('bs', d), d)

    def test_update(self):
        key = 'get_set'
        d = testdata()
        job = self.open_job(test_token)
        job.document.update({key: d})
        self.assertIn(key, job.document)

    def test_clear(self):
        key = 'clear'
        d = testdata()
        job = self.open_job(test_token)
        job.document[key] = d
        self.assertIn(key, job.document)
        self.assertEqual(len(job.document), 1)
        job.document.clear()
        self.assertNotIn(key, job.document)
        self.assertEqual(len(job.document), 0)

    def test_reopen(self):
        key = 'clear'
        d = testdata()
        job = self.open_job(test_token)
        job.document[key] = d
        self.assertIn(key, job.document)
        self.assertEqual(len(job.document), 1)
        job2 = self.open_job(test_token)
        self.assertIn(key, job2.document)
        self.assertEqual(len(job2.document), 1)

if __name__ == '__main__':
    unittest.main()
