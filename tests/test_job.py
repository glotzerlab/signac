import unittest
import os
import io
import warnings
import uuid
import copy
import random
import six

import signac.contrib
import signac.common.config

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


# Make sure the jobs created for this test are unique.
test_token = {'test_token': str(uuid.uuid4())}

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')
warnings.filterwarnings(
    'ignore', category=PendingDeprecationWarning, message=r'.*Cache API.*')

BUILTINS = [
    ({'e': [1.0, '1.0', 1, True]}, '4d8058a305b940005be419b30e99bb53'),
    ({'d': True}, '33cf9999de25a715a56339c6c1b28b41'),
    ({'f': (1.0, '1.0', 1, True)}, 'e998db9b595e170bdff936f88ccdbf75'),
    ({'a': 1}, '42b7b4f2921788ea14dac5566e6f06d0'),
    ({'c': '1.0'}, '80fa45716dd3b83fa970877489beb42e'),
    ({'b': 1.0}, '0ba6c5a46111313f11c41a6642520451'),
]


def builtins_dict():
    random.shuffle(BUILTINS)
    d = dict()
    for b in BUILTINS:
        d.update(b[0])
    return d
BUILTINS_HASH = '7a80b58db53bbc544fc27fcaaba2ce44'


def nested_dict():
    d = dict(builtins_dict())
    d['g'] = builtins_dict()
    return d
NESTED_HASH = 'bd6f5828f4410b665bffcec46abeb8f3'


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
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
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
        self.project = signac.contrib.Project(config=self.config)
        # self.addCleanup(self.project.remove, force=True)

    def tearDown(self):
        pass

    def open_job(self, *args, **kwargs):
        project = self.project
        return project.open_job(*args, **kwargs)


class JobIDTest(BaseJobTest):

    def test_builtins(self):
        for p, h in BUILTINS:
            self.assertEqual(str(self.project.open_job(p)), h)
        self.assertEqual(
            str(self.project.open_job(builtins_dict())), BUILTINS_HASH)

    def test_shuffle(self):
        for i in range(10):
            self.assertEqual(
                str(self.project.open_job(builtins_dict())), BUILTINS_HASH)

    def test_nested(self):
        for i in range(10):
            self.assertEqual(
                str(self.project.open_job(nested_dict())), NESTED_HASH)


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

    def test_init(self):
        job = self.open_job(test_token)
        self.assertFalse(os.path.isdir(job.workspace()))
        job.init()
        self.assertTrue(os.path.isdir(job.workspace()))
        self.assertTrue(os.path.exists(os.path.join(job.workspace(), job.FN_MANIFEST)))

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

    def test_copy_document(self):
        key = 'get_set'
        d = testdata()
        job = self.open_job(test_token)
        job.document[key] = d
        self.assertTrue(bool(job.document))
        self.assertEqual(len(job.document), 1)
        self.assertIn(key, job.document)
        self.assertEqual(job.document[key], d)
        self.assertEqual(job.document.get(key), d)
        self.assertEqual(job.document.get('bs', d), d)
        copy = dict(job.document)
        self.assertTrue(bool(copy))
        self.assertEqual(len(copy), 1)
        self.assertIn(key, copy)
        self.assertEqual(copy[key], d)
        self.assertEqual(copy.get(key), d)
        self.assertEqual(copy.get('bs', d), d)

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
