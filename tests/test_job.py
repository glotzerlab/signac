# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import absolute_import
import unittest
import os
import io
import warnings
import uuid
import copy
import random
import json

import signac.contrib
import signac.common.config
from signac.common import six
from signac.errors import DestinationExistsError

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

NESTED_HASH = 'bd6f5828f4410b665bffcec46abeb8f3'


def config_from_cfg(cfg):
    cfile = io.StringIO('\n'.join(cfg))
    return signac.common.config.get_config(cfile)


def open_job(cfg, *args, **kwargs):
    config = config_from_cfg(cfg)
    project = signac.contrib.project.Project(config=config)
    return project.open_job(*args, **kwargs)


def testdata():
    return str(uuid.uuid4())


class BaseJobTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)
        self._tmp_pr = os.path.join(self._tmp_dir.name, 'pr')
        self._tmp_wd = os.path.join(self._tmp_dir.name, 'wd')
        os.mkdir(self._tmp_pr)
        self.config = signac.common.config.load_config()
        self.project = signac.Project.init_project(
            name='testing_test_project',
            root=self._tmp_pr,
            workspace=self._tmp_wd)
        self.project.config['default_host'] = 'testing'

    def tearDown(self):
        pass

    def open_job(self, *args, **kwargs):
        project = self.project
        return project.open_job(*args, **kwargs)

    @classmethod
    def nested_dict(self):
        d = dict(builtins_dict())
        d['g'] = builtins_dict()
        return d


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
                str(self.project.open_job(self.nested_dict())), NESTED_HASH)

    def test_sequences_identity(self):
        job1 = self.project.open_job({'a': [1.0, '1.0', 1, True]})
        job2 = self.project.open_job({'a': (1.0, '1.0', 1, True)})
        self.assertEqual(str(job1), str(job2))
        self.assertEqual(job1.statepoint(), job2.statepoint())


class JobTest(BaseJobTest):

    def test_repr(self):
        job = self.project.open_job({'a': 0})
        job2 = self.project.open_job({'a': 0})
        self.assertEqual(repr(job), repr(job2))
        self.assertEqual(job, job2)

    def test_str(self):
        job = self.project.open_job({'a': 0})
        self.assertEqual(str(job), job.get_id())

    def test_isfile(self):
        job = self.project.open_job({'a': 0})
        fn = 'test.txt'
        fn_ = os.path.join(job.workspace(), fn)
        self.assertFalse(job.isfile(fn))
        job.init()
        self.assertFalse(job.isfile(fn))
        with open(fn_, 'w') as file:
            file.write('hello')
        self.assertTrue(job.isfile(fn))


class JobSPInterfaceTest(BaseJobTest):

    def test_interface_read_only(self):
        sp = self.nested_dict()
        job = self.open_job(sp)
        self.assertEqual(job.statepoint(), json.loads(json.dumps(sp)))
        for x in ('a', 'b', 'c', 'd', 'e'):
            self.assertEqual(getattr(job.sp, x), sp[x])
            self.assertEqual(job.sp[x], sp[x])
        for x in ('a', 'b', 'c', 'd', 'e'):
            self.assertEqual(getattr(job.sp.g, x), sp['g'][x])
            self.assertEqual(job.sp[x], sp[x])
        for x in ('a', 'b', 'c', 'd', 'e'):
            self.assertEqual(job.sp.get(x), sp[x])
            self.assertEqual(job.sp.get(x), sp[x])
            self.assertEqual(job.sp.g.get(x), sp['g'][x])
        self.assertIsNone(job.sp.get('not_in_sp'))
        self.assertIsNone(job.sp.g.get('not_in_sp'))
        self.assertIsNone(job.sp.get('not_in_sp', None))
        self.assertIsNone(job.sp.g.get('not_in_sp', None))
        self.assertEqual(job.sp.get('not_in_sp', 23), 23)
        self.assertEqual(job.sp.g.get('not_in_sp', 23), 23)

    def test_interface_contains(self):
        sp = self.nested_dict()
        job = self.open_job(sp)
        for x in ('a', 'b', 'c', 'd', 'e'):
            self.assertIn(x, job.sp)
            self.assertIn(x, job.sp.g)

    def test_interface_read_write(self):
        sp = self.nested_dict()
        job = self.open_job(sp)
        job.init()
        for x in ('a', 'b', 'c', 'd', 'e'):
            self.assertEqual(getattr(job.sp, x), sp[x])
            self.assertEqual(job.sp[x], sp[x])
        for x in ('a', 'b', 'c', 'd', 'e'):
            self.assertEqual(getattr(job.sp.g, x), sp['g'][x])
            self.assertEqual(job.sp[x], sp[x])
        l = [1, 1.0, '1.0', True, None]
        b = list(l) + [l] + [tuple(l)]
        for v in b:
            for x in ('a', 'b', 'c', 'd', 'e'):
                setattr(job.sp, x, v)
                self.assertEqual(getattr(job.sp, x), v)
                setattr(job.sp.g, x, v)
                self.assertEqual(getattr(job.sp.g, x), v)

    def test_interface_nested_kws(self):
        job = self.open_job({'a.b.c': 0})
        self.assertEqual(job.sp['a.b.c'], 0)
        with self.assertRaises(AttributeError):
            job.sp.a.b.c
        job.sp['a.b.c'] = 1
        self.assertEqual(job.sp['a.b.c'], 1)
        job.sp.a = dict(b=dict(c=2))
        self.assertEqual(job.sp.a.b.c, 2)
        self.assertEqual(job.sp['a']['b']['c'], 2)

    def test_interface_reserved_keywords(self):
        job = self.open_job({'with': 0, 'pop': 1})
        self.assertEqual(job.sp['with'], 0)
        self.assertEqual(job.sp['pop'], 1)
        self.assertEqual(job.sp.pop('with'), 0)
        self.assertNotIn('with', job.sp)

    def test_interface_illegal_type(self):
        job = self.open_job(dict(a=0))
        self.assertEqual(job.sp.a, 0)

        class Foo(object):
            pass
        with self.assertRaises(TypeError):
            job.sp.a = Foo()

    def test_interface_rename(self):
        job = self.open_job(dict(a=0))
        job.init()
        self.assertEqual(job.sp.a, 0)
        job.sp.b = job.sp.pop('a')
        self.assertNotIn('a', job.sp)
        self.assertEqual(job.sp.b, 0)

    def test_interface_add(self):
        job = self.open_job(dict(a=0))
        job.init()
        with self.assertRaises(AttributeError):
            job.sp.b
        job.sp.b = 1
        self.assertIn('b', job.sp)
        self.assertEqual(job.sp.b, 1)

    def test_interface_delete(self):
        job = self.open_job(dict(a=0, b=0))
        job.init()
        self.assertIn('b', job.sp)
        self.assertEqual(job.sp.b, 0)
        del job.sp['b']
        self.assertNotIn('b', job.sp)
        with self.assertRaises(AttributeError):
            job.sp.b

    def test_interface_destination_conflict(self):
        job_a = self.open_job(dict(a=0))
        job_b = self.open_job(dict(b=0))
        job_a.init()
        id_a = job_a.get_id()
        job_a.sp = dict(b=0)
        self.assertEqual(job_a.statepoint(), dict(b=0))
        self.assertEqual(job_a, job_b)
        self.assertNotEqual(job_a.get_id(), id_a)
        job_a = self.open_job(dict(a=0))
        # Moving to existing job, no problem while empty:
        self.assertNotEqual(job_a, job_b)
        job_a.sp = dict(b=0)
        job_a = self.open_job(dict(a=0))
        job_b.init()
        # Moving to an existing job with data leads
        # to an error:
        job_a.document['a'] = 0
        job_b.document['a'] = 0
        self.assertNotEqual(job_a, job_b)
        with self.assertRaises(RuntimeError):
            job_a.sp = dict(b=0)
        with self.assertRaises(DestinationExistsError):
            job_a.sp = dict(b=0)


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
        self.assertEqual(job.workspace(), job.ws)
        self.assertTrue(os.path.isdir(job.workspace()))
        self.assertTrue(os.path.isdir(job.ws))
        self.assertTrue(os.path.exists(os.path.join(job.workspace(), job.FN_MANIFEST)))

    def test_construction(self):
        job = self.open_job(test_token)
        job2 = eval(repr(job))
        self.assertEqual(job, job2)

    def test_open_job_close(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with self.open_job(test_token) as job:
                pass
            job.remove()

    def test_open_job_close_manual(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            job = self.open_job(test_token)
            job.open()
            job.close()
            job.remove()

    def test_open_job_close_with_error(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            job = self.open_job(test_token)

            class TestError(Exception):
                pass
            with self.assertRaises(TestError):
                with job:
                    raise TestError()
            job.remove()

    def test_reopen_job(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with self.open_job(test_token) as job:
                job_id = job.get_id()
                self.assertEqual(str(job_id), str(job))

            with self.open_job(test_token) as job:
                self.assertEqual(job.get_id(), job_id)
            job.remove()

    def test_close_nonopen_job(self):
        job = self.open_job(test_token)
        job.close()
        with job:
            pass

    def test_close_job_while_open(self):
        rp = os.path.realpath
        cwd = rp(os.getcwd())
        job = self.open_job(test_token)
        with job:
            job.close()
            self.assertEqual(cwd, rp(os.getcwd()))

    def test_open_job_recursive(self):
        rp = os.path.realpath
        cwd = rp(os.getcwd())
        job = self.open_job(test_token)
        with job:
            self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
        self.assertEqual(cwd, rp(os.getcwd()))
        with job:
            self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
            os.chdir(self.project.root_directory())
        self.assertEqual(cwd, rp(os.getcwd()))
        with job:
            self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
            with job:
                self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
            self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
        self.assertEqual(cwd, rp(os.getcwd()))
        with job:
            self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
            os.chdir(self.project.root_directory())
            with job:
                self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
            self.assertEqual(rp(os.getcwd()), rp(self.project.root_directory()))
        self.assertEqual(cwd, rp(os.getcwd()))
        with job:
            job.close()
            self.assertEqual(cwd, rp(os.getcwd()))
            with job:
                self.assertEqual(rp(job.workspace()), rp(os.getcwd()))
            self.assertEqual(cwd, rp(os.getcwd()))
        self.assertEqual(cwd, rp(os.getcwd()))

    def test_corrupt_workspace(self):
        job = self.open_job(test_token)
        job.init()
        fn_manifest = os.path.join(job.workspace(), job.FN_MANIFEST)
        with open(fn_manifest, 'w') as file:
            file.write("corrupted")
        job2 = self.open_job(test_token)
        with self.assertRaises(RuntimeError):
            job2.init()


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

    def test_get_set_doc(self):
        key = 'get_set'
        d = testdata()
        job = self.open_job(test_token)
        self.assertFalse(bool(job.doc))
        self.assertEqual(len(job.doc), 0)
        self.assertNotIn(key, job.doc)
        job.doc[key] = d
        self.assertTrue(bool(job.doc))
        self.assertEqual(len(job.doc), 1)
        self.assertIn(key, job.doc)
        self.assertEqual(job.doc[key], d)
        self.assertEqual(job.doc.get(key), d)
        self.assertEqual(job.doc.get('bs', d), d)

    def test_get_set_nested(self):
        d0 = testdata()
        d1 = testdata()
        d2 = testdata()
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        self.assertEqual(len(job.document), 0)
        self.assertNotIn('key0', job.document)
        job.document['key0'] = d0
        self.assertEqual(len(job.document), 1)
        self.assertIn('key0', job.document)
        self.assertEqual(job.document['key0'], d0)
        with self.assertRaises(AttributeError):
            job.document.key0.key1
        job.document.key0 = {'key1': d0}
        self.assertEqual(len(job.document), 1)
        self.assertIn('key0', job.document)
        self.assertEqual(job.document(), {'key0': {'key1': d0}})
        self.assertEqual(job.document['key0'], {'key1': d0})
        self.assertEqual(job.document['key0']['key1'], d0)
        self.assertEqual(job.document.key0, {'key1': d0})
        self.assertEqual(job.document.key0.key1, d0)
        job.document.key0.key1 = d1
        self.assertEqual(job.document, {'key0': {'key1': d1}})
        self.assertEqual(job.document['key0'], {'key1': d1})
        self.assertEqual(job.document['key0']['key1'], d1)
        self.assertEqual(job.document.key0, {'key1': d1})
        self.assertEqual(job.document.key0.key1, d1)
        job.document['key0']['key1'] = d2
        self.assertEqual(job.document, {'key0': {'key1': d2}})
        self.assertEqual(job.document['key0'], {'key1': d2})
        self.assertEqual(job.document['key0']['key1'], d2)
        self.assertEqual(job.document.key0, {'key1': d2})
        self.assertEqual(job.document.key0.key1, d2)

    def test_get_set_nested_doc(self):
        d0 = testdata()
        d1 = testdata()
        d2 = testdata()
        assert d0 != d1 != d2
        job = self.open_job(test_token)
        self.assertEqual(len(job.doc), 0)
        self.assertNotIn('key0', job.doc)
        job.doc['key0'] = d0
        self.assertEqual(len(job.doc), 1)
        self.assertIn('key0', job.doc)
        self.assertEqual(job.doc['key0'], d0)
        with self.assertRaises(AttributeError):
            job.doc.key0.key1
        job.doc.key0 = {'key1': d0}
        self.assertEqual(len(job.doc), 1)
        self.assertIn('key0', job.doc)
        self.assertEqual(job.doc(), {'key0': {'key1': d0}})
        self.assertEqual(job.doc['key0'], {'key1': d0})
        self.assertEqual(job.doc['key0']['key1'], d0)
        self.assertEqual(job.doc.key0, {'key1': d0})
        self.assertEqual(job.doc.key0.key1, d0)
        job.doc.key0.key1 = d1
        self.assertEqual(job.doc, {'key0': {'key1': d1}})
        self.assertEqual(job.doc['key0'], {'key1': d1})
        self.assertEqual(job.doc['key0']['key1'], d1)
        self.assertEqual(job.doc.key0, {'key1': d1})
        self.assertEqual(job.doc.key0.key1, d1)
        job.doc['key0']['key1'] = d2
        self.assertEqual(job.doc, {'key0': {'key1': d2}})
        self.assertEqual(job.doc['key0'], {'key1': d2})
        self.assertEqual(job.doc['key0']['key1'], d2)
        self.assertEqual(job.doc.key0, {'key1': d2})
        self.assertEqual(job.doc.key0.key1, d2)

    def test_assign(self):
        key = 'assign'
        d0 = testdata()
        d1 = testdata()
        job = self.open_job(test_token)
        self.assertEqual(len(job.document), 0)
        job.document[key] = d0
        self.assertEqual(len(job.document), 1)
        self.assertEqual(job.document(), {key: d0})
        with self.assertRaises(ValueError):
            job.document = d1
        job.document = {key: d1}
        self.assertEqual(len(job.document), 1)
        self.assertEqual(job.document(), {key: d1})

    def test_assign_doc(self):
        key = 'assign'
        d0 = testdata()
        d1 = testdata()
        job = self.open_job(test_token)
        self.assertEqual(len(job.doc), 0)
        job.doc[key] = d0
        self.assertEqual(len(job.doc), 1)
        self.assertEqual(job.doc(), {key: d0})
        with self.assertRaises(ValueError):
            job.doc = d1
        job.doc = {key: d1}
        self.assertEqual(len(job.doc), 1)
        self.assertEqual(job.doc(), {key: d1})

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

    def test_clear_document(self):
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

    def test_concurrency(self):
        key = 'concurrent'
        d = testdata()
        job = self.open_job(test_token)
        job2 = self.open_job(test_token)
        self.assertNotIn(key, job.document)
        self.assertNotIn(key, job2.document)
        job.document[key] = d
        self.assertIn(key, job.document)
        self.assertIn(key, job2.document)

    def test_remove(self):
        key = 'remove'
        job = self.open_job(test_token)
        job.remove()
        d = testdata()
        job.document[key] = d
        self.assertIn(key, job.document)
        self.assertEqual(len(job.document), 1)
        fn_test = os.path.join(job.workspace(), 'test')
        with open(fn_test, 'w') as file:
            file.write('test')
        self.assertTrue(os.path.isfile(fn_test))
        job.remove()
        self.assertNotIn(key, job.document)
        self.assertFalse(os.path.isfile(fn_test))

    def test_clear_job(self):
        key = 'clear'
        job = self.open_job(test_token)
        self.assertNotIn(job, self.project)
        job.clear()
        self.assertNotIn(job, self.project)
        job.clear()
        self.assertNotIn(job, self.project)
        job.init()
        self.assertIn(job, self.project)
        job.clear()
        self.assertIn(job, self.project)
        job.clear()
        job.clear()
        self.assertIn(job, self.project)
        d = testdata()
        job.document[key] = d
        self.assertIn(job, self.project)
        self.assertIn(key, job.document)
        self.assertEqual(len(job.document), 1)
        job.clear()
        self.assertEqual(len(job.document), 0)
        with open(job.fn('test'), 'w') as file:
            file.write('test')
        self.assertTrue(job.isfile('test'))
        self.assertIn(job, self.project)
        job.clear()
        self.assertFalse(job.isfile('test'))
        self.assertEqual(len(job.document), 0)

    def test_reset(self):
        key = 'reset'
        job = self.open_job(test_token)
        self.assertNotIn(job, self.project)
        job.reset()
        self.assertIn(job, self.project)
        self.assertEqual(len(job.document), 0)
        job.document[key] = testdata()
        self.assertEqual(len(job.document), 1)
        job.reset()
        self.assertIn(job, self.project)
        self.assertEqual(len(job.document), 0)

    def test_doc(self):
        key = 'test_doc'
        job = self.open_job(test_token)

        def check_content(key, d):
            self.assertEqual(job.doc[key], d)
            self.assertEqual(getattr(job.doc, key), d)
            self.assertEqual(job.doc()[key], d)
            self.assertEqual(job.document[key], d)
            self.assertEqual(getattr(job.document, key), d)
            self.assertEqual(job.document()[key], d)

        d = testdata()
        job.doc[key] = d
        check_content(key, d)
        d2 = testdata()
        job.doc[key] = d2
        check_content(key, d2)
        d3 = testdata()
        job.document[key] = d3
        check_content(key, d3)
        d4 = testdata()
        setattr(job.doc, key, d4)
        check_content(key, d4)

    def test_sp_formatting(self):
        job = self.open_job({'a': 0})
        self.assertEqual('{job.statepoint.a}'.format(job=job), str(job.sp.a))
        self.assertEqual('{job.sp.a}'.format(job=job), str(job.sp.a))
        self.assertEqual('{job.statepoint[a]}'.format(job=job), str(job.sp.a))
        self.assertEqual('{job.sp[a]}'.format(job=job), str(job.sp.a))
        job.sp.a = dict(b=0)
        self.assertEqual('{job.statepoint.a.b}'.format(job=job), str(job.sp.a.b))
        self.assertEqual('{job.sp.a.b}'.format(job=job), str(job.sp.a.b))
        self.assertEqual('{job.statepoint[a][b]}'.format(job=job), str(job.sp.a.b))
        self.assertEqual('{job.sp[a][b]}'.format(job=job), str(job.sp.a.b))

    def test_doc_formatting(self):
        job = self.open_job(test_token)
        job.doc.a = 0
        self.assertEqual('{job.doc.a}'.format(job=job), str(job.doc.a))
        self.assertEqual('{job.doc[a]}'.format(job=job), str(job.doc.a))
        self.assertEqual('{job.document.a}'.format(job=job), str(job.doc.a))
        self.assertEqual('{job.document[a]}'.format(job=job), str(job.doc.a))
        job.doc.a = dict(b=0)
        self.assertEqual('{job.doc.a.b}'.format(job=job), str(job.doc.a.b))
        self.assertEqual('{job.doc.a.b}'.format(job=job), str(job.doc.a.b))
        self.assertEqual('{job.document.a.b}'.format(job=job), str(job.doc.a.b))
        self.assertEqual('{job.document[a][b]}'.format(job=job), str(job.doc.a.b))

    def test_reset_statepoint_job(self):
        key = 'move_job'
        d = testdata()
        src = test_token
        dst = dict(test_token)
        dst['dst'] = True
        src_job = self.open_job(src)
        src_job.document[key] = d
        self.assertIn(key, src_job.document)
        self.assertEqual(len(src_job.document), 1)
        src_job.reset_statepoint(dst)
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        self.assertIn(key, dst_job.document)
        self.assertEqual(len(dst_job.document), 1)
        self.assertNotIn(key, src_job.document)
        with self.assertRaises(RuntimeError):
            src_job.reset_statepoint(dst)
        with self.assertRaises(DestinationExistsError):
            src_job.reset_statepoint(dst)

    def test_reset_statepoint_project(self):
        key = 'move_job'
        d = testdata()
        src = test_token
        dst = dict(test_token)
        dst['dst'] = True
        src_job = self.open_job(src)
        src_job.document[key] = d
        self.assertIn(key, src_job.document)
        self.assertEqual(len(src_job.document), 1)
        self.project.reset_statepoint(src_job, dst)
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        self.assertIn(key, dst_job.document)
        self.assertEqual(len(dst_job.document), 1)
        self.assertNotIn(key, src_job.document)
        with self.assertRaises(RuntimeError):
            self.project.reset_statepoint(src_job, dst)
        with self.assertRaises(DestinationExistsError):
            self.project.reset_statepoint(src_job, dst)

    def test_update_statepoint(self):
        key = 'move_job'
        d = testdata()
        src = test_token
        extension = {'dst': True}
        dst = dict(src)
        dst.update(extension)
        extension2 = {'dst': False}
        dst2 = dict(src)
        dst2.update(extension2)
        src_job = self.open_job(src)
        src_job.document[key] = d
        self.assertIn(key, src_job.document)
        self.assertEqual(len(src_job.document), 1)
        self.project.update_statepoint(src_job, extension)
        src_job = self.open_job(src)
        dst_job = self.open_job(dst)
        self.assertEqual(dst_job.statepoint(), dst)
        self.assertIn(key, dst_job.document)
        self.assertEqual(len(dst_job.document), 1)
        self.assertNotIn(key, src_job.document)
        with self.assertRaises(RuntimeError):
            self.project.reset_statepoint(src_job, dst)
        with self.assertRaises(DestinationExistsError):
            self.project.reset_statepoint(src_job, dst)
        with self.assertRaises(KeyError):
            self.project.update_statepoint(dst_job, extension2)
        self.project.update_statepoint(dst_job, extension2, overwrite=True)
        dst2_job = self.open_job(dst2)
        self.assertEqual(dst2_job.statepoint(), dst2)
        self.assertIn(key, dst2_job.document)
        self.assertEqual(len(dst2_job.document), 1)


if __name__ == '__main__':
    unittest.main()
