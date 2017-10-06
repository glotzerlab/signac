# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import unittest
import logging
from time import sleep

import signac
from signac import sync
from signac.common import six
from signac.core.jsondict import JSONDict
from signac.syncutil import _DocProxy
from signac.sync import _FileModifyProxy
from signac.errors import FileSyncConflict
from signac.errors import DocumentSyncConflict
from signac.errors import SchemaSyncConflict
from signac.contrib.utility import _mkdir_p

from test_job import BaseJobTest


if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


if six.PY2:
    def touch(fname, times=None):
        """Utility function for updating a file time stamp.

        Source:
            https://stackoverflow.com/questions/1158076/implement-touch-using-python
        """
        fhandle = open(fname, 'a')
        try:
            os.utime(fname, times)
        finally:
            fhandle.close()
else:
    def touch(fname, mode=0o666, dir_fd=None, **kwargs):
        """Utility function for updating a file time stamp.

        Source:
            https://stackoverflow.com/questions/1158076/implement-touch-using-python
        """
        flags = os.O_CREAT | os.O_APPEND
        with os.fdopen(os.open(fname, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
            os.utime(f.fileno() if os.utime in os.supports_fd else fname,
                     dir_fd=None if os.supports_fd else dir_fd, **kwargs)


class DocProxyTest(unittest.TestCase):

    def test_basic(self):
        doc = dict(a=0)
        proxy = _DocProxy(doc)
        self.assertEqual(proxy, proxy)
        self.assertEqual(proxy, doc)
        str(proxy)
        repr(proxy)
        self.assertEqual(len(proxy), len(doc))
        self.assertEqual(proxy['a'], doc['a'])
        self.assertIn('a', proxy)
        for key in proxy:
            self.assertEqual(key, 'a')
        for key in proxy.keys():
            self.assertEqual(key, 'a')
        proxy['a'] = 1
        self.assertEqual(proxy['a'], doc['a'])
        self.assertEqual(proxy, proxy)
        self.assertEqual(proxy, doc)
        doc['a'] = 2
        proxy.update(doc)
        self.assertEqual(proxy['a'], doc['a'])
        self.assertEqual(proxy, proxy)
        self.assertEqual(proxy, doc)

    def test_dry_run(self):
        doc = dict(a=0)
        proxy = _DocProxy(doc, dry_run=True)
        self.assertEqual(proxy, proxy)
        self.assertEqual(proxy, doc)
        str(proxy)
        repr(proxy)
        self.assertEqual(len(proxy), len(doc))
        self.assertEqual(proxy['a'], doc['a'])
        self.assertIn('a', proxy)
        for key in proxy:
            self.assertEqual(key, 'a')
        for key in proxy.keys():
            self.assertEqual(key, 'a')
        proxy['a'] = 1
        self.assertEqual(proxy['a'], 0)
        self.assertEqual(proxy, proxy)
        self.assertEqual(proxy, doc)


class FileModifyProxyTest(unittest.TestCase):

    def test_copy(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix='signac_') as tmp:
            fn_src = os.path.join(tmp, 'src.txt')
            fn_dst = os.path.join(tmp, 'dst.txt')
            touch(fn_src)
            self.assertTrue(os.path.isfile(fn_src))
            self.assertFalse(os.path.isfile(fn_dst))
            proxy.copy(fn_src, fn_dst)
            self.assertTrue(os.path.isfile(fn_src))
            self.assertTrue(os.path.isfile(fn_dst))

    def test_copy_dry_run(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix='signac_') as tmp:
            fn_src = os.path.join(tmp, 'src.txt')
            fn_dst = os.path.join(tmp, 'dst.txt')
            with open(fn_src, 'w') as file:
                file.write('test')
            self.assertTrue(os.path.isfile(fn_src))
            self.assertFalse(os.path.isfile(fn_dst))
            proxy.copy(fn_src, fn_dst)
            self.assertTrue(os.path.isfile(fn_src))
            self.assertFalse(os.path.isfile(fn_dst))

    def test_copytree(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix='signac_') as tmp:
            src = os.path.join(tmp, 'src')
            dst = os.path.join(tmp, 'dst')
            _mkdir_p(src)
            fn_src = os.path.join(src, 'test.txt')
            fn_dst = os.path.join(dst, 'test.txt')
            touch(fn_src)
            self.assertTrue(os.path.isfile(fn_src))
            self.assertFalse(os.path.isfile(fn_dst))
            proxy.copytree(src, dst)
            self.assertTrue(os.path.isfile(fn_src))
            self.assertTrue(os.path.isfile(fn_dst))

    def test_copytree_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix='signac_') as tmp:
            src = os.path.join(tmp, 'src')
            dst = os.path.join(tmp, 'dst')
            _mkdir_p(src)
            fn_src = os.path.join(src, 'test.txt')
            fn_dst = os.path.join(dst, 'test.txt')
            touch(fn_src)
            self.assertTrue(os.path.isfile(fn_src))
            self.assertFalse(os.path.isfile(fn_dst))
            proxy.copytree(src, dst)
            self.assertTrue(os.path.isfile(fn_src))
            self.assertFalse(os.path.isfile(fn_dst))

    def test_remove(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix='signac_') as tmp:
            fn = os.path.join(tmp, 'test.txt')
            self.assertFalse(os.path.isfile(fn))
            touch(fn)
            self.assertTrue(os.path.isfile(fn))
            proxy.remove(fn)
            self.assertFalse(os.path.isfile(fn))

    def test_remove_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix='signac_') as tmp:
            fn = os.path.join(tmp, 'test.txt')
            self.assertFalse(os.path.isfile(fn))
            touch(fn)
            self.assertTrue(os.path.isfile(fn))
            proxy.remove(fn)
            self.assertTrue(os.path.isfile(fn))

    def test_create_backup(self):
        proxy = _FileModifyProxy()
        with TemporaryDirectory(prefix='signac_') as tmp:
            fn = os.path.join(tmp, 'test.txt')
            self.assertFalse(os.path.isfile(fn))
            with open(fn, 'w') as file:
                file.write('a')
            self.assertTrue(os.path.isfile(fn))
            with proxy.create_backup(fn) as fn_backup:
                self.assertTrue(os.path.isfile(fn_backup))
            self.assertTrue(os.path.isfile(fn))
            self.assertFalse(os.path.isfile(fn_backup))
            with self.assertRaises(RuntimeError):
                with proxy.create_backup(fn) as fn_backup:
                    self.assertTrue(os.path.isfile(fn_backup))
                    with open(fn, 'w') as file:
                        file.write('b')
                    raise RuntimeError()
            self.assertTrue(os.path.isfile(fn))
            self.assertFalse(os.path.isfile(fn_backup))
            with open(fn) as file:
                self.assertEqual(file.read(), 'a')

    def test_create_backup_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with TemporaryDirectory(prefix='signac_') as tmp:
            fn = os.path.join(tmp, 'test.txt')
            self.assertFalse(os.path.isfile(fn))
            with open(fn, 'w') as file:
                file.write('a')
            self.assertTrue(os.path.isfile(fn))
            with proxy.create_backup(fn) as fn_backup:
                self.assertFalse(os.path.isfile(fn_backup))
            self.assertTrue(os.path.isfile(fn))
            self.assertFalse(os.path.isfile(fn_backup))
            with self.assertRaises(RuntimeError):
                with proxy.create_backup(fn) as fn_backup:
                    self.assertFalse(os.path.isfile(fn_backup))
                    with open(fn, 'w') as file:
                        file.write('b')
                    raise RuntimeError()
            self.assertTrue(os.path.isfile(fn))
            self.assertFalse(os.path.isfile(fn_backup))
            with open(fn) as file:
                self.assertEqual(file.read(), 'b')


class FileModifyProxyDocBackupTest(unittest.TestCase):

    def setUp(self):
        self.doc = dict()

    def test_create_doc_dict(self):
        proxy = _FileModifyProxy()
        with proxy.create_doc_backup(self.doc) as p:
            pass
        with proxy.create_doc_backup(self.doc) as p:
            p['a'] = 0
        self.assertEqual(len(self.doc), 1)
        self.assertEqual(self.doc['a'], 0)

    def test_create_doc_dict_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with proxy.create_doc_backup(self.doc) as p:
            pass
        with proxy.create_doc_backup(self.doc) as p:
            p['a'] = 0
        self.assertEqual(len(self.doc), 0)

    def test_create_doc_dict_with_error(self):
        proxy = _FileModifyProxy()
        with self.assertRaises(RuntimeError):
            with proxy.create_doc_backup(self.doc) as p:
                p['a'] = 0
                raise RuntimeError()
        self.assertEqual(len(self.doc), 0)

    def test_create_doc_dict_with_error_dryrun(self):
        proxy = _FileModifyProxy(dry_run=True)
        with self.assertRaises(RuntimeError):
            with proxy.create_doc_backup(self.doc) as p:
                p['a'] = 0
                raise RuntimeError()
        self.assertEqual(len(self.doc), 0)


class FileModifyProxyJSONDocBackupTest(FileModifyProxyDocBackupTest):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)
        self.doc = JSONDict(
            filename=os.path.join(self._tmp_dir.name, 'doc.json'))


class JobSyncTest(BaseJobTest):

    def test_sync_no_implicit_init(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})
        self.assertNotIn(job_dst, self.project)
        self.assertNotIn(job_src, self.project)
        job_dst.sync(job_src)
        self.assertNotIn(job_dst, self.project)
        self.assertNotIn(job_src, self.project)
        job_src.init()
        self.assertIn(job_src, self.project)
        job_dst.sync(job_src)
        self.assertIn(job_dst, self.project)

    def test_file_sync(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})
        with job_src:
            with open('test', 'w') as file:
                file.write('test')
            os.makedirs('subdir')
            with open('subdir/test2', 'w') as file:
                file.write('test2')
        self.assertTrue(job_src.isfile('test'))
        try:
            logging.disable(logging.WARNING)
            job_dst.sync(job_src)
        finally:
            logging.disable(logging.NOTSET)
        self.assertIn(job_dst, self.project)
        self.assertTrue(job_dst.isfile('test'))
        self.assertFalse(job_dst.isfile('subdir/test2'))
        with open(job_dst.fn('test')) as file:
            self.assertEqual(file.read(), 'test')

    def test_file_sync_recursive(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})
        with job_src:
            with open('test', 'w') as file:
                file.write('test')
            os.makedirs('subdir')
            with open('subdir/test2', 'w') as file:
                file.write('test2')
        self.assertTrue(job_src.isfile('test'))
        job_dst.sync(job_src, recursive=True)
        self.assertIn(job_dst, self.project)
        self.assertTrue(job_dst.isfile('test'))
        self.assertTrue(job_dst.isfile('subdir/test2'))
        with open(job_dst.fn('test')) as file:
            self.assertEqual(file.read(), 'test')
        with open(job_dst.fn('subdir/test2')) as file:
            self.assertEqual(file.read(), 'test2')

    def test_file_sync_deep(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})
        with job_src:
            with open('test', 'w') as file:
                file.write('test')
            os.makedirs('subdir')
            with open('subdir/test2', 'w') as file:
                file.write('test2')
        self.assertTrue(job_src.isfile('test'))
        job_dst.sync(job_src, deep=True, recursive=True)
        self.assertIn(job_dst, self.project)
        self.assertTrue(job_dst.isfile('test'))
        self.assertTrue(job_dst.isfile('subdir/test2'))
        with open(job_dst.fn('test')) as file:
            self.assertEqual(file.read(), 'test')
        with open(job_dst.fn('subdir/test2')) as file:
            self.assertEqual(file.read(), 'test2')

    def _reset_differing_jobs(self, jobs):
        for i, job in enumerate(jobs):
            with job:
                with open('test', 'w') as file:
                    file.write('x' * i)
                _mkdir_p('subdir')
                with open('subdir/test2', 'w') as file:
                    file.write('x' * i)

        def differs(fn):
            x = set()
            for job in jobs:
                with open(job.fn(fn)) as file:
                    x.add(file.read())
            return len(x) > 1

        return differs

    def test_file_sync_with_conflict(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})
        differs = self._reset_differing_jobs((job_dst, job_src))
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        with self.assertRaises(FileSyncConflict):
            job_dst.sync(job_src, recursive=True)
        job_dst.sync(job_src, sync.FileSync.never)
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        job_dst.sync(job_src, sync.FileSync.always, exclude='test', recursive=True)
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        job_dst.sync(job_src, sync.FileSync.always, exclude=['test', 'bs'], recursive=True)
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        sleep(1)
        touch(job_src.fn('test'))
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        self.assertFalse(differs('test'))
        touch(job_src.fn('subdir/test2'))
        job_dst.sync(job_src, sync.FileSync.update, exclude='test2', recursive=True)
        self.assertFalse(differs('test'))
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        self.assertFalse(differs('subdir/test2'))

    def test_file_sync_strategies(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})

        def reset():
            return self._reset_differing_jobs((job_dst, job_src))

        differs = reset()
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        with self.assertRaises(FileSyncConflict):
            job_dst.sync(job_src, recursive=True)
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        job_dst.sync(job_src, sync.FileSync.never, recursive=True)
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        job_dst.sync(job_src, sync.FileSync.always, recursive=True)
        self.assertFalse(differs('test'))
        self.assertFalse(differs('subdir/test2'))
        reset()
        self.assertTrue(differs('test'))
        self.assertTrue(differs('subdir/test2'))
        sleep(1)
        touch(job_src.fn('test'))
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        self.assertFalse(differs('test'))
        touch(job_src.fn('subdir/test2'))
        job_dst.sync(job_src, sync.FileSync.update, recursive=True)
        self.assertFalse(differs('test'))
        self.assertFalse(differs('subdir/test2'))

    def _reset_document_sync(self):
        job_src = self.open_job({'a': 0})
        job_dst = self.open_job({'a': 1})
        job_src.document['a'] = 0
        job_src.document['nested'] = dict(a=1)
        self.assertNotEqual(job_src.document, job_dst.document)
        return job_dst, job_src

    def test_document_sync(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.sync(job_src)
        self.assertEqual(len(job_dst.document), len(job_src.document))
        self.assertEqual(job_src.document, job_dst.document)
        self.assertEqual(job_src.document['a'], job_dst.document['a'])
        self.assertEqual(job_src.document['nested']['a'], job_dst.document['nested']['a'])
        job_dst.sync(job_src)
        self.assertEqual(job_src.document, job_dst.document)

    def test_document_sync_nested(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.document['nested'] = dict(a=0)
        with self.assertRaises(DocumentSyncConflict):
            job_dst.sync(job_src)
        self.assertNotEqual(job_src.document, job_dst.document)

    def test_document_sync_explicit_overwrit(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.sync(job_src, doc_sync=sync.DocSync.update)
        self.assertEqual(job_src.document, job_dst.document)

    def test_document_sync_overwrite_specific(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey('nested.a'))
        self.assertEqual(job_src.document, job_dst.document)

    def test_document_sync_partially_differing(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.document['a'] = 0
        job_dst.sync(job_src)
        self.assertEqual(job_src.document, job_dst.document)

    def test_document_sync_differing_keys(self):
        job_dst, job_src = self._reset_document_sync()
        job_src.document['b'] = 1
        job_src.document['nested']['b'] = 1
        job_dst.sync(job_src)
        self.assertEqual(job_src.document, job_dst.document)

    def test_document_sync_no_sync(self):
        job_dst, job_src = self._reset_document_sync()
        self.assertTrue(sync.DocSync.NO_SYNC is False)
        job_dst.sync(job_src, doc_sync=False)
        self.assertNotEqual(job_src.document, job_dst.document)
        self.assertEqual(len(job_dst.document), 0)

    def test_document_sync_dst_has_extra_key(self):
        job_dst, job_src = self._reset_document_sync()
        job_dst.document['b'] = 2
        self.assertNotIn('b', job_src.document)
        self.assertIn('b', job_dst.document)
        job_dst.sync(job_src)
        self.assertNotIn('b', job_src.document)
        self.assertIn('b', job_dst.document)
        self.assertNotEqual(job_dst.document, job_src.document)
        self.assertEqual(job_dst.document['nested'], job_src.document['nested'])
        self.assertEqual(job_dst.document['a'], job_src.document['a'])

    def test_document_sync_with_error(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})
        job_dst.document['a'] = 0
        job_src.document['a'] = 1

        def raise_error(src, dst):
            raise RuntimeError()

        with self.assertRaises(RuntimeError):
            job_dst.sync(job_src, doc_sync=raise_error)

    def test_document_sync_with_conflict(self):
        job_dst = self.open_job({'a': 0})
        job_src = self.open_job({'a': 1})

        def reset():
            job_src.document['a'] = 0
            job_src.document['nested'] = dict(a=1)
            job_dst.document['a'] = 1
            job_dst.document['nested'] = dict(a=2)

        reset()
        self.assertNotEqual(job_dst.document, job_src.document)
        with self.assertRaises(DocumentSyncConflict):
            job_dst.sync(job_src)
        self.assertNotEqual(job_dst.document, job_src.document)
        job_dst.sync(job_src, doc_sync=sync.DocSync.NO_SYNC)
        self.assertNotEqual(job_dst.document, job_src.document)
        self.assertNotEqual(job_dst.document['a'], job_src.document['a'])
        self.assertNotEqual(job_dst.document['nested'], job_src.document['nested'])
        reset()     # only sync a
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey('a'))
        self.assertNotEqual(job_dst.document, job_src.document)
        self.assertNotEqual(job_dst.document['nested'], job_src.document['nested'])
        self.assertEqual(job_dst.document['a'], job_src.document['a'])
        reset()     # only sync nested
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey('nested'))
        self.assertNotEqual(job_dst.document, job_src.document)
        self.assertNotEqual(job_dst.document['a'], job_src.document['a'])
        self.assertEqual(job_dst.document['nested'], job_src.document['nested'])
        reset()
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey('(nested\.)?a'))
        self.assertEqual(job_dst.document, job_src.document)
        self.assertEqual(job_dst.document['nested'], job_src.document['nested'])
        self.assertEqual(job_dst.document['a'], job_src.document['a'])
        reset()
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey(lambda key: key.startswith('a')))
        self.assertNotEqual(job_dst.document, job_src.document)
        self.assertNotEqual(job_dst.document['nested'], job_src.document['nested'])
        self.assertEqual(job_dst.document['a'], job_src.document['a'])
        reset()
        job_dst.sync(job_src, doc_sync=sync.DocSync.ByKey(lambda key: key.startswith('nested')))
        self.assertNotEqual(job_dst.document, job_src.document)
        self.assertNotEqual(job_dst.document['a'], job_src.document['a'])
        self.assertEqual(job_dst.document['nested'], job_src.document['nested'])
        reset()
        job_dst.sync(job_src, doc_sync=sync.DocSync.update)
        self.assertEqual(job_dst.document, job_src.document)
        self.assertEqual(job_dst.document['nested'], job_src.document['nested'])
        self.assertEqual(job_dst.document['a'], job_src.document['a'])


class ProjectSyncTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)
        self._tmp_pr_a = os.path.join(self._tmp_dir.name, 'pr_a')
        self._tmp_pr_b = os.path.join(self._tmp_dir.name, 'pr_b')
        os.mkdir(self._tmp_pr_a)
        os.mkdir(self._tmp_pr_b)
        self.project_a = signac.Project.init_project(
            name='test-project-a', root=self._tmp_pr_a)
        self.project_b = signac.Project.init_project(
            name='test-project-b', root=self._tmp_pr_b)

    def _init_job(self, job, data='data'):
        with job:
            with open('test.txt', 'w') as file:
                file.write(str(data))

    def test_src_and_dst_identical(self):
        with self.assertRaises(ValueError):
            self.project_a.sync(self.project_a)

    def test_src_and_dst_empty(self):
        self.project_a.sync(self.project_b)
        self.assertEqual(len(self.project_a), len(self.project_b))

    def test_src_empty(self):
        for i in range(4):
            self._init_job(self.project_b.open_job({'a': i}))
        self.project_a.sync(self.project_b)
        self.assertEqual(len(self.project_a), len(self.project_b))

    def test_dst_empty(self):
        for i in range(4):
            self._init_job(self.project_a.open_job({'a': i}))
        self.project_a.sync(self.project_b)
        self.assertEqual(len(self.project_a), 4)
        self.assertEqual(len(self.project_b), 0)

    def test_doc_sync(self):
        self.project_a.document['a'] = 0
        self.assertIn('a', self.project_a.document)
        self.assertNotIn('a', self.project_b.document)
        self.project_a.sync(self.project_b)
        self.assertIn('a', self.project_a.document)
        self.assertNotIn('a', self.project_b.document)
        self.project_b.document['b'] = 1
        self.project_a.sync(self.project_b)
        self.assertIn('b', self.project_a.document)
        self.project_a.document['b'] = 2
        with self.assertRaises(DocumentSyncConflict):
            self.project_a.sync(self.project_b)
        self.project_a.sync(self.project_b, doc_sync=sync.DocSync.ByKey('b'))

    def _setup_mixed(self):
        for i in range(4):
            if i % 2 == 0:
                self._init_job(self.project_a.open_job({'a': i}))
            if i % 3 == 0:
                self._init_job(self.project_b.open_job({'a': i}))

    def test_mixed(self):
        self._setup_mixed()
        with self.assertRaises(SchemaSyncConflict):
            self.project_a.sync(self.project_b)
        self.assertEqual(len(self.project_a), 2)
        self.assertEqual(len(self.project_b), 2)
        self.project_a.sync(self.project_b, check_schema=False)
        self.assertEqual(len(self.project_a), 3)

    def _setup_jobs(self):
        for i in range(4):
            self._init_job(self.project_a.open_job({'a': i}))
            self._init_job(self.project_b.open_job({'a': i}))

    def test_with_conflict(self):
        self._setup_jobs()
        self.assertEqual(len(self.project_a), len(self.project_b))
        job_a0 = self.project_a.open_job({'a': 0})
        with open(job_a0.fn('test.txt'), 'w') as file:
            file.write('newdata')
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b)

    def test_with_conflict_never(self):
        self._setup_jobs()
        job_a0 = self.project_a.open_job({'a': 0})
        with open(job_a0.fn('text.txt'), 'w') as file:
            file.write('otherdata')
        self.project_a.sync(self.project_b, sync.FileSync.never)
        with open(job_a0.fn('text.txt')) as file:
            self.assertEqual(file.read(), 'otherdata')

    def test_selection(self):
        self._setup_jobs()
        self.assertEqual(len(self.project_a), len(self.project_b))
        job_a0 = self.project_a.open_job({'a': 0})
        with open(job_a0.fn('test.txt'), 'w') as file:
            file.write('newdata')
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b)
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_a)
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_b)
        self.assertEqual(len(self.project_a.find_jobs({'a': 0})), 1)
        self.assertEqual(len(self.project_b.find_jobs({'a': 0})), 1)
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_a.find_jobs({'a': 0}))
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_b.find_jobs({'a': 0}))
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_a.find_job_ids({'a': 0}))
        with self.assertRaises(FileSyncConflict):
            self.project_a.sync(self.project_b, selection=self.project_b.find_job_ids({'a': 0}))
        f = {'a': {'$ne': 0}}
        self.project_a.sync(self.project_b, selection=self.project_a.find_jobs(f))
        self.project_a.sync(self.project_b, selection=self.project_b.find_jobs(f))
        self.project_a.sync(self.project_b, selection=self.project_a.find_job_ids(f))
        self.project_a.sync(self.project_b, selection=self.project_b.find_job_ids(f))


if __name__ == '__main__':
    unittest.main()
