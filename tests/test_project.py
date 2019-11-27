# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import uuid
import warnings
import logging
import itertools
import json
import pickle
from tarfile import TarFile
from zipfile import ZipFile
from tempfile import TemporaryDirectory

import signac
from signac.errors import DestinationExistsError
from signac.contrib.linked_view import _find_all_links
from signac.contrib.schema import ProjectSchema
from signac.contrib.errors import JobsCorruptedError
from signac.contrib.errors import WorkspaceError
from signac.contrib.errors import StatepointParsingError
from signac.contrib.project import JobsCursor, Project  # noqa: F401

from test_job import BaseJobTest


try:
    import pandas  # noqa
except ImportError:
    PANDAS = False
else:
    PANDAS = True

try:
    import h5py    # noqa
    H5PY = True
except ImportError:
    H5PY = False


# Make sure the jobs created for this test are unique.
test_token = {'test_token': str(uuid.uuid4())}

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')


class BaseProjectTest(BaseJobTest):
    pass


class ProjectTest(BaseProjectTest):

    def test_get(self):
        pass

    def test_get_id(self):
        self.assertEqual(self.project.get_id(), 'testing_test_project')
        self.assertEqual(str(self.project), self.project.get_id())

    def test_repr(self):
        repr(self.project)
        p = eval(repr(self.project))
        self.assertEqual(repr(p), repr(self.project))
        self.assertEqual(p, self.project)

    def test_str(self):
        str(self.project) == self.project.get_id()

    def test_root_directory(self):
        self.assertEqual(self._tmp_pr, self.project.root_directory())

    def test_workspace_directory(self):
        self.assertEqual(self._tmp_wd, self.project.workspace())

    def test_config_modification(self):
        with warnings.catch_warnings(record=True) as w:
            self.project.config['foo'] = 'bar'
            self.assertEqual(len(w), 1)
            self.assertEqual(w[0].category, DeprecationWarning)

    def test_workspace_directory_with_env_variable(self):
        os.environ['SIGNAC_ENV_DIR_TEST'] = self._tmp_wd
        self.project.config['workspace_dir'] = '${SIGNAC_ENV_DIR_TEST}'
        self.assertEqual(self._tmp_wd, self.project.workspace())

    def test_fn(self):
        self.assertEqual(
            self.project.fn('test/abc'),
            os.path.join(self.project.root_directory(), 'test/abc'))

    def test_isfile(self):
        self.assertFalse(self.project.isfile('test'))
        with open(self.project.fn('test'), 'w'):
            pass
        self.assertTrue(self.project.isfile('test'))

    def test_document(self):
        self.assertFalse(self.project.document)
        self.assertEqual(len(self.project.document), 0)
        self.project.document['a'] = 42
        self.assertEqual(len(self.project.document), 1)
        self.assertTrue(self.project.document)
        prj2 = type(self.project).get_project(root=self.project.root_directory())
        self.assertTrue(prj2.document)
        self.assertEqual(len(prj2.document), 1)
        self.project.document.clear()
        self.assertFalse(self.project.document)
        self.assertEqual(len(self.project.document), 0)
        self.assertFalse(prj2.document)
        self.assertEqual(len(prj2.document), 0)
        self.project.document.a = {'b': 43}
        self.assertEqual(self.project.document, {'a': {'b': 43}})
        self.project.document.a.b = 44
        self.assertEqual(self.project.document, {'a': {'b': 44}})
        self.project.document = {'a': {'b': 45}}
        self.assertEqual(self.project.document, {'a': {'b': 45}})

    def test_doc(self):
        self.assertFalse(self.project.doc)
        self.assertEqual(len(self.project.doc), 0)
        self.project.doc['a'] = 42
        self.assertEqual(len(self.project.doc), 1)
        self.assertTrue(self.project.doc)
        prj2 = type(self.project).get_project(root=self.project.root_directory())
        self.assertTrue(prj2.doc)
        self.assertEqual(len(prj2.doc), 1)
        self.project.doc.clear()
        self.assertFalse(self.project.doc)
        self.assertEqual(len(self.project.doc), 0)
        self.assertFalse(prj2.doc)
        self.assertEqual(len(prj2.doc), 0)
        self.project.doc.a = {'b': 43}
        self.assertEqual(self.project.doc, {'a': {'b': 43}})
        self.project.doc.a.b = 44
        self.assertEqual(self.project.doc, {'a': {'b': 44}})
        self.project.doc = {'a': {'b': 45}}
        self.assertEqual(self.project.doc, {'a': {'b': 45}})

    @unittest.skipIf(not H5PY, 'test requires the h5py package')
    def test_data(self):
        with self.project.data:
            self.assertFalse(self.project.data)
            self.assertEqual(len(self.project.data), 0)
            self.project.data['a'] = 42
            self.assertEqual(len(self.project.data), 1)
            self.assertTrue(self.project.data)
        prj2 = type(self.project).get_project(root=self.project.root_directory())
        with prj2.data:
            self.assertTrue(prj2.data)
            self.assertEqual(len(prj2.data), 1)
        with self.project.data:
            self.project.data.clear()
            self.assertFalse(self.project.data)
            self.assertEqual(len(self.project.data), 0)
        with prj2.data:
            self.assertFalse(prj2.data)
            self.assertEqual(len(prj2.data), 0)
        with self.project.data:
            self.project.data.a = {'b': 43}
            self.assertEqual(self.project.data, {'a': {'b': 43}})
            self.project.data.a.b = 44
            self.assertEqual(self.project.data, {'a': {'b': 44}})
            self.project.data = {'a': {'b': 45}}
            self.assertEqual(self.project.data, {'a': {'b': 45}})

    def test_write_read_statepoint(self):
        statepoints = [{'a': i} for i in range(5)]
        self.project.dump_statepoints(statepoints)
        self.project.write_statepoints(statepoints)
        read = list(self.project.read_statepoints().values())
        self.assertEqual(len(read), len(statepoints))
        more_statepoints = [{'b': i} for i in range(5, 10)]
        self.project.write_statepoints(more_statepoints)
        read2 = list(self.project.read_statepoints())
        self.assertEqual(len(read2), len(statepoints) + len(more_statepoints))
        for id_ in self.project.read_statepoints().keys():
            self.project.get_statepoint(id_)

    def test_workspace_path_normalization(self):
        def norm_path(p):
            return os.path.abspath(os.path.expandvars(p))

        self.assertEqual(self.project.workspace(), norm_path(self._tmp_wd))

        abs_path = '/path/to/workspace'
        self.project.config['workspace_dir'] = abs_path
        self.assertEqual(self.project.workspace(), norm_path(abs_path))

        rel_path = 'path/to/workspace'
        self.project.config['workspace_dir'] = rel_path
        self.assertEqual(
            self.project.workspace(),
            norm_path(os.path.join(self.project.root_directory(), self.project.workspace())))

    def test_no_workspace_warn_on_find(self):
        self.assertFalse(os.path.exists(self.project.workspace()))
        with self.assertLogs(level='INFO') as cm:
            list(self.project.find_jobs())
            self.assertEqual(len(cm.output), 2)

    def test_workspace_broken_link_error_on_find(self):
        wd = self.project.workspace()
        os.symlink(wd + '~', self.project.fn('workspace-link'))
        self.project.config['workspace_dir'] = 'workspace-link'
        with self.assertRaises(WorkspaceError):
            list(self.project.find_jobs())

    def test_workspace_read_only_path(self):
        # Create file where workspace would be, thus preventing the creation
        # of the workspace directory.
        with open(os.path.join(self.project.workspace()), 'w'):
            pass

        with self.assertRaises(OSError):     # Ensure that the file is in place.
            os.mkdir(self.project.workspace())

        self.assertTrue(issubclass(WorkspaceError, OSError))

        try:
            logging.disable(logging.ERROR)
            with self.assertRaises(WorkspaceError):
                list(self.project.find_jobs())
        finally:
            logging.disable(logging.NOTSET)

        self.assertFalse(os.path.isdir(self._tmp_wd))
        self.assertFalse(os.path.isdir(self.project.workspace()))

    def test_find_job_ids(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['b'] = sp['a']
        self.assertEqual(len(statepoints), len(list(self.project.find_job_ids())))
        self.assertEqual(1, len(list(self.project.find_job_ids({'a': 0}))))
        self.assertEqual(0, len(list(self.project.find_job_ids({'a': 5}))))
        self.assertEqual(1, len(list(self.project.find_job_ids(doc_filter={'b': 0}))))
        self.assertEqual(0, len(list(self.project.find_job_ids(doc_filter={'b': 5}))))
        for job_id in self.project.find_job_ids():
            self.assertEqual(self.project.open_job(id=job_id).get_id(), job_id)
        index = list(self.project.index())
        for job_id in self.project.find_job_ids(index=index):
            self.assertEqual(self.project.open_job(id=job_id).get_id(), job_id)

    def test_find_jobs(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        self.assertEqual(len(self.project), len(self.project.find_jobs()))
        self.assertEqual(len(self.project), len(self.project.find_jobs({})))
        self.assertEqual(1, len(list(self.project.find_jobs({'a': 0}))))
        self.assertEqual(0, len(list(self.project.find_jobs({'a': 5}))))

    def test_find_jobs_next(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        jobs = self.project.find_jobs()
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=DeprecationWarning, module='signac')
            for i in range(2):  # run this twice
                jobs_ = set()
                for i in range(len(self.project)):
                    job = jobs.next()
                    self.assertIn(job, self.project)
                    jobs_.add(job)
                with self.assertRaises(StopIteration):
                    job = jobs.next()
                self.assertEqual(jobs_, set(self.project))

    def test_find_jobs_arithmetic_operators(self):
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        self.assertEqual(len(self.project), 10)
        self.assertEqual(len(self.project.find_jobs({'a': {'$lt': 5}})), 5)
        self.assertEqual(len(self.project.find_jobs({'a.$lt': 5})), 5)

    def test_find_jobs_logical_operators(self):
        for i in range(10):
            self.project.open_job({'a': i, 'b': {'c': i}}).init()
        self.assertEqual(len(self.project), 10)
        with self.assertRaises(ValueError):
            list(self.project.find_jobs({'$and': {'foo': 'bar'}}))
        self.assertEqual(len(self.project.find_jobs({'$and': [{}, {'a': 0}]})), 1)
        self.assertEqual(len(self.project.find_jobs({'$or': [{}, {'a': 0}]})), len(self.project))
        q = {'$and': [{'a': 0}, {'a': 1}]}
        self.assertEqual(len(self.project.find_jobs(q)), 0)
        q = {'$or': [{'a': 0}, {'a': 1}]}
        self.assertEqual(len(self.project.find_jobs(q)), 2)
        q = {'$and': [{'$and': [{'a': 0}, {'a': 1}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 0)
        q = {'$and': [{'$or': [{'a': 0}, {'a': 1}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 2)
        q = {'$or': [{'$or': [{'a': 0}, {'a': 1}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 2)
        q = {'$or': [{'$and': [{'a': 0}, {'a': 1}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 0)
        self.assertEqual(len(self.project.find_jobs({'$and': [{}, {'b': {'c': 0}}]})), 1)
        self.assertEqual(len(self.project.find_jobs(
            {'$or': [{}, {'b': {'c': 0}}]})), len(self.project))
        q = {'$and': [{'b': {'c': 0}}, {'b': {'c': 1}}]}
        self.assertEqual(len(self.project.find_jobs(q)), 0)
        q = {'$or': [{'b': {'c': 0}}, {'b': {'c': 1}}]}
        self.assertEqual(len(self.project.find_jobs(q)), 2)
        q = {'$and': [{'$and': [{'b': {'c': 0}}, {'b': {'c': 1}}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 0)
        q = {'$and': [{'$or': [{'b': {'c': 0}}, {'b': {'c': 1}}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 2)
        q = {'$or': [{'$or': [{'b': {'c': 0}}, {'b': {'c': 1}}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 2)
        q = {'$or': [{'$and': [{'b': {'c': 0}}, {'b': {'c': 1}}]}]}
        self.assertEqual(len(self.project.find_jobs(q)), 0)

    def test_num_jobs(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        self.assertEqual(len(statepoints), self.project.num_jobs())
        self.assertEqual(len(statepoints), len(self.project))
        self.assertEqual(len(statepoints), len(self.project.find_jobs()))

    def test_len_find_jobs(self):
        statepoints = [{'a': i, 'b': i < 3} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        self.assertEqual(len(self.project), len(self.project.find_jobs()))
        self.assertEqual(3, len(self.project.find_jobs({'b': True})))

    def test_iteration(self):
        statepoints = [{'a': i, 'b': i < 3} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        for i, job in enumerate(self.project):
            pass
        self.assertEqual(i, len(self.project) - 1)

    def test_open_job_by_id(self):
        statepoints = [{'a': i} for i in range(5)]
        jobs = [self.project.open_job(sp) for sp in statepoints]
        self.project._sp_cache.clear()
        try:
            logging.disable(logging.WARNING)
            for job in jobs:
                with self.assertRaises(KeyError):
                    self.project.open_job(id=str(job))
            for job in jobs:
                job.init()
            for job in jobs:
                self.project.open_job(id=str(job))
            with self.assertRaises(KeyError):
                self.project.open_job(id='abc')
            with self.assertRaises(ValueError):
                self.project.open_job()
            with self.assertRaises(ValueError):
                self.project.open_job(statepoints[0], id=str(jobs[0]))
        finally:
            logging.disable(logging.NOTSET)

    def test_open_job_by_abbreviated_id(self):
        statepoints = [{'a': i} for i in range(5)]
        [self.project.open_job(sp).init() for sp in statepoints]
        aid_len = self.project.min_len_unique_id()
        for job in self.project.find_jobs():
            aid = job.get_id()[:aid_len]
            self.assertEqual(self.project.open_job(id=aid), job)
        with self.assertRaises(LookupError):
            for job in self.project.find_jobs():
                self.project.open_job(id=job.get_id()[:aid_len - 1])
        with self.assertRaises(KeyError):
            self.project.open_job(id='abc')

    def test_missing_statepoint_file(self):
        job = self.project.open_job(dict(a=0))
        job.init()

        os.remove(job.fn(job.FN_MANIFEST))

        self.project._sp_cache.clear()
        self.project._remove_persistent_cache_file()
        try:
            logging.disable(logging.CRITICAL)
            with self.assertRaises(JobsCorruptedError):
                self.project.open_job(id=job.get_id()).init()
        finally:
            logging.disable(logging.NOTSET)

    def test_corrupted_statepoint_file(self):
        job = self.project.open_job(dict(a=0))
        job.init()

        # overwrite state point manifest file
        with open(job.fn(job.FN_MANIFEST), 'w'):
            pass

        self.project._sp_cache.clear()
        self.project._remove_persistent_cache_file()
        try:
            logging.disable(logging.CRITICAL)
            with self.assertRaises(JobsCorruptedError):
                self.project.open_job(id=job.get_id())
        finally:
            logging.disable(logging.NOTSET)

    def test_rename_workspace(self):
        job = self.project.open_job(dict(a=0))
        job.init()
        # First, we move the job to the wrong directory.
        wd = job.workspace()
        wd_invalid = os.path.join(self.project.workspace(), '0' * 32)
        os.rename(wd, wd_invalid)  # Move to incorrect id.
        self.assertFalse(os.path.exists(job.workspace()))

        try:
            logging.disable(logging.CRITICAL)

            # This should raise an error when calling check().
            with self.assertRaises(JobsCorruptedError):
                self.project.check()

            # The repair attempt should be successful.
            self.project.repair()
            self.project.check()

            # We corrupt it again, but this time ...
            os.rename(wd, wd_invalid)
            with self.assertRaises(JobsCorruptedError):
                self.project.check()
            #  ... we reinitalize the initial job, ...
            job.init()
            with self.assertRaises(JobsCorruptedError):
                # ... which means the repair attempt must fail.
                self.project.repair()
            with self.assertRaises(JobsCorruptedError):
                self.project.check()
            # Some manual clean-up should get things back on track.
            job.remove()
            with self.assertRaises(JobsCorruptedError):
                self.project.check()
            self.project.repair()
            self.project.check()
        finally:
            logging.disable(logging.NOTSET)

    def test_repair_corrupted_workspace(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()

        for i, job in enumerate(self.project):
            pass
        self.assertEqual(i, 4)

        # no manifest file
        with self.project.open_job(statepoints[0]) as job:
            os.remove(job.FN_MANIFEST)
        # blank manifest file
        with self.project.open_job(statepoints[1]) as job:
            with open(job.FN_MANIFEST, 'w'):
                pass

        # Need to clear internal and persistent cache to encounter error.
        self.project._sp_cache.clear()
        self.project._remove_persistent_cache_file()

        # Ensure that state point hash table does not exist.
        self.assertFalse(os.path.isfile(self.project.fn(self.project.FN_STATEPOINTS)))

        # disable logging temporarily
        try:
            logging.disable(logging.CRITICAL)

            # Iterating through the jobs should now result in an error.
            with self.assertRaises(JobsCorruptedError):
                for job in self.project:
                    pass

            with self.assertRaises(JobsCorruptedError):
                self.project.repair()

            self.project.write_statepoints(statepoints)
            self.project.repair()

            os.remove(self.project.fn(self.project.FN_STATEPOINTS))
            self.project._sp_cache.clear()
            for job in self.project:
                pass
        finally:
            logging.disable(logging.NOTSET)

    def test_index(self):
        docs = list(self.project.index(include_job_document=True))
        self.assertEqual(len(docs), 0)
        docs = list(self.project.index(include_job_document=False))
        self.assertEqual(len(docs), 0)
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        job_ids = set((job.get_id() for job in self.project.find_jobs()))
        docs = list(self.project.index())
        job_ids_cmp = set((doc['_id'] for doc in docs))
        self.assertEqual(job_ids, job_ids_cmp)
        self.assertEqual(len(docs), len(statepoints))
        for sp in statepoints:
            with self.project.open_job(sp):
                with open('test.txt', 'w'):
                    pass
        docs = list(self.project.index({'.*/test.txt': 'TextFile'}))
        self.assertEqual(len(docs), 2 * len(statepoints))
        self.assertEqual(len(set((doc['_id'] for doc in docs))), len(docs))

    def test_signac_project_crawler(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        job_ids = set((job.get_id() for job in self.project.find_jobs()))
        index = dict()
        for doc in self.project.index():
            index[doc['_id']] = doc
        self.assertEqual(len(index), len(job_ids))
        self.assertEqual(set(index.keys()), set(job_ids))
        crawler = signac.contrib.SignacProjectCrawler(self.project.root_directory())
        index2 = dict()
        for doc in crawler.crawl():
            index2[doc['_id']] = doc
        for _id, _id2 in zip(index, index2):
            self.assertEqual(_id, _id2)
            self.assertEqual(index[_id], index2[_id])
        self.assertEqual(index, index2)
        for job in self.project.find_jobs():
            with open(job.fn('test.txt'), 'w') as file:
                file.write('test\n')
        formats = {r'.*/test\.txt': 'TextFile'}
        index = dict()
        for doc in self.project.index(formats):
            index[doc['_id']] = doc
        self.assertEqual(len(index), 2 * len(job_ids))

        class Crawler(signac.contrib.SignacProjectCrawler):
            called = False

            def process(self_, doc, dirpath, fn):
                Crawler.called = True
                doc = super(Crawler, self_).process(doc=doc, dirpath=dirpath, fn=fn)
                if 'format' in doc and doc['format'] is None:
                    self.assertEqual(doc['_id'], doc['signac_id'])
                return doc
        for p, fmt in formats.items():
            Crawler.define(p, fmt)
        index2 = dict()
        for doc in Crawler(root=self.project.root_directory()).crawl():
            index2[doc['_id']] = doc
        self.assertEqual(index, index2)
        self.assertTrue(Crawler.called)

    def test_custom_project(self):

        class CustomProject(signac.Project):
            pass

        project = CustomProject.get_project(root=self.project.root_directory())
        self.assertTrue(isinstance(project, signac.Project))
        self.assertTrue(isinstance(project, CustomProject))

    def test_custom_job_class(self):

        class CustomJob(signac.contrib.job.Job):
            def __init__(self, *args, **kwargs):
                super(CustomJob, self).__init__(*args, **kwargs)

        class CustomProject(signac.Project):
            Job = CustomJob

        project = CustomProject.get_project(root=self.project.root_directory())
        self.assertTrue(isinstance(project, signac.Project))
        self.assertTrue(isinstance(project, CustomProject))
        job = project.open_job(dict(a=0))
        self.assertTrue(isinstance(job, CustomJob))
        self.assertTrue(isinstance(job, signac.contrib.job.Job))

    def test_project_contains(self):
        job = self.open_job(dict(a=0))
        self.assertNotIn(job, self.project)
        job.init()
        self.assertIn(job, self.project)

    def test_job_move(self):
        root = self._tmp_dir.name
        project_a = signac.init_project('ProjectA', os.path.join(root, 'a'))
        project_b = signac.init_project('ProjectB', os.path.join(root, 'b'))
        job = project_a.open_job(dict(a=0))
        job_b = project_b.open_job(dict(a=0))
        self.assertNotEqual(job, job_b)
        self.assertNotEqual(hash(job), hash(job_b))
        self.assertNotIn(job, project_a)
        self.assertNotIn(job, project_b)
        job.init()
        self.assertIn(job, project_a)
        self.assertNotIn(job, project_b)
        job.move(project_b)
        self.assertIn(job, project_b)
        self.assertNotIn(job, project_a)
        self.assertEqual(job, job_b)
        self.assertEqual(hash(job), hash(job_b))
        with job:
            job.document['a'] = 0
            with open('hello.txt', 'w') as file:
                file.write('world!')
        job_ = project_b.open_job(job.statepoint())
        self.assertEqual(job, job_)
        self.assertEqual(hash(job), hash(job_))
        self.assertEqual(job_, job_b)
        self.assertEqual(hash(job_), hash(job_b))
        self.assertTrue(job_.isfile('hello.txt'))
        self.assertEqual(job_.document['a'], 0)

    def test_job_clone(self):
        root = self._tmp_dir.name
        project_a = signac.init_project('ProjectA', os.path.join(root, 'a'))
        project_b = signac.init_project('ProjectB', os.path.join(root, 'b'))
        job_a = project_a.open_job(dict(a=0))
        self.assertNotIn(job_a, project_a)
        self.assertNotIn(job_a, project_b)
        with job_a:
            job_a.document['a'] = 0
            with open('hello.txt', 'w') as file:
                file.write('world!')
        self.assertIn(job_a, project_a)
        self.assertNotIn(job_a, project_b)
        job_b = project_b.clone(job_a)
        self.assertIn(job_a, project_a)
        self.assertIn(job_a, project_b)
        self.assertIn(job_b, project_a)
        self.assertIn(job_b, project_b)
        self.assertEqual(job_a.document, job_b.document)
        self.assertTrue(job_a.isfile('hello.txt'))
        self.assertTrue(job_b.isfile('hello.txt'))
        with self.assertRaises(DestinationExistsError):
            project_b.clone(job_a)
        try:
            project_b.clone(job_a)
        except DestinationExistsError as error:
            self.assertNotEqual(error.destination, job_a)
            self.assertEqual(error.destination, job_b)

    def test_schema_init(self):
        s = ProjectSchema()
        self.assertEqual(len(s), 0)
        self.assertFalse(s)

    def test_schema(self):
        for i in range(10):
            self.project.open_job({
                'const': 0,
                'const2': {'const3': 0},
                'a': i,
                'b': {'b2': i},
                'c': [i if i % 2 else None, 0, 0],
                'd': [[i, 0, 0]],
                'e': {'e2': [i, 0, 0]} if i % 2 else 0,  # heterogeneous!
                'f': {'f2': [[i, 0, 0]]},
            }).init()

        s = self.project.detect_schema()
        self.assertEqual(len(s), 9)
        for k in 'const', 'const2.const3', 'a', 'b.b2', 'c', 'd', 'e.e2', 'f.f2':
            self.assertIn(k, s)
            self.assertIn(k.split('.'), s)
            # The following calls should not error out.
            s[k]
            s[k.split('.')]
        repr(s)
        self.assertEqual(s.format(), str(s))
        s = self.project.detect_schema(exclude_const=True)
        self.assertEqual(len(s), 7)
        self.assertNotIn('const', s)
        self.assertNotIn(('const2', 'const3'), s)
        self.assertNotIn('const2.const3', s)
        self.assertNotIn(type, s['e'])

    def test_schema_subset(self):
        for i in range(5):
            self.project.open_job(dict(a=i)).init()
        s_sub = self.project.detect_schema()
        for i in range(10):
            self.project.open_job(dict(a=i)).init()

        self.assertNotEqual(s_sub, self.project.detect_schema())
        s = self.project.detect_schema(subset=self.project.find_jobs({'a.$lt': 5}))
        self.assertEqual(s, s_sub)
        s = self.project.detect_schema(subset=self.project.find_job_ids({'a.$lt': 5}))
        self.assertEqual(s, s_sub)

    def test_schema_eval(self):
        for i in range(10):
            for j in range(10):
                self.project.open_job(dict(a=i, b=j)).init()
        s = self.project.detect_schema()
        self.assertEqual(s, s(self.project))
        self.assertEqual(s, s([job.sp for job in self.project]))
        # Check that it works with iterables that can only be consumed once
        self.assertEqual(s, s((job.sp for job in self.project)))

    def test_schema_difference(self):
        def get_sp(i):
            return {
                'const': 0,
                'const2': {'const3': 0},
                'a': i,
                'b': {'b2': i},
                'c': [i, 0, 0],
                'd': [[i, 0, 0]],
                'e': {'e2': [i, 0, 0]},
                'f': {'f2': [[i, 0, 0]]},
            }

        for i in range(10):
            self.project.open_job(get_sp(i)).init()

        s = self.project.detect_schema()
        s2 = self.project.detect_schema()
        s3 = self.project.detect_schema(exclude_const=True)
        s4 = self.project.detect_schema(exclude_const=True)

        self.assertEqual(len(s), 8)
        self.assertEqual(len(s2), 8)
        self.assertEqual(len(s3), 6)
        self.assertEqual(len(s4), 6)

        self.assertEqual(s, s2)
        self.assertNotEqual(s, s3)
        self.assertNotEqual(s, s4)
        self.assertEqual(s3, s4)

        self.assertEqual(len(s.difference(s3)), len(s) - len(s3))
        self.project.open_job(get_sp(11)).init()
        s_ = self.project.detect_schema()
        s3_ = self.project.detect_schema(exclude_const=True)

        self.assertNotEqual(s, s_)
        self.assertNotEqual(s3, s3_)
        self.assertEqual(s.difference(s_), s3.difference(s3_))
        self.assertEqual(len(s.difference(s_, ignore_values=True)), 0)
        self.assertEqual(len(s3.difference(s3_, ignore_values=True)), 0)

    def test_jobs_groupby(self):
        def get_sp(i):
            return {
                'a': i,
                'b': i % 2,
                'c': i % 3
            }

        for i in range(12):
            self.project.open_job(get_sp(i)).init()

        for k, g in self.project.groupby('a'):
            self.assertEqual(len(list(g)), 1)
            for job in list(g):
                self.assertEqual(job.sp['a'], k)
        for k, g in self.project.groupby('b'):
            self.assertEqual(len(list(g)), 6)
            for job in list(g):
                self.assertEqual(job.sp['b'], k)
        with self.assertRaises(KeyError):
            for k, g in self.project.groupby('d'):
                pass
        for k, g in self.project.groupby('d', default=-1):
            self.assertEqual(k, -1)
            self.assertEqual(len(list(g)), len(self.project))
        for k, g in self.project.groupby(('b', 'c')):
            self.assertEqual(len(list(g)), 2)
            for job in list(g):
                self.assertEqual(job.sp['b'], k[0])
                self.assertEqual(job.sp['c'], k[1])
        for k, g in self.project.groupby(lambda job: job.sp['a'] % 4):
            self.assertEqual(len(list(g)), 3)
            for job in list(g):
                self.assertEqual(job.sp['a'] % 4, k)
        for k, g in self.project.groupby(lambda job: str(job)):
            self.assertEqual(len(list(g)), 1)
            for job in list(g):
                self.assertEqual(str(job), k)
        group_count = 0
        for k, g in self.project.groupby():
            self.assertEqual(len(list(g)), 1)
            group_count = group_count + 1
            for job in list(g):
                self.assertEqual(str(job), k)
        self.assertEqual(group_count, len(list(self.project.find_jobs())))

    def test_jobs_groupbydoc(self):
        def get_doc(i):
            return {
                'a': i,
                'b': i % 2,
                'c': i % 3
            }

        for i in range(12):
            job = self.project.open_job({'i': i})
            job.init()
            job.document = get_doc(i)

        for k, g in self.project.groupbydoc('a'):
            self.assertEqual(len(list(g)), 1)
            for job in list(g):
                self.assertEqual(job.document['a'], k)
        for k, g in self.project.groupbydoc('b'):
            self.assertEqual(len(list(g)), 6)
            for job in list(g):
                self.assertEqual(job.document['b'], k)
        with self.assertRaises(KeyError):
            for k, g in self.project.groupbydoc('d'):
                pass
        for k, g in self.project.groupbydoc('d', default=-1):
            self.assertEqual(k, -1)
            self.assertEqual(len(list(g)), len(self.project))
        for k, g in self.project.groupbydoc(('b', 'c')):
            self.assertEqual(len(list(g)), 2)
            for job in list(g):
                self.assertEqual(job.document['b'], k[0])
                self.assertEqual(job.document['c'], k[1])
        for k, g in self.project.groupbydoc(lambda doc: doc['a'] % 4):
            self.assertEqual(len(list(g)), 3)
            for job in list(g):
                self.assertEqual(job.document['a'] % 4, k)
        for k, g in self.project.groupbydoc(lambda doc: str(doc)):
            self.assertEqual(len(list(g)), 1)
            for job in list(g):
                self.assertEqual(str(job.document), k)
        group_count = 0
        for k, g in self.project.groupbydoc():
            self.assertEqual(len(list(g)), 1)
            group_count = group_count + 1
            for job in list(g):
                self.assertEqual(str(job), k)
        self.assertEqual(group_count, len(list(self.project.find_jobs())))

    def test_temp_project(self):
        with self.project.temporary_project() as tmp_project:
            self.assertEqual(len(tmp_project), 0)
            tmp_root_dir = tmp_project.root_directory()
            self.assertTrue(os.path.isdir(tmp_root_dir))
            for i in range(10):     # init some jobs
                tmp_project.open_job(dict(a=i)).init()
            self.assertEqual(len(tmp_project), 10)
        self.assertFalse(os.path.isdir(tmp_root_dir))


class ProjectExportImportTest(BaseProjectTest):

    def test_export(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data)
        self.assertEqual(len(self.project), 10)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a', str(i))))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_single_job(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(1):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data)
        self.assertEqual(len(self.project), 1)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_custom_path_function(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))

        with self.assertRaises(RuntimeError):
            self.project.export_to(target=prefix_data, path=lambda job: 'non_unique')

        self.project.export_to(
            target=prefix_data, path=lambda job: os.path.join('my_a', str(job.sp.a)))

        self.assertEqual(len(self.project), 10)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'my_a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'my_a', str(i))))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_custom_path_string_modify_tree_flat(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.project.open_job(dict(a=i, b=j, c=k, d=l)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))

        with self.assertRaises(RuntimeError):
            self.project.export_to(target=prefix_data, path='non_unique')

        self.project.export_to(
            target=prefix_data, path=os.path.join('a', '{a}', 'b', '{b}', '{{auto:_}}'))

        self.assertEqual(len(self.project), 80)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a',
                                                                   str(i), 'b', str(j),
                                                                   'c_%d_d_%d' % (k, l))))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_custom_path_string_modify_tree_tree(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.project.open_job(dict(a=i, b=j, c=k, d=l)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))

        with self.assertRaises(RuntimeError):
            self.project.export_to(target=prefix_data, path='non_unique')

        self.project.export_to(
            target=prefix_data, path=os.path.join('c', '{c}', 'b', '{b}', '{{auto}}'))

        self.assertEqual(len(self.project), 80)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        # self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'c',
                                                                   str(k), 'b', str(j), 'd',
                                                                   str(l), 'a', str(i))))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_custom_path_string_modify_flat_flat(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.project.open_job(dict(a=i, b=j, c=k, d=l)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))

        with self.assertRaises(RuntimeError):
            self.project.export_to(target=prefix_data, path='non_unique')

        self.project.export_to(target=prefix_data, path='c_{c}_b_{b}/{{auto:_}}')

        self.assertEqual(len(self.project), 80)
        self.assertEqual(len(os.listdir(prefix_data)), 4)
        # self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.assertTrue(os.path.isdir(os.path.join(
                            prefix_data, 'c_%d_b_%d' % (k, j), 'd_%d_a_%d' % (l, i))))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_custom_path_string_modify_flat_tree(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.project.open_job(dict(a=i, b=j, c=k, d=l)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))

        with self.assertRaises(RuntimeError):
            self.project.export_to(target=prefix_data, path='non_unique')

        self.project.export_to(
            target=prefix_data, path='c_{c}_b_{b}/{{auto}}')

        self.assertEqual(len(self.project), 80)
        self.assertEqual(len(os.listdir(prefix_data)), 4)
        # self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            for j in range(2):
                for k in range(2):
                    for l in range(2):
                        self.assertTrue(os.path.isdir(os.path.join(
                            prefix_data, 'c_%d_b_%d/d/%d/a/%d' % (k, j, l, i))))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_custom_path_string(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))

        with self.assertRaises(RuntimeError):
            self.project.export_to(target=prefix_data, path='non_unique')

        self.project.export_to(target=prefix_data, path='my_a/{job.sp.a}')  # why not jus {a}

        self.assertEqual(len(self.project), 10)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'my_a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'my_a', str(i))))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_move(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a', str(i))))
        self.assertEqual(len(self.project.import_from(origin=prefix_data)), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_custom_path_function_move(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))

        with self.assertRaises(RuntimeError):
            self.project.export_to(
                target=prefix_data,
                path=lambda job: 'non_unique',
                copytree=os.rename)

        self.project.export_to(
            target=prefix_data,
            path=lambda job: os.path.join('my_a', str(job.sp.a)),
            copytree=os.rename)

        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'my_a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'my_a', str(i))))
        self.assertEqual(len(self.project.import_from(origin=prefix_data)), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_tarfile(self):
        target = os.path.join(self._tmp_dir.name, 'data.tar')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=target)
        self.assertEqual(len(self.project), 10)
        with TarFile(name=target) as tarfile:
            for i in range(10):
                self.assertIn('a/{}'.format(i), tarfile.getnames())
        os.rename(self.project.workspace(), self.project.workspace() + '~')
        self.assertEqual(len(self.project), 0)
        self.project.import_from(origin=target)
        self.assertEqual(len(self.project), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_tarfile_zipped(self):
        target = os.path.join(self._tmp_dir.name, 'data.tar.gz')
        for i in range(10):
            with self.project.open_job(dict(a=i)) as job:
                os.makedirs(job.fn('sub-dir'))
                with open(job.fn('sub-dir/signac_statepoint.json'), 'w') as file:
                    file.write(json.dumps({"foo": 0}))
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=target)
        self.assertEqual(len(self.project), 10)
        with TarFile.open(name=target, mode='r:gz') as tarfile:
            for i in range(10):
                self.assertIn('a/{}'.format(i), tarfile.getnames())
                self.assertIn('a/{}/sub-dir/signac_statepoint.json'.format(i), tarfile.getnames())
        os.rename(self.project.workspace(), self.project.workspace() + '~')
        self.assertEqual(len(self.project), 0)
        self.project.import_from(origin=target)
        self.assertEqual(len(self.project), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))
        for job in self.project:
            self.assertTrue(job.isfile('sub-dir/signac_statepoint.json'))

    def test_export_import_zipfile(self):
        target = os.path.join(self._tmp_dir.name, 'data.zip')
        for i in range(10):
            with self.project.open_job(dict(a=i)) as job:
                os.makedirs(job.fn('sub-dir'))
                with open(job.fn('sub-dir/signac_statepoint.json'), 'w') as file:
                    file.write(json.dumps({"foo": 0}))
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=target)
        self.assertEqual(len(self.project), 10)
        with ZipFile(target) as zipfile:
            for i in range(10):
                self.assertIn('a/{}/signac_statepoint.json'.format(i), zipfile.namelist())
                self.assertIn('a/{}/sub-dir/signac_statepoint.json'.format(i), zipfile.namelist())
        os.rename(self.project.workspace(), self.project.workspace() + '~')
        self.assertEqual(len(self.project), 0)
        self.project.import_from(origin=target)
        self.assertEqual(len(self.project), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))
        for job in self.project:
            self.assertTrue(job.isfile('sub-dir/signac_statepoint.json'))

    def test_export_import(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project.import_from(prefix_data)), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_conflict(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data)
        with self.assertRaises(DestinationExistsError):
            self.assertEqual(len(self.project.import_from(prefix_data)), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_conflict_synced(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data)
        with self.assertRaises(DestinationExistsError):
            self.assertEqual(len(self.project.import_from(prefix_data)), 10)
        with self.project.temporary_project() as tmp_project:
            self.assertEqual(len(tmp_project.import_from(prefix_data)), 10)
            self.assertEqual(len(tmp_project), 10)
            self.project.sync(tmp_project)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))
        self.assertEqual(len(self.project.import_from(prefix_data, sync=True)), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_conflict_synced_with_args(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data)
        with self.assertRaises(DestinationExistsError):
            self.assertEqual(len(self.project.import_from(prefix_data)), 10)

        selection = list(self.project.find_jobs(dict(a=0)))
        os.rename(self.project.workspace(), self.project.workspace() + '~')
        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(self.project.import_from(prefix_data,
                                                      sync=dict(selection=selection))), 10)
        self.assertEqual(len(self.project), 1)
        self.assertEqual(len(self.project.find_jobs(dict(a=0))), 1)
        self.assertIn(list(self.project.find_job_ids())[0], ids_before_export)

    def test_export_import_schema_callable(self):

        def my_schema(path):
            import re
            m = re.match(r'.*\/a/(?P<a>\d+)$', path)
            if m:
                return dict(a=int(m.groupdict()['a']))

        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project.import_from(prefix_data, schema=my_schema)), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_schema_callable_non_unique(self):

        def my_schema_non_unique(path):
            import re
            m = re.match(r'.*\/a/(?P<a>\d+)$', path)
            if m:
                return dict(a=0)

        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        self.project.export_to(target=prefix_data, copytree=os.rename)
        with self.assertRaises(RuntimeError):
            self.project.import_from(prefix_data, schema=my_schema_non_unique)

    def test_export_import_simple_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a', str(i))))
        with self.assertRaises(StatepointParsingError):
            self.project.import_from(origin=prefix_data, schema='a/{b:int}')
        self.assertEqual(len(self.project.import_from(prefix_data)), 10)
        self.assertEqual(len(self.project), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_simple_path_nested_with_schema(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=dict(b=dict(c=i)))).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a.b.c'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a.b.c', str(i))))
        with self.assertRaises(StatepointParsingError):
            self.project.import_from(origin=prefix_data, schema='a.b.c/{a.b:int}')
        self.assertEqual(
            len(self.project.import_from(origin=prefix_data, schema='a.b.c/{a.b.c:int}')), 10)
        self.assertEqual(len(self.project), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_simple_path_with_float(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=float(i))).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a', str(float(i)))))
        self.assertEqual(len(self.project.import_from(prefix_data)), 10)
        self.assertEqual(len(self.project), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_complex_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        sp_0 = [{'a': i, 'b': i % 3} for i in range(5)]
        sp_1 = [{'a': i, 'b': i % 3, 'c': {'a': i, 'b': 0}} for i in range(5)]
        sp_2 = [{'a': i, 'b': i % 3, 'c': {'a': i, 'b': 0, 'c': {'a': i, 'b': 0}}}
                for i in range(5)]
        statepoints = sp_0 + sp_1 + sp_2
        for sp in statepoints:
            self.project.open_job(sp).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.project.import_from(prefix_data)
        self.assertEqual(len(self.project), len(statepoints))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_simple_path_schema_from_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a', str(i))))
        ret = self.project.import_from(origin=prefix_data, schema='a/{a:int}')
        self.assertEqual(len(ret), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_simple_path_schema_from_path_float(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        for i in range(10):
            self.project.open_job(dict(a=float(i))).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.assertEqual(len(os.listdir(prefix_data)), 1)
        self.assertEqual(len(os.listdir(os.path.join(prefix_data, 'a'))), 10)
        for i in range(10):
            self.assertTrue(os.path.isdir(os.path.join(prefix_data, 'a', str(float(i)))))
        ret = self.project.import_from(origin=prefix_data, schema='a/{a:int}')
        self.assertEqual(len(ret), 0)  # should not match
        ret = self.project.import_from(origin=prefix_data, schema='a/{a:float}')
        self.assertEqual(len(ret), 10)
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_export_import_complex_path_nested_schema_from_path(self):
        prefix_data = os.path.join(self._tmp_dir.name, 'data')
        statepoints = [{'a': i, 'b': {'c': i % 3}} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.export_to(target=prefix_data, copytree=os.rename)
        self.assertEqual(len(self.project), 0)
        self.project.import_from(origin=prefix_data, schema='b.c/{b.c:int}/a/{a:int}')
        self.assertEqual(len(self.project), len(statepoints))
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))

    def test_import_own_project(self):
        for i in range(10):
            self.project.open_job(dict(a=i)).init()
        ids_before_export = list(sorted(self.project.find_job_ids()))
        self.project.import_from(origin=self.project.workspace())
        self.assertEqual(ids_before_export, list(sorted(self.project.find_job_ids())))
        with self.project.temporary_project() as tmp_project:
            tmp_project.import_from(origin=self.project.workspace())
            self.assertEqual(ids_before_export, list(sorted(tmp_project.find_job_ids())))
            self.assertEqual(len(tmp_project), len(self.project))


class ProjectRepresentationTest(BaseProjectTest):

    valid_sp_values = [None, 0, 1, 0.0, 1.0, True, False, [0, 1, 2], [0, 1.0, False]]

    num_few_jobs = 10
    num_many_jobs = 200

    def call_repr_methods(self):

        with self.subTest(of='project'):
            with self.subTest(type='str'):
                str(self.project)
            with self.subTest(type='repr'):
                self.assertEqual(eval(repr(self.project)), self.project)
            with self.subTest(type='html'):
                for use_pandas in (True, False):
                    type(self.project)._use_pandas_for_html_repr = use_pandas
                    with self.subTest(use_pandas=use_pandas):
                        if use_pandas and not PANDAS:
                            raise unittest.SkipTest('requires use_pandas')
                        self.project._repr_html_()

        with self.subTest(of='JobsCursor'):
            for filter_ in (None, ):
                with self.subTest(filter=filter_):
                    with self.subTest(type='str'):
                        str(self.project.find_jobs(filter_))
                    with self.subTest(type='repr'):
                        q = self.project.find_jobs(filter_)
                        self.assertEqual(eval(repr(q)), q)
                    with self.subTest(type='html'):
                        for use_pandas in (True, False):
                            type(self.project)._use_pandas_for_html_repr = use_pandas
                            with self.subTest(use_pandas=use_pandas):
                                if use_pandas and not PANDAS:
                                    raise unittest.SkipTest('requires use_pandas')
                                self.project.find_jobs(filter_)._repr_html_()

    def test_repr_no_jobs(self):
        self.call_repr_methods()

    def test_repr_few_jobs_homogeneous(self):
        # Many jobs with many different state points
        for i in range(self.num_few_jobs):
            self.project.open_job(
                {'{}_{}'.format(i, j): v
                 for j, v in enumerate(self.valid_sp_values)}).init()
        self.call_repr_methods()

    def test_repr_many_jobs_homogeneous(self):
        # Many jobs with many different state points
        for i in range(self.num_many_jobs):
            self.project.open_job(
                {'{}_{}'.format(i, j): v
                 for j, v in enumerate(self.valid_sp_values)}).init()
        self.call_repr_methods()

    def test_repr_few_jobs_heterogeneous(self):
        # Many jobs with many different state points
        for i in range(self.num_few_jobs):
            for v in self.valid_sp_values:
                self.project.open_job(dict(a=v)).init()
        self.call_repr_methods()

    def test_repr_many_jobs_heterogeneous(self):
        # Many jobs with many different state points
        for i in range(self.num_many_jobs):
            for v in self.valid_sp_values:
                self.project.open_job(dict(a=v)).init()
        self.call_repr_methods()


class LinkedViewProjectTest(BaseProjectTest):

    def test_create_linked_view(self):

        def clean(filter=None):
            """Helper function for wiping out views"""
            for job in self.project.find_jobs(filter):
                job.remove()
            self.project.create_linked_view(prefix=view_prefix)

        sp_0 = [{'a': i, 'b': i % 3} for i in range(5)]
        sp_1 = [{'a': i, 'b': i % 3, 'c': {'a': i, 'b': 0}} for i in range(5)]
        sp_2 = [{'a': i, 'b': i % 3, 'c': {'a': i, 'b': 0, 'c': {'a': i, 'b': 0}}}
                for i in range(5)]
        statepoints = sp_0 + sp_1 + sp_2
        view_prefix = os.path.join(self._tmp_pr, 'view')
        # empty project
        self.project.create_linked_view(prefix=view_prefix)
        # one job
        self.project.open_job(statepoints[0]).init()
        self.project.create_linked_view(prefix=view_prefix)
        # more jobs
        for sp in statepoints:
            self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)
        self.assertTrue(os.path.isdir(view_prefix))
        all_links = list(_find_all_links(view_prefix))
        dst = set(map(lambda l: os.path.realpath(os.path.join(view_prefix, l, 'job')), all_links))
        src = set(map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs()))
        self.assertEqual(len(all_links), self.project.num_jobs())
        self.project.create_linked_view(prefix=view_prefix)
        all_links = list(_find_all_links(view_prefix))
        self.assertEqual(len(all_links), self.project.num_jobs())
        dst = set(map(lambda l: os.path.realpath(os.path.join(view_prefix, l, 'job')), all_links))
        src = set(map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs()))
        self.assertEqual(src, dst)
        # update with subset
        subset = list(self.project.find_job_ids({'b': 0}))
        job_subset = [self.project.open_job(id=id) for id in subset]

        # Catch deprecation warning for use of index.
        with warnings.catch_warnings(record=True) as w:
            bad_index = [dict(_id=i) for i in range(3)]
            with self.assertRaises(ValueError):
                self.project.create_linked_view(prefix=view_prefix, job_ids=subset, index=bad_index)
            self.assertEqual(len(w), 1)
            self.assertEqual(w[0].category, DeprecationWarning)

        self.project.create_linked_view(prefix=view_prefix, job_ids=subset)
        all_links = list(_find_all_links(view_prefix))
        self.assertEqual(len(all_links), len(subset))
        dst = set(map(lambda l: os.path.realpath(os.path.join(view_prefix, l, 'job')), all_links))
        src = set(map(lambda j: os.path.realpath(j.workspace()), job_subset))
        self.assertEqual(src, dst)
        # some jobs removed
        clean({'b': 0})
        all_links = list(_find_all_links(view_prefix))
        self.assertEqual(len(all_links), self.project.num_jobs())
        dst = set(map(lambda l: os.path.realpath(os.path.join(view_prefix, l, 'job')), all_links))
        src = set(map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs()))
        self.assertEqual(src, dst)
        # all jobs removed
        clean()
        all_links = list(_find_all_links(view_prefix))
        self.assertEqual(len(all_links), self.project.num_jobs())
        dst = set(map(lambda l: os.path.realpath(os.path.join(view_prefix, l, 'job')), all_links))
        src = set(map(lambda j: os.path.realpath(j.workspace()), self.project.find_jobs()))
        self.assertEqual(src, dst)

    def test_create_linked_view_homogeneous_schema_tree(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'c', str(
                        sp['c']), 'b', str(sp['b']), 'a', str(sp['a']), 'job')))

    def test_create_linked_view_homogeneous_schema_tree_tree(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path='a/{a}/{{auto}}')

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(
                        sp['a']), 'c', str(sp['c']), 'b', str(sp['b']), 'job')))

    def test_create_linked_view_homogeneous_schema_tree_flat(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path='a/{a}/{{auto:_}}')

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(
                        sp['a']), 'c_%s_b_%s' % (str(sp['c']), str(sp['b'])), 'job')))

    def test_create_linked_view_homogeneous_schema_flat_flat(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path='a_{a}/{{auto:_}}')

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'b': b, 'c': c}
                    self.assertTrue(os.path.isdir(os.path.join(
                        view_prefix, 'a_%s/c_%s_b_%s' % (str(sp['a']), str(sp['c']), str(sp['b'])),
                        'job')))

    def test_create_linked_view_homogeneous_schema_flat_tree(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(10)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        d_vals = ["rock", "paper", "scissors"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    for d in d_vals:
                        sp = {'a': a, 'b': b, 'c': c, 'd': d}
                        self.project.open_job(sp).init()

        self.project.create_linked_view(prefix=view_prefix, path='a_{a}/{{auto}}')

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    for d in d_vals:
                        sp = {'a': a, 'b': b, 'c': c, 'd': d}
                        self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a_%s' %
                                                                   str(sp['a']), 'c', str(sp['c']),
                                                                   'd', str(sp['d']), 'b',
                                                                   str(sp['b']), 'job')))

    def test_create_linked_view_homogeneous_schema_nested(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(2)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'd': {'b': b, 'c': c}}
                    self.project.open_job(sp).init()

        self.project.create_linked_view(prefix=view_prefix)

        # check all dir:
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'd': {'b': b, 'c': c}}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']),
                                                               'd.c', str(sp['d']['c']), 'd.b',
                                                               str(sp['d']['b']), 'job')))

    def test_create_linked_view_homogeneous_schema_nested_provide_partial_path(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(2)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'd': {'b': b, 'c': c}}
                    self.project.open_job(sp).init()

        self.project.create_linked_view(prefix=view_prefix, path='a/{a}/d.c/{d.c}/{{auto}}')

        # check all dir:
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    sp = {'a': a, 'd': {'b': b, 'c': c}}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']),
                                                               'd.c', str(sp['d']['c']), 'd.b',
                                                               str(sp['d']['b']), 'job')))

    def test_create_linked_view_heterogeneous_disjoint_schema(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(5)
        b_vals = range(3, 13)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                sp = {'a': a, 'b': b}
                self.project.open_job(sp).init()
            for c in c_vals:
                sp = {'a': a, 'c': c}
                self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        # test each directory
        for a in a_vals:
            for b in b_vals:
                sp = {'a': a, 'b': b}
                self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']),
                                                           'b', str(sp['b']), 'job')))
            for c in c_vals:
                sp = {'a': a, 'c': c}
                self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'c', sp['c'], 'a',
                                                           str(sp['a']), 'job')))

    def test_create_linked_view_heterogeneous_disjoint_schema_nested(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(2)
        b_vals = range(3, 8)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                sp = {'a': a, 'd': {'b': b}}
                self.project.open_job(sp).init()
            for c in c_vals:
                sp = {'a': a, 'd': {'c': c}}
                self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                sp = {'a': a, 'd': {'b': b}}
                self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']),
                                                           'd.b', str(sp['d']['b']), 'job')))
            for c in c_vals:
                sp = {'a': a, 'd': {'c': c}}
                self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']), 'd.c',
                                                           sp['d']['c'], 'job')))

    def test_create_linked_view_heterogeneous_fizz_schema_flat(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(5)
        b_vals = range(5)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    if a % 3 == 0:
                        sp = {'a': a, 'b': b}
                    else:
                        sp = {'a': a, 'b': b, 'c': c}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                for c in c_vals:
                    if a % 3 == 0:
                        sp = {'a': a, 'b': b}
                        self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']),
                                                                   'b', str(sp['b']), 'job')))
                    else:
                        sp = {'a': a, 'b': b, 'c': c}
                        self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'c', sp['c'], 'a',
                                                                   str(sp['a']), 'b', str(sp['b']),
                                                                   'job')))

    def test_create_linked_view_heterogeneous_schema_nested(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(5)
        b_vals = range(10)
        for a in a_vals:
            for b in b_vals:
                if a % 3 == 0:
                    sp = {'a': a, 'b': {'c': b}}
                else:
                    sp = {'a': a, 'b': b}
                self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        for a in a_vals:
            for b in b_vals:
                if a % 3 == 0:
                    sp = {'a': a, 'b': {'c': b}}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']),
                                                               'b.c', str(sp['b']['c']), 'job')))
                else:
                    sp = {'a': a, 'b': b}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'a', str(sp['a']),
                                                               'b', str(sp['b']), 'job')))

    def test_create_linked_view_heterogeneous_schema_nested_partial_homogenous_path_provide(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(5)
        b_vals = range(10)
        d_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for d in d_vals:
                for b in b_vals:
                    if a % 3 == 0:
                        sp = {'a': a, 'b': {'c': b}, 'd': d}
                    else:
                        sp = {'a': a, 'b': b, 'd': d}
                    self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix, path='d/{d}/{{auto}}')

        for a in a_vals:
            for b in b_vals:
                if a % 3 == 0:
                    sp = {'a': a, 'b': {'c': b}, 'd': d}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'd', sp['d'], 'a',
                                                               str(sp['a']), 'b.c',
                                                               str(sp['b']['c']), 'job')))
                else:
                    sp = {'a': a, 'b': b, 'd': d}
                    self.assertTrue(os.path.isdir(os.path.join(view_prefix, 'd', sp['d'], 'a',
                                                               str(sp['a']), 'b', str(sp['b']),
                                                               'job')))

    def test_create_linked_view_heterogeneous_schema_problematic(self):
        self.project.open_job(dict(a=1)).init()
        self.project.open_job(dict(a=1, b=1)).init()
        view_prefix = os.path.join(self._tmp_pr, 'view')
        with self.assertRaises(RuntimeError):
            self.project.create_linked_view(view_prefix)

    def test_create_linked_view_with_slash_raises_error(self):
        bad_chars = [os.sep, " ", "*"]
        statepoints = [{
            'a{}b'.format(i): 0, 'b': 'bad{}val'.format(i)
            } for i in bad_chars]
        view_prefix = os.path.join(self._tmp_pr, 'view')
        for sp in statepoints:
            self.project.open_job(sp).init()
            with self.assertRaises(RuntimeError):
                self.project.create_linked_view(prefix=view_prefix)


class UpdateCacheAfterInitJob(signac.contrib.job.Job):

    def init(self, *args, **kwargs):
        super(UpdateCacheAfterInitJob, self).init(*args, **kwargs)
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=FutureWarning, module='signac')
            self._project.update_cache()


class UpdateCacheAfterInitJobProject(signac.Project):
    "This is a test class that regularly calls the update_cache() method."
    Job = UpdateCacheAfterInitJob


class CachedProjectTest(ProjectTest):

    project_class = UpdateCacheAfterInitJobProject

    def test_repr(self):
        repr(self)


class ProjectInitTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)

    def test_get_project(self):
        root = self._tmp_dir.name
        with self.assertRaises(LookupError):
            signac.get_project(root=root)
        project = signac.init_project(name='testproject', root=root)
        self.assertEqual(project.get_id(), 'testproject')
        self.assertEqual(project.workspace(), os.path.join(root, 'workspace'))
        self.assertEqual(project.root_directory(), root)
        project = signac.Project.init_project(name='testproject', root=root)
        self.assertEqual(project.get_id(), 'testproject')
        self.assertEqual(project.workspace(), os.path.join(root, 'workspace'))
        self.assertEqual(project.root_directory(), root)
        project = signac.get_project(root=root)
        self.assertEqual(project.get_id(), 'testproject')
        self.assertEqual(project.workspace(), os.path.join(root, 'workspace'))
        self.assertEqual(project.root_directory(), root)
        project = signac.Project.get_project(root=root)
        self.assertEqual(project.get_id(), 'testproject')
        self.assertEqual(project.workspace(), os.path.join(root, 'workspace'))
        self.assertEqual(project.root_directory(), root)

    def test_get_project_non_local(self):
        root = self._tmp_dir.name
        subdir = os.path.join(root, 'subdir')
        os.mkdir(subdir)
        project = signac.init_project(root=root, name='testproject')
        self.assertEqual(project, project.get_project(root=root))
        self.assertEqual(project, signac.get_project(root=root))
        self.assertEqual(project, project.get_project(root=root, search=False))
        self.assertEqual(project, signac.get_project(root=root, search=False))
        self.assertEqual(project, project.get_project(root=os.path.relpath(root), search=False))
        self.assertEqual(project, signac.get_project(root=os.path.relpath(root), search=False))
        with self.assertRaises(LookupError):
            self.assertEqual(project, project.get_project(root=subdir, search=False))
        with self.assertRaises(LookupError):
            self.assertEqual(project, signac.get_project(root=subdir, search=False))
        self.assertEqual(project, project.get_project(root=subdir, search=True))
        self.assertEqual(project, signac.get_project(root=subdir, search=True))

    def test_init(self):
        root = self._tmp_dir.name
        with self.assertRaises(LookupError):
            signac.get_project(root=root)
        project = signac.init_project(name='testproject', root=root)
        self.assertEqual(project.get_id(), 'testproject')
        self.assertEqual(project.workspace(), os.path.join(root, 'workspace'))
        self.assertEqual(project.root_directory(), root)
        # Second initialization should not make any difference.
        project = signac.init_project(name='testproject', root=root)
        project = signac.get_project(root=root)
        self.assertEqual(project.get_id(), 'testproject')
        self.assertEqual(project.workspace(), os.path.join(root, 'workspace'))
        self.assertEqual(project.root_directory(), root)
        project = signac.Project.get_project(root=root)
        self.assertEqual(project.get_id(), 'testproject')
        self.assertEqual(project.workspace(), os.path.join(root, 'workspace'))
        self.assertEqual(project.root_directory(), root)
        # Deviating initialization parameters should result in errors.
        with self.assertRaises(RuntimeError):
            signac.init_project(name='testproject2', root=root)
        with self.assertRaises(RuntimeError):
            signac.init_project(
                name='testproject',
                root=root,
                workspace='workspace2')
        with self.assertRaises(RuntimeError):
            signac.init_project(
                name='testproject2',
                root=root,
                workspace='workspace2')

    def test_nested_project(self):
        def check_root(root=None):
            if root is None:
                root = os.getcwd()
            self.assertEqual(
                os.path.realpath(signac.get_project(root=root).root_directory()),
                os.path.realpath(root))
        root = self._tmp_dir.name
        root_a = os.path.join(root, 'project_a')
        root_b = os.path.join(root_a, 'project_b')
        signac.init_project('testprojectA', root_a)
        self.assertEqual(signac.get_project(root=root_a).get_id(), 'testprojectA')
        check_root(root_a)
        signac.init_project('testprojectB', root_b)
        self.assertEqual(signac.get_project(root=root_b).get_id(), 'testprojectB')
        check_root(root_b)
        cwd = os.getcwd()
        try:
            os.chdir(root_a)
            check_root()
            self.assertEqual(signac.get_project().get_id(), 'testprojectA')
        finally:
            os.chdir(cwd)
        try:
            os.chdir(root_b)
            self.assertEqual(signac.get_project().get_id(), 'testprojectB')
            check_root()
        finally:
            os.chdir(cwd)

    def test_get_job_valid_workspace(self):
        # Test case: The root-path is the job workspace path.
        root = self._tmp_dir.name
        project = signac.init_project(name='testproject', root=root)
        job = project.open_job({'a': 1})
        job.init()
        with job:
            # The context manager enters the working directory of the job
            self.assertEqual(project.get_job(), job)
            self.assertEqual(signac.get_job(), job)

    def test_get_job_invalid_workspace(self):
        # Test case: The root-path is not the job workspace path.
        root = self._tmp_dir.name
        project = signac.init_project(name='testproject', root=root)
        job = project.open_job({'a': 1})
        job.init()
        # We shouldn't be able to find a job while in the workspace directory,
        # since no signac_statepoint.json exists.
        cwd = os.getcwd()
        try:
            os.chdir(project.workspace())
            with self.assertRaises(LookupError):
                project.get_job()
            with self.assertRaises(LookupError):
                signac.get_job()
        finally:
            os.chdir(cwd)

    def test_get_job_nested_project(self):
        # Test case: The job workspace dir is also a project root dir.
        root = self._tmp_dir.name
        project = signac.init_project(name='testproject', root=root)
        job = project.open_job({'a': 1})
        job.init()
        with job:
            nestedproject = signac.init_project('nestedproject')
            nestedproject.open_job({'b': 2}).init()
            self.assertEqual(project.get_job(), job)
            self.assertEqual(signac.get_job(), job)

    def test_get_job_subdir(self):
        # Test case: Get a job from a sub-directory of the job workspace dir.
        root = self._tmp_dir.name
        project = signac.init_project(name='testproject', root=root)
        job = project.open_job({'a': 1})
        job.init()
        with job:
            os.mkdir('test_subdir')
            self.assertEqual(project.get_job('test_subdir'), job)
            self.assertEqual(signac.get_job('test_subdir'), job)
        self.assertEqual(project.get_job(job.fn('test_subdir')), job)
        self.assertEqual(signac.get_job(job.fn('test_subdir')), job)

    def test_get_job_nested_project_subdir(self):
        # Test case: Get a job from a sub-directory of the job workspace dir
        # when the job workspace is also a project root dir
        root = self._tmp_dir.name
        project = signac.init_project(name='testproject', root=root)
        job = project.open_job({'a': 1})
        job.init()
        with job:
            nestedproject = signac.init_project('nestedproject')
            nestedproject.open_job({'b': 2}).init()
            os.mkdir('test_subdir')
            self.assertEqual(project.get_job('test_subdir'), job)
            self.assertEqual(signac.get_job('test_subdir'), job)
        self.assertEqual(project.get_job(job.fn('test_subdir')), job)
        self.assertEqual(signac.get_job(job.fn('test_subdir')), job)

    def test_get_job_symlink_other_project(self):
        # Test case: Get a job from a symlink in another project workspace
        root = self._tmp_dir.name
        project_a_dir = os.path.join(root, 'project_a')
        project_b_dir = os.path.join(root, 'project_b')
        os.mkdir(project_a_dir)
        os.mkdir(project_b_dir)
        project_a = signac.init_project(name='project_a', root=project_a_dir)
        project_b = signac.init_project(name='project_b', root=project_b_dir)
        job_a = project_a.open_job({'a': 1})
        job_a.init()
        job_b = project_b.open_job({'b': 1})
        job_b.init()
        symlink_path = os.path.join(project_b.workspace(), job_a._id)
        os.symlink(job_a.ws, symlink_path)
        self.assertEqual(project_a.get_job(symlink_path), job_a)
        self.assertEqual(project_b.get_job(symlink_path), job_a)
        self.assertEqual(signac.get_job(symlink_path), job_a)


class ProjectPicklingTest(BaseProjectTest):

    def test_pickle_project_empty(self):
        blob = pickle.dumps(self.project)
        self.assertEqual(pickle.loads(blob), self.project)

    def test_pickle_project_with_jobs(self):
        for i in range(3):
            self.project.open_job(dict(a=i, b=dict(c=i), d=list(range(i, i+3)))).init()
        blob = pickle.dumps(self.project)
        self.assertEqual(pickle.loads(blob), self.project)

    def test_pickle_jobs_directly(self):
        for i in range(3):
            self.project.open_job(dict(a=i, b=dict(c=i), d=list(range(i, i+3)))).init()
        for job in self.project:
            self.assertEqual(pickle.loads(pickle.dumps(job)), job)


class TestTestingProjectInitialization(BaseProjectTest):

    # Sanity check on all different combinations of inputs
    def test_input_args(self):
        for nested, listed, het in itertools.product([True, False], repeat=3):
            with self.project.temporary_project() as tmp_project:
                jobs = signac.testing.init_jobs(
                    tmp_project, nested=nested, listed=listed, heterogeneous=het)
                self.assertGreater(len(tmp_project), 0)
                self.assertEqual(len(tmp_project), len(jobs))
                # check that call does not fail:
                tmp_project.detect_schema()


if __name__ == '__main__':
    unittest.main()
