# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import uuid
import warnings
import logging

import signac
from signac.common import six
from signac.contrib.formats import TextFile
from signac.errors import DestinationExistsError
from signac.contrib.project import _find_all_links

from test_job import BaseJobTest

if six.PY2:
    logging.basicConfig(level=logging.WARNING)
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


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

    def test_workspace_directory_with_env_variable(self):
        os.environ['SIGNAC_ENV_DIR_TEST'] = self._tmp_wd
        self.project.config['workspace_dir'] = '${SIGNAC_ENV_DIR_TEST}'
        self.assertEqual(self._tmp_wd, self.project.workspace())

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

    def test_find_statepoints(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        self.assertEqual(
            len(statepoints),
            len(list(self.project.find_statepoints())))
        self.assertEqual(
            1, len(list(self.project.find_statepoints({'a': 0}))))

    def test_find_statepoint_sequences(self):
        statepoints = [{'a': (i, i+1)} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        self.assertEqual(
            len(statepoints),
            len(list(self.project.find_statepoints())))
        self.assertEqual(
            1,
            len(list(self.project.find_statepoints({'a': [0, 1]}))))
        self.assertEqual(
            1,
            len(list(self.project.find_statepoints({'a': (0, 1)}))))

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
            self.project.find_jobs({'$and': {'foo': 'bar'}})
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
        self.assertEqual(len(self.project.find_jobs({'$or': [{}, {'b': {'c': 0}}]})), len(self.project))
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
        docs = self.project.find_jobs()
        self.assertEqual(len(statepoints), len(self.project.find_jobs()))

    def test_len_find_jobs(self):
        statepoints = [{'a': i, 'b': i<3} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        self.assertEqual(len(self.project), len(self.project.find_jobs()))
        self.assertEqual(3, len(self.project.find_jobs({'b': True})))

    def test_iteration(self):
        statepoints = [{'a': i, 'b': i<3} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).init()
        for i, job in enumerate(self.project):
            pass
        self.assertEqual(i, len(self.project)-1)

    def test_open_job_by_id(self):
        statepoints = [{'a': i} for i in range(5)]
        jobs = [self.project.open_job(sp) for sp in statepoints]
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
        jobs = [self.project.open_job(sp).init() for sp in statepoints]
        aid_len = self.project.min_len_unique_id()
        for job in self.project.find_jobs():
            aid = job.get_id()[:aid_len]
            self.assertEqual(self.project.open_job(id=aid), job)
        with self.assertRaises(LookupError):
            for job in self.project.find_jobs():
                self.project.open_job(id=job.get_id()[:aid_len-1])
        with self.assertRaises(KeyError):
            self.project.open_job(id='abc')

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
        bad_index = [dict(_id=i) for i in range(3)]
        with self.assertRaises(ValueError):
            self.project.create_linked_view(prefix=view_prefix, job_ids=subset, index=bad_index)
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

    def test_create_linked_view_homogeneous_schema_flat(self):
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

        # Loop over levels and test each of them
        c_dirs = os.listdir(view_prefix)
        self.assertEqual(sorted(['_'.join(['c', x]) for x in c_vals]), sorted(c_dirs))
        for c in c_dirs:
            c_view_prefix = os.path.join(view_prefix, c)
            b_dirs = os.listdir(c_view_prefix)
            self.assertEqual(sorted(['_'.join(['b', str(x)]) for x in b_vals]), sorted(b_dirs))
            for b in b_dirs:
                b_view_prefix = os.path.join(c_view_prefix, b)
                a_dirs = os.listdir(b_view_prefix)
                self.assertEqual(sorted(['_'.join(['a', str(x)]) for x in a_vals]), sorted(a_dirs))

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

        # Loop over levels and test each of them
        a_dirs = os.listdir(view_prefix)
        self.assertEqual(sorted(['_'.join(['a', str(x)]) for x in a_vals]), sorted(a_dirs))
        for a in a_dirs:
            a_view_prefix = os.path.join(view_prefix, a)
            d_c_dirs = os.listdir(a_view_prefix)
            self.assertEqual(
                sorted(['_'.join(['d', 'c', str(x)]) for x in c_vals]),
                sorted(d_c_dirs))
            for d_c in d_c_dirs:
                d_c_view_prefix = os.path.join(a_view_prefix, d_c)
                d_b_dirs = os.listdir(d_c_view_prefix)
                self.assertEqual(
                    sorted(['_'.join(['d', 'b', str(x)]) for x in b_vals]),
                    sorted(d_b_dirs))

    def test_create_linked_view_heterogeneous_disjoint_schema_flat(self):
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

        # Loop over levels and test each of them
        root_dirs = os.listdir(view_prefix)
        expected_a_dirs = ['_'.join(['a', str(x)]) for x in a_vals]
        expected_b_dirs = ['_'.join(['b', str(x)]) for x in b_vals]
        expected_c_dirs = ['_'.join(['c', x]) for x in c_vals]
        self.assertEqual(sorted(expected_a_dirs + expected_c_dirs), sorted(root_dirs))
        for rt in root_dirs:
            sub_view_prefix = os.path.join(view_prefix, rt)
            subdirs = os.listdir(sub_view_prefix)
            if rt in expected_a_dirs:
                self.assertEqual(sorted(expected_b_dirs), sorted(subdirs))
            elif rt in expected_c_dirs:
                self.assertEqual(sorted(expected_a_dirs), sorted(subdirs))
            else:
                raise RuntimeError("Unexpected top-level directory.")

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

        # Loop over levels and test each of them
        a_dirs = os.listdir(view_prefix)
        self.assertEqual(sorted(['_'.join(['a', str(x)]) for x in a_vals]), sorted(a_dirs))
        for a in a_dirs:
            a_view_prefix = os.path.join(view_prefix, a)
            d_dirs = os.listdir(a_view_prefix)
            expected_d_b_dirs = ['_'.join(['d', 'b', str(x)]) for x in b_vals]
            expected_d_c_dirs = ['_'.join(['d', 'c', str(x)]) for x in c_vals]
            self.assertEqual(sorted(expected_d_b_dirs + expected_d_c_dirs), sorted(d_dirs))

    def test_create_linked_view_heterogeneous_fizz_buzz_schema_flat(self):
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

        # Loop over levels and test each of them
        root_dirs = os.listdir(view_prefix)
        expected_b_dirs = ['_'.join(['b', str(x)]) for x in b_vals]
        expected_a_dirs = ['a_0', 'a_3']
        expected_c_dirs = ['c_bar', 'c_baz', 'c_foo']
        self.assertEqual(sorted(expected_a_dirs + expected_c_dirs), sorted(root_dirs))
        for rt in root_dirs:
            sub_view_prefix = os.path.join(view_prefix, rt)
            subdirs = os.listdir(sub_view_prefix)
            if rt in expected_a_dirs:
                self.assertEqual(sorted(expected_b_dirs), sorted(subdirs))
            elif rt in expected_c_dirs:
                self.assertEqual(['a_1', 'a_2', 'a_4'], sorted(subdirs))
            else:
                raise RuntimeError("Unexpected top-level directory.")

    def test_create_linked_view_heterogeneous_fizz_buzz_schema_nested(self):
        view_prefix = os.path.join(self._tmp_pr, 'view')
        a_vals = range(5)
        b_vals = range(10)
        c_vals = ["foo", "bar", "baz"]
        for a in a_vals:
            for b in b_vals:
                if a % 3 == 0:
                    sp = {'a': a, 'b': {'c': b}}
                else:
                    sp = {'a': a, 'b': b}
                self.project.open_job(sp).init()
        self.project.create_linked_view(prefix=view_prefix)

        # Loop over levels and test each of them
        root_dirs = os.listdir(view_prefix)
        expected_a_dirs = ['_'.join(['a', str(x)]) for x in a_vals]
        expected_b_dirs = ['_'.join(['b', str(x)]) for x in b_vals]
        expected_nested_b_dirs = ['_'.join(['b', 'c', str(x)]) for x in b_vals]
        self.assertEqual(sorted(root_dirs), sorted(expected_a_dirs))
        for rt in root_dirs:
            sub_view_prefix = os.path.join(view_prefix, rt)
            subdirs = os.listdir(sub_view_prefix)
            a, x = rt.split('_')
            if int(x) % 3 == 0:
                self.assertEqual(sorted(subdirs), expected_nested_b_dirs)
            else:
                self.assertEqual(sorted(subdirs), expected_b_dirs)

    def test_find_job_documents(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        self.assertEqual(
            len(list(self.project.find_job_documents({'a': 0}))), 1)
        job_docs = list(self.project.find_job_documents())
        self.assertEqual(len(statepoints), len(job_docs))
        for job_doc in job_docs:
            sp = job_doc['statepoint']
            self.assertEqual(str(self.project.open_job(sp)), job_doc['_id'])

    def test_find_job_documents_illegal_key(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        list(self.project.find_job_documents())
        self.assertEqual(len(statepoints), len(
            list(self.project.find_job_documents())))
        list(self.project.find_job_documents({'a': 1}))
        self.project.open_job({'a': 0}).document['_id'] = True
        with self.assertRaises(KeyError):
            list(self.project.find_job_documents())
        del self.project.open_job({'a': 0}).document['_id']
        list(self.project.find_job_documents())
        self.project.open_job({'a': 1}).document['statepoint'] = True
        with self.assertRaises(KeyError):
            list(self.project.find_job_documents())
        del self.project.open_job({'a': 1}).document['statepoint']
        list(self.project.find_job_documents())

    def test_repair_corrupted_workspace(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        # no manifest file
        with self.project.open_job(statepoints[0]) as job:
            os.remove(job.FN_MANIFEST)
        # blank manifest file
        with self.project.open_job(statepoints[1]) as job:
            with open(job.FN_MANIFEST, 'w'):
                pass
        # disable logging temporarily
        try:
            logging.disable(logging.CRITICAL)
            with self.assertRaises(Exception):
                for i, statepoint in enumerate(self.project.find_statepoints()):
                    pass
            # The skip_errors function helps to identify corrupt directories.
            for i, statepoint in enumerate(self.project.find_statepoints(
                    skip_errors=True)):
                pass
            with self.assertRaises(RuntimeWarning):
                self.project.repair()
            self.project.write_statepoints(statepoints)
            self.project.repair()
            for i, statepoint in enumerate(self.project.find_statepoints()):
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
        docs = list(self.project.index({'.*/test.txt': TextFile}))
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
        self.assertEqual(index, index2)
        for job in self.project.find_jobs():
            with open(job.fn('test.txt'), 'w') as file:
                file.write('test\n')
        formats = {r'.*/test\.txt': signac.contrib.formats.TextFile}
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


if __name__ == '__main__':
    unittest.main()
