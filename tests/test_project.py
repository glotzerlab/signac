import unittest
import os
import uuid
import warnings
import logging

import signac

from test_job import BaseJobTest


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
        self.assertEqual(self.project.get_id(), self.config['project'])
        self.assertEqual(str(self.project), self.project.get_id())

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
            job = self.project.open_job(sp)
            job.document['test'] = True
        self.assertEqual(len(statepoints), len(
            list(self.project.find_statepoints())))

    def test_find_jobs(self):
        statepoints = [{'a': i} for i in range(5)]
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        self.assertEqual(len(statepoints), len(list(self.project.find_jobs())))
        self.assertEqual(1, len(list(self.project.find_jobs({'a': 0}))))
        self.assertEqual(0, len(list(self.project.find_jobs({'a': 5}))))

    def test_create_view(self):
        # Test for highly heterogenous parameter space
        sp_0 = [{'a': i, 'b': 0} for i in range(5)]
        sp_1 = [{'a': i, 'b': 0, 'c': {'a': i, 'b': 0}} for i in range(5)]
        sp_2 = [{'a': i, 'b': 0, 'c': {'a': i, 'b': 0, 'c': {'a': i, 'b': 0}}}
                for i in range(5)]
        statepoints = sp_0 + sp_1 + sp_2
        for sp in statepoints:
            self.project.open_job(sp).document['test'] = True
        key_set = list(signac.contrib.project._find_unique_keys(statepoints))
        self.assertEqual(len(statepoints), len(
            list(signac.contrib.project._make_urls(statepoints, key_set))))
        view_prefix = os.path.join(self._tmp_pr, 'view')
        self.project.create_view(prefix=view_prefix)
        self.assertTrue(os.path.isdir(view_prefix))

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
        self.project.open_job({'a': 0}).document['_id'] = True
        list(self.project.find_job_documents({'a': 1}))  # should not throw
        with self.assertRaises(KeyError):
            list(self.project.find_job_documents())
        with self.assertRaises(KeyError):
            list(self.project.find_job_documents({'a': 0}))
        self.project.open_job({'a': 1}).document['statepoint'] = True
        with self.assertRaises(KeyError):
            list(self.project.find_job_documents())
        with self.assertRaises(KeyError):
            list(self.project.find_job_documents({'a': 1}))
        list(self.project.find_job_documents({'a': 2}))  # should not throw

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
        # Implement logging temporarily
        try:
            logging.disable(logging.CRITICAL)
            # There is really not much that we can but warn the user here.
            # We might introduce a repair() function at some point.
            with self.assertRaises(ValueError):
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
        except:
            raise
        finally:
            logging.disable(logging.NOTSET)



if __name__ == '__main__':
    unittest.main()
