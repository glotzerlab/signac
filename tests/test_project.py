import unittest
import os
import uuid
import tempfile
import subprocess
import warnings
from tempfile import TemporaryDirectory

import pymongo

from test_job import JobTest

# Make sure the jobs created for this test are unique.
test_token = {'test_token': str(uuid.uuid4())}

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')

PYMONGO_3 = pymongo.version_tuple[0] == 3

@unittest.skipIf(not PYMONGO_3, 'test requires pymongo version >= 3.0.x')
class ProjectTest(JobTest):
    pass

class ProjectViewTest(ProjectTest):
    
    def test_get_links(self):
        project = self.project
        A = ['a_{}'.format(i) for i in range(2)]
        B = ['b_{}'.format(i) for i in range(2)]
        for a in A:
            for b in B:
                p = dict(test_token)
                p.update({'a': a, 'b': b})
                with project.open_job(p) as test_job:
                    test_job.document['result'] = True
        url = 'view/a/{a}/b/{b}'
        self.assertEqual(
            len(list(project.get_storage_links(url))),
            len(A) * len(B))
        list(project.get_storage_links(url+'/{c}'))

    def test_create_view_default_url(self):
        project = self.project
        A = ['a_{}'.format(i) for i in range(2)]
        B = ['b_{}'.format(i) for i in range(2)]
        for a in A:
            for b in B:
                p = dict(test_token)
                p.update({'a': a, 'b': b, 'c': 'C'})
                with project.open_job(p) as test_job:
                    test_job.document['result'] = True
        with TemporaryDirectory(prefix = 'comdb_') as tmp:
            project.create_view(prefix=tmp)
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_0/b/b_0')))
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_0/b/b_1')))
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_1/b/b_0')))
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_1/b/b_1')))
            self.assertFalse(os.path.isdir(os.path.join(tmp, 'a/a_0/b/b_0/c/C')))

    def test_create_view_custom_url(self):
        project = self.project
        A = ['a_{}'.format(i) for i in range(2)]
        B = ['b_{}'.format(i) for i in range(2)]
        for a in A:
            for b in B:
                p = dict(test_token)
                p.update({'a': a, 'b': b})
                with project.open_job(p) as test_job:
                    test_job.document['result'] = True
        with TemporaryDirectory(prefix = 'comdb_') as tmp:
            url = os.path.join(tmp,'a/{a}/b/{b}')
            project.create_view(url)

    def test_create_flat_view(self):
        project = self.project
        A = ['a_{}'.format(i) for i in range(2)]
        B = ['b_{}'.format(i) for i in range(2)]
        for a in A:
            for b in B:
                p = dict(test_token)
                p.update({'a': a, 'b': b})
                with project.open_job(p) as test_job:
                    test_job.document['result'] = True
                    with open('testfile_w', 'w') as file:
                        file.write('abc')
                    with test_job.storage.open_file('testfile_s', 'w') as file:
                        file.write('abc')
        with TemporaryDirectory(prefix = 'comdb_') as tmp:
            project.create_flat_view(prefix=tmp)
            for job in project.find_jobs():
                self.assertTrue(os.path.isdir(os.path.join(tmp, 'storage', job.get_id())))
                self.assertTrue(os.path.islink(os.path.join(tmp, 'storage', job.get_id())))
                self.assertTrue(os.path.isdir(os.path.join(tmp, 'workspace', job.get_id())))
                self.assertTrue(os.path.islink(os.path.join(tmp, 'workspace', job.get_id())))
                self.assertTrue(os.path.isfile(os.path.join(tmp, 'storage', job.get_id(), 'testfile_s')))
                self.assertTrue(os.path.isfile(os.path.join(tmp, 'workspace', job.get_id(), 'testfile_w')))

class BaseProjectConsoleTest(unittest.TestCase):
    
    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp_dir = tempfile.TemporaryDirectory(prefix = 'signac_')
        os.chdir(self._tmp_dir.name)
        self.addCleanup(self._tmp_dir.cleanup)
        self.addCleanup(self.return_to_cwd)
        subprocess.check_output(['signac', 'init', '--template', 'testing', 'testing'])

    def return_to_cwd(self):
        os.chdir(self._cwd)

    def tearDown(self):
        subprocess.check_output(['signac', '--yes', 'remove', '--project', '--force'])

class ProjectConsoleTest(BaseProjectConsoleTest):

    def test_create_and_remove(self):    
        pass

    def test_example_scripts(self):
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['python3', 'analyze.py'])

    def test_clear_and_removal(self):
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['signac', '--yes', 'clear'])
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['signac', '--yes', 'remove', '-j', 'all'])

    def test_view(self):
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['signac', 'view'])
        self.assertTrue(os.path.isdir('view/a/0/b/0'))
        self.assertTrue(os.path.isfile('view/a/0/b/0/my_result.txt'))

if __name__ == '__main__':
    unittest.main()
