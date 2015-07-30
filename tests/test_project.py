import unittest
import os
import sys
import io
import tempfile
import subprocess
from contextlib import contextmanager

# Make sure the jobs created for this test are unique.
import uuid
test_token = {'test_token': str(uuid.uuid4())}

import warnings
warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='compdb')

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

from test_job import JobTest

@unittest.skipIf(not PYMONGO_3, 'test requires pymongo version >= 3.0.x')
class ProjectTest(JobTest):
    pass

class ProjectBackupTest(ProjectTest):
    
    def test_dump_db_snapshot(self):
        from compdb.contrib import get_project
        project = get_project()
        with project.open_job(test_token) as job:
            job.document['result'] = 123
        self.stdout = sys.stdout
        sys.stdout=io.StringIO()
        try:
            project.dump_db_snapshot()
        finally:
            sys.stdout=self.stdout

    def test_create_db_snapshot(self):
        from compdb.contrib import get_project
        from os import remove
        project = get_project()
        with project.open_job(test_token) as job:
            job.document['result'] = 123
        fn_tmp = '_dump.tar'
        project.create_snapshot(fn_tmp, full = False)
        remove(fn_tmp)

    def test_create_and_restore_db_snapshot(self):
        from os import remove
        from compdb.contrib import get_project
        from tempfile import TemporaryFile
        project = get_project()
        with project.open_job(test_token) as job:
            job.document['result'] = 123
        fn_tmp = '_dump.tar'
        project.create_snapshot(fn_tmp, full = False)
        project.restore_snapshot(fn_tmp)
        remove(fn_tmp)

    def test_create_and_restore_snapshot(self):
        from os import remove
        from compdb.contrib import get_project
        from tempfile import TemporaryFile
        project = get_project()
        A = ['a_{}'.format(i) for i in range(2)]
        B = ['b_{}'.format(i) for i in range(2)]
        def states():
            for a in A:
                for b in B:
                    p = dict(test_token)
                    p.update({'a': a, 'b': b})
                    yield p
        for state in states():
            with project.open_job(state) as job:
                job.document['result'] = 123
                with job.storage.open_file('result.txt', 'wb') as file:
                    file.write('123'.encode())
        fn_tmp = '_full_dump.tar'
        project.create_snapshot(fn_tmp)
        project.restore_snapshot(fn_tmp)
        remove(fn_tmp)

    def test_bad_restore(self):
        from compdb.contrib import get_project
        from tempfile import TemporaryFile
        project = get_project()
        with project.open_job(test_token) as job:
            job.document['result'] = 123
        fn_tmp = '_dump.tar'
        self.assertRaises(FileNotFoundError, project.restore_snapshot, '_bullshit.tar')

class ProjectViewTest(ProjectTest):
    
    def test_get_links(self):
        from compdb.contrib import get_project
        project = get_project()
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
        import os
        from compdb.contrib import get_project
        from tempfile import TemporaryDirectory
        project = get_project()
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
        import os
        from compdb.contrib import get_project
        from tempfile import TemporaryDirectory
        project = get_project()
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
        import os
        from compdb.contrib import get_project
        from tempfile import TemporaryDirectory
        project = get_project()
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

def set_check_true(job):
    with job:
        job.document['check'] = True

def start_pool(state_points, exclude_condition, rank, size, jobs = []):
    import tempfile
    from compdb.contrib import get_project
    project = get_project()
    job_pool = project.job_pool(state_points, exclude_condition)
    for job in jobs:
        job_pool.submit(job)
    with tempfile.NamedTemporaryFile() as jobfile:
        job_pool.start(rank, size, jobfile = jobfile.name)

class ProjectPoolTest(ProjectTest):
    
    def test_start(self):
        from compdb.contrib import get_project
        project = get_project()
        state_points = [{'a': a, 'b': b} for a in range(3) for b in range(3)]
        def dummy_function(job):
            pass

        pool = project.job_pool(state_points)
        pool.submit(dummy_function)
        try:
            pool.start()
        except EnvironmentError:
            import warnings
            msg = "requires mpi4py"
            raise unittest.SkipTest(msg)

    def test_pool_concurrency(self):
        from multiprocessing import Pool
        from compdb.contrib import get_project
        project = get_project()
        state_points = [{'a': a, 'b': b} for a in range(3) for b in range(3)]
        job_pool = project.job_pool(state_points)
        num_proc = min(len(state_points), 4)
        jobs = []
        condition = {}
        with Pool(processes = num_proc) as pool:
            result = pool.starmap_async(
                start_pool,
                [(state_points, condition, rank, num_proc, jobs)
                    for rank in range(num_proc)])
            result.get(timeout = 20)

    def test_pool_condition(self):
        from multiprocessing import Pool
        from compdb.contrib import get_project
        project = get_project()
        state_points = [{'a': a, 'b': b} for a in range(4) for b in range(4)]
        condition = {'check': True}
        pool = project.job_pool(state_points, exclude = condition)
        self.assertEqual(len(pool), len(state_points))

    def test_pool_concurrency_with_condition(self):
        from multiprocessing import Pool
        from compdb.contrib import get_project
        project = get_project()
        state_points = [{'a': a, 'b': b} for a in range(3) for b in range(3)]
        condition = {'check': True}
        job_pool = project.job_pool(state_points, exclude = condition)
        self.assertEqual(len(job_pool), len(state_points))
        num_proc = min(len(job_pool), 4)
        jobs = [set_check_true]
        with Pool(processes = num_proc) as pool:
            result = pool.starmap_async(
                start_pool,
                [(state_points, condition, rank, num_proc, jobs)
                    for rank in range(num_proc)])
            result.get(timeout = 20)
        job_pool = project.job_pool(state_points, condition)
        self.assertEqual(len(job_pool), 0)

def simple_function(x):
    return x*x

class ProjectQueueTest(ProjectTest):

    def test_queue(self):
        from compdb.contrib import get_project
        from compdb.contrib.project import Empty
        project = get_project()
        queue = project.job_queue
        num_jobs = 10
        futures = [queue.submit(simple_function, i) for i in range(num_jobs)]
        try:
            queue.enter_loop(timeout = 0.1)
        except Empty:
            pass
        for i, future in enumerate(futures):
            result = future.result(0.1)
            self.assertEqual(result, simple_function(i))

class BaseProjectConsoleTest(unittest.TestCase):
    
    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp_dir = tempfile.TemporaryDirectory(prefix = 'compdb_')
        os.chdir(self._tmp_dir.name)
        self.addCleanup(self._tmp_dir.cleanup)
        self.addCleanup(self.return_to_cwd)
        subprocess.check_output(['compdb', 'init', '--template', 'testing', 'testing'])

    def return_to_cwd(self):
        os.chdir(self._cwd)

    def tearDown(self):
        subprocess.check_output(['compdb', '--yes', 'remove', '--project', '--force'])

class ProjectConsoleTest(BaseProjectConsoleTest):

    def test_create_and_remove(self):    
        pass

    def test_example_scripts(self):
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['python3', 'analyze.py'])

    def test_clear_and_removal(self):
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['compdb', '--yes', 'clear'])
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['compdb', '--yes', 'remove', '-j', 'all'])

    def test_snapshot_and_restore(self):
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['compdb', 'snapshot', 'test.tar'])
        subprocess.check_output(['compdb', 'restore', 'test.tar'])
        subprocess.check_output(['compdb', 'snapshot', 'test.tar.gz'])
        subprocess.check_output(['compdb', 'restore', 'test.tar.gz'])

    def test_view(self):
        subprocess.check_output(['python3', 'job.py'])
        subprocess.check_output(['compdb', 'view'])
        self.assertTrue(os.path.isdir('view/a/0/b/0'))
        self.assertTrue(os.path.isfile('view/a/0/b/0/my_result.txt'))

if __name__ == '__main__':
    unittest.main()
