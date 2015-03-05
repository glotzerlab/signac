import unittest
from contextlib import contextmanager

# Make sure the jobs created for this test are unique.
import uuid
test_token = {'test_token': str(uuid.uuid4())}

from test_job import safe_open_job, JobTest

class ProjectBackupTest(JobTest):
    
    def test_dump_db_snapshot(self):
        from compdb.contrib import get_project
        project = get_project()
        with project.open_job('test_dump', test_token) as job:
            job.document['result'] = 123
        project.dump_db_snapshot()

    def test_create_db_snapshot(self):
        from compdb.contrib import get_project
        from os import remove
        project = get_project()
        with project.open_job('test_create_snapshot', test_token) as job:
            job.document['result'] = 123
        fn_tmp = '_dump.tar'
        project.create_snapshot(fn_tmp, full = False)
        remove(fn_tmp)

    def test_create_and_restore_db_snapshot(self):
        from os import remove
        from compdb.contrib import get_project
        from tempfile import TemporaryFile
        project = get_project()
        with project.open_job('test_create_snapshot', test_token) as job:
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
            with project.open_job('test_full_restore', state) as job:
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
        with project.open_job('test_create_snapshot', test_token) as job:
            job.document['result'] = 123
        fn_tmp = '_dump.tar'
        self.assertRaises(FileNotFoundError, project.restore_snapshot, '_bullshit.tar')

class ProjectViewTest(JobTest):
    
    def test_get_links(self):
        from compdb.contrib import get_project
        project = get_project()
        A = ['a_{}'.format(i) for i in range(2)]
        B = ['b_{}'.format(i) for i in range(2)]
        for a in A:
            for b in B:
                p = dict(test_token)
                p.update({'a': a, 'b': b})
                with project.open_job('test_views', p) as test_job:
                    test_job.document['result'] = True
        url = 'view/a/{a}/b/{b}'
        self.assertEqual(len(list(project._get_links(url, parameters=['a','b']))), len(A) * len(B))
        def check_invalid_url():
            list(project._get_links(url+'/{c}', parameters = ['a', 'b']))
        self.assertRaises(KeyError, check_invalid_url)

    def test_create_view(self):
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
                with project.open_job('test_views', p) as test_job:
                    test_job.document['result'] = True
        with TemporaryDirectory(prefix = 'comdb_') as tmp:
            url = os.path.join(tmp,'a/{a}/b/{b}')
            project.create_view(url)

def open_pool(state_points, rank, condition = None):
    from compdb.contrib import get_project
    project = get_project()
    with project.job_pool(state_points, condition) as pool:
        try:
            job = pool.open_job('myjob', rank)
            if job.parameters()['a'] == 0:
                job.document['check'] = True
        except IndexError:
            raise
            return False
    return True

class ProjectPoolTest(JobTest):
    
    def test_enter(self):
        from compdb.contrib import get_project
        project = get_project()
        state_points = [{'a': a, 'b': b} for a in range(3) for b in range(3)]
        with project.job_pool(state_points) as pool:
            pass

    def test_pool_concurrency(self):
        from multiprocessing import Pool
        state_points = [{'a': a, 'b': b} for a in range(3) for b in range(3)]
        num_processes = min(len(state_points), 4)
        with Pool(processes = num_processes) as pool:
            result = pool.starmap_async(
                open_pool, [(state_points, rank) for rank in range(len(state_points))])
            result = result.get(timeout = 20)
            self.assertEqual(result, [True] * len(state_points))

    def test_pool_condition(self):
        from multiprocessing import Pool
        from compdb.contrib import get_project
        project = get_project()
        state_points = [{'a': a, 'b': b} for a in range(4) for b in range(4)]
        condition = {'check': True}
        pool = project.job_pool(state_points, condition)
        self.assertEqual(len(pool), len(state_points))

    def test_pool_concurrency_with_condition(self):
        from multiprocessing import Pool
        from compdb.contrib import get_project
        project = get_project()
        state_points = [{'a': a, 'b': b} for a in range(3) for b in range(3)]
        condition = {'check': True}
        job_pool = project.job_pool(state_points, condition)
        pool_len = len(job_pool)
        self.assertEqual(len(job_pool), len(state_points))
        num_processes = min(len(job_pool), 4)
        with Pool(processes = num_processes) as pool:
            result = pool.starmap_async(
                open_pool, [(state_points, rank, condition) 
                    for rank in range(len(job_pool))])
            result = result.get(timeout = 20)
            self.assertEqual(result, [True] * pool_len)

            job_pool = project.job_pool(state_points, condition)
            pool_len = len(job_pool)
            result = pool.starmap_async(
                open_pool, [(state_points, rank, condition) 
                    for rank in range(len(job_pool))])
            result = result.get(timeout = 20)
            self.assertEqual(result, [True] * pool_len)

if __name__ == '__main__':
    unittest.main()
