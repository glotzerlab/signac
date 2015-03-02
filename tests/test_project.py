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
        project.create_db_snapshot(fn_tmp)
        remove(fn_tmp)

    def test_create_and_restore_db_snapshot(self):
        from os import remove
        from compdb.contrib import get_project
        from tempfile import TemporaryFile
        project = get_project()
        with project.open_job('test_create_snapshot', test_token) as job:
            job.document['result'] = 123
        fn_tmp = '_dump.tar'
        project.create_db_snapshot(fn_tmp)
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

if __name__ == '__main__':
    unittest.main()
