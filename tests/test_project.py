import unittest
from contextlib import contextmanager

# Make sure the jobs created for this test are unique.
import uuid
test_token = {'test_token': str(uuid.uuid4())}

from test_job import safe_open_job, JobTest

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
