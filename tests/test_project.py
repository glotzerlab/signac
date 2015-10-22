import unittest
import os
import uuid
import warnings
from tempfile import TemporaryDirectory

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


@unittest.skip("Views are currently not implemented.")
class ProjectViewTest(BaseProjectTest):

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
        list(project.get_storage_links(url + '/{c}'))

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
        with TemporaryDirectory(prefix='comdb_') as tmp:
            project.create_view(prefix=tmp)
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_0/b/b_0')))
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_0/b/b_1')))
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_1/b/b_0')))
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'a/a_1/b/b_1')))
            self.assertFalse(os.path.isdir(
                os.path.join(tmp, 'a/a_0/b/b_0/c/C')))

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
        with TemporaryDirectory(prefix='comdb_') as tmp:
            url = os.path.join(tmp, 'a/{a}/b/{b}')
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
        with TemporaryDirectory(prefix='comdb_') as tmp:
            project.create_flat_view(prefix=tmp)
            for job in project.find_jobs():
                self.assertTrue(os.path.isdir(
                    os.path.join(tmp, 'storage', job.get_id())))
                self.assertTrue(os.path.islink(
                    os.path.join(tmp, 'storage', job.get_id())))
                self.assertTrue(os.path.isdir(
                    os.path.join(tmp, 'workspace', job.get_id())))
                self.assertTrue(os.path.islink(
                    os.path.join(tmp, 'workspace', job.get_id())))
                self.assertTrue(os.path.isfile(os.path.join(
                    tmp, 'storage', job.get_id(), 'testfile_s')))
                self.assertTrue(os.path.isfile(os.path.join(
                    tmp, 'workspace', job.get_id(), 'testfile_w')))

if __name__ == '__main__':
    unittest.main()
