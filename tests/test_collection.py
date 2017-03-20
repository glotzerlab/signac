import os
import unittest

from signac import Collection
from signac.common import six
if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


class CollectionTest(unittest.TestCase):

    def setUp(self):
        self.c = Collection()

    def test_init(self):
        self.assertEqual(len(self.c), 0)

    def test_insert(self):
        doc = dict(a=0)
        self.c['0'] = doc
        self.assertEqual(len(self.c), 1)
        self.assertEqual(self.c['0'], doc)
        self.assertEqual(list(self.c.find()), [doc])
        with self.assertRaises(ValueError):
            self.c['0'] = dict(_id='1')
        with self.assertRaises(TypeError):
            self.c[0] = dict(a=0)
        with self.assertRaises(TypeError):
            self.c[1.0] = dict(a=0)

    def test_insert_and_remove(self):
        doc = dict(a=0)
        self.c['0'] = doc
        self.assertEqual(len(self.c), 1)
        self.assertEqual(self.c['0'], doc)
        self.assertEqual(list(self.c.find()), [doc])
        del self.c['0']
        self.assertEqual(len(self.c), 0)
        with self.assertRaises(KeyError):
            self.assertEqual(self.c['0'], doc)

    def test_contains(self):
        self.assertFalse('0' in self.c)
        self.c['0'] = dict()
        self.assertTrue('0' in self.c)
        del self.c['0']
        self.assertFalse('0' in self.c)
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        for _id in self.c:
            self.assertTrue(_id in self.c)
        for doc in docs:
            self.assertTrue(doc['_id'] in self.c)

    def test_update(self):
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c), len(docs))

    def test_index(self):
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        with self.assertRaises(KeyError):
            index = self.c.index('a')
        index = self.c.index('a', build=True)
        self.assertEqual(len(index), len(self.c))
        for value, _ids in index.items():
            for _id in _ids:
                self.assertEqual(self.c[_id]['a'], value)
        index = self.c.index('b', build=True)
        del self.c[docs[0]['_id']]
        self.assertEqual(len(self.c), len(docs)-1)
        index = self.c.index('a', build=True)
        self.assertEqual(len(index), len(self.c))
        for value, _ids in index.items():
            for _id in _ids:
                self.assertEqual(self.c[_id]['a'], value)
        self.c[docs[0]['_id']] = docs[0]
        index = self.c.index('a', build=True)
        self.assertEqual(len(index), len(self.c))
        for value, _ids in index.items():
            for _id in _ids:
                self.assertEqual(self.c[_id]['a'], value)
        self.c['0'] = dict(a=-1)
        index = self.c.index('a', build=True)
        self.assertEqual(len(index), len(self.c))
        for value, _ids in index.items():
            for _id in _ids:
                self.assertEqual(self.c[_id]['a'], value)

    def test_clear(self):
        self.assertEqual(len(self.c), 0)
        self.c['0'] = dict(a=0)
        self.assertEqual(len(self.c), 1)
        self.c.clear()
        self.assertEqual(len(self.c), 0)

    def test_iteration(self):
        self.assertEqual(len(self.c), 0)
        self.assertEqual(len(list(self.c.find())), 0)
        docs = self.c['0'] = dict(a=0)
        self.assertEqual(len(self.c), 1)
        self.assertEqual(len(list(self.c.find())), 1)
        self.c.clear()
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c), len(docs))
        self.assertEqual(len(list(self.c.find())), len(docs))
        self.assertEqual(
            {doc['a'] for doc in docs},
            {doc['a'] for doc in self.c.find()})

    def test_find(self):
        self.assertEqual(len(list(self.c.find())), 0)
        self.assertEqual(list(self.c.find()), [])
        self.assertEqual(len(list(self.c.find({'a': 0}))), 0)
        self.assertEqual(list(self.c.find()), [])
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(list(self.c.find())), len(docs))
        self.assertEqual(len(list(self.c.find({'a': 0}))), 1)
        self.assertEqual(list(self.c.find({'a': 0}))[0], docs[0])
        self.assertEqual(len(list(self.c.find({'a': -1}))), 0)
        del self.c[docs[0]['_id']]
        self.assertEqual(len(list(self.c.find({'a': 0}))), 0)

    def test_find_types(self):
        # Note: All of the iterables will be normalized to lists!
        t = [1, 1.0, '1', [1], tuple([1])]
        for i, t in enumerate(t):
            self.c.clear()
            doc = self.c[str(i)] = dict( a=t)
            self.assertEqual(list(self.c.find(doc)), [self.c[str(i)]])

    def test_find_nested(self):
        docs = [dict(a=dict(b=i)) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(list(self.c.find())), len(docs))
        self.assertEqual(len(list(self.c.find({'a.b': 0}))), 1)
        self.assertEqual(len(list(self.c.find({'a': {'b': 0}}))), 1)
        self.assertEqual(list(self.c.find({'a.b': 0}))[0], docs[0])
        del self.c[docs[0]['_id']]
        self.assertEqual(len(list(self.c.find({'a.b': 0}))), 0)
        self.assertEqual(len(list(self.c.find({'a': {'b': 0}}))), 0)

    def test_replace_one(self):
        docs = [dict(a=i) for i in range(10)]
        docs_ = [dict(a=-i) for i in range(10)]
        self.c.update(docs)
        for doc, doc_ in zip(docs, docs_):
            self.c.replace_one(doc, doc_)
        self.assertEqual(len(self.c), len(docs_))
        self.assertEqual(len(list(self.c.find())), len(docs_))
        self.assertEqual(
            set((doc['a'] for doc in docs_)),
            set((doc['a'] for doc in self.c.find())))


class FileCollectionTest(CollectionTest):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_collection_')
        self._fn_collection = os.path.join(self._tmp_dir.name, 'test.txt')
        self.addCleanup(self._tmp_dir.cleanup)
        self.c = Collection.open(self._fn_collection)
        self.addCleanup(self.c.close)

    def test_reopen(self):
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        self.c.flush()
        with Collection.open(self._fn_collection) as c:
            self.assertEqual(len(c), len(self.c))
            for _id in self.c:
                self.assertTrue(_id in c)


if __name__ == '__main__':
    unittest.main()
