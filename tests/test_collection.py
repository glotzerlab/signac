from __future__ import division
import os
import io
import unittest
from collections import OrderedDict
from itertools import islice

from signac import Collection
from signac.common import six
from signac.errors import InvalidKeyError
if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory

n = 42
N = 100

ARITHMETIC_DOCS = [{'a': i} for i in range(N)]

ARITHMETIC_EXPRESSIONS = [
    ({'$eq': n}, 1),
    ({'$ne': n}, N - 1),
    ({'$lt': n}, n),
    ({'$gt': n}, N - n - 1),
    ({'$lte': n}, n + 1),
    ({'$gte': n}, N - n),
]


ARRAY_EXPRESSIONS = [
    ({'$in': []}, 0),
    ({'$in': [0, 1, 2]}, 3),
    ({'$in': ['a', 'b', 'c']}, 0),
    ({'$nin': []}, N),
    ({'$nin': [0, 1, 2]}, N - 3),
    ({'$nin': ['a', 'b', 'c']}, N),
]

LOGICAL_EXPRESSIONS = [
    ({'$and': []}, ValueError),
    ({'a': {'$and': []}}, KeyError),
    ({'a': {'$and': [{'b': 0}]}}, KeyError),
    ({'$and': [{'a': n}]}, 1),
    ({'$and': [{'$not': {'a': n}}]}, N - 1),
    ({'$and': [{'a': n}, {'a': n + 1}]}, 0),
    ({'$and': [{'a': n}, {'$not': {'a': n}}]}, 0),
    ({'$or': []}, ValueError),
    ({'a': {'$or': []}}, KeyError),
    ({'a': {'$or': [{'b': 0}]}}, KeyError),
    ({'$or': [{'$not': {'a': n}}]}, N - 1),
    ({'$or': [{'a': n}, {'a': n + 1}]}, 2),
    ({'$or': [{'a': n}, {'$not': {'a': n}}]}, N),
]


class CollectionTest(unittest.TestCase):

    def setUp(self):
        self.c = Collection()

    def test_init(self):
        self.assertEqual(len(self.c), 0)

    def test_buffer_size(self):
        docs = [{'a': i, '_id': str(i)} for i in range(10)]
        self.c = Collection(docs)
        self.assertEqual(len(self.c._file.getvalue()), 0)
        self.c.flush()
        self.assertGreater(len(self.c._file.getvalue()), 0)

    def test_init_with_list_with_ids_sequential(self):
        docs = [{'a': i, '_id': str(i)} for i in range(10)]
        self.c = Collection(docs)
        self.assertEqual(len(self.c), len(docs))
        for doc in docs:
            self.assertIn(doc['_id'], self.c)

    def test_init_with_list_with_ids_non_sequential(self):
        docs = [{'a': i, '_id': '{:032d}'.format(i**3)} for i in range(10)]
        self.c = Collection(docs)
        self.assertEqual(len(self.c), len(docs))
        for doc in docs:
            self.assertIn(doc['_id'], self.c)

    def test_init_with_list_without_ids(self):
        docs = [{'a': i} for i in range(10)]
        self.c = Collection(docs)
        self.assertEqual(len(self.c), len(docs))
        for doc in docs:
            self.assertIn(doc['_id'], self.c)

    def test_init_with_list_with_and_without_ids(self):
        docs = [{'a': i} for i in range(10)]
        for i, doc in enumerate(islice(docs, 5)):
            doc.setdefault('_id', str(i))
        self.c = Collection(docs)
        self.assertEqual(len(self.c), len(docs))
        for doc in docs:
            self.assertIn(doc['_id'], self.c)

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

    def test_insert_multiple(self):
        doc = dict(a=0)
        self.assertEqual(len(self.c), 0)
        self.c.insert_one(doc.copy())
        self.assertEqual(len(self.c), 1)
        self.c.insert_one(doc.copy())
        self.assertEqual(len(self.c), 2)

    def test_int_float_equality(self):
        self.c.insert_one(dict(a=1))
        self.c.insert_one(dict(a=1.0))
        self.assertEqual(len(self.c.find(dict(a=1))), 1)
        self.assertEqual(len(self.c.find(dict(a=1.0))), 1)
        for doc in self.c.find(dict(a=1)):
            self.assertEqual(type(doc['a']), int)
        for doc in self.c.find(dict(a=1.0)):
            self.assertEqual(type(doc['a']), float)

    def test_copy(self):
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        c2 = Collection(self.c)
        self.assertEqual(len(self.c), len(c2))
        for doc in c2:
            self.assertEqual(len(self.c.find(doc)), 1)

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
        _id = self.c.insert_one(dict())
        self.assertIn(_id, self.c)
        del self.c[_id]
        self.assertFalse(_id in self.c)
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        for _id in self.c.ids:
            self.assertIn(_id, self.c)
        for doc in docs:
            self.assertIn(doc['_id'], self.c)

    def test_update(self):
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c), len(docs))

    def test_update_collision(self):
        docs = [dict(_id=str(i), a=i) for i in range(10)]
        self.c.update(docs)
        # Update the first ten, insert the second ten
        new_docs = [dict(_id=str(i), a=i*2) for i in range(20)]
        self.c.update(new_docs)
        self.assertEqual(len(self.c), len(new_docs))
        self.assertEqual(self.c['0'], new_docs[0])

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
        self.assertEqual(len(self.c), len(docs) - 1)
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

    def test_reindex(self):
        self.assertEqual(len(self.c), 0)
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c), len(docs))
        self.assertEqual(len(self.c.find({'a': 0})), 1)
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c.find({'a': 0})), 2)

    def test_clear(self):
        self.assertEqual(len(self.c), 0)
        self.c['0'] = dict(a=0)
        self.assertEqual(len(self.c), 1)
        self.c.clear()
        self.assertEqual(len(self.c), 0)

    def test_iteration(self):
        self.assertEqual(len(self.c), 0)
        self.assertEqual(len(self.c.find()), 0)
        docs = self.c['0'] = dict(a=0)
        self.assertEqual(len(self.c), 1)
        self.assertEqual(len(self.c.find()), 1)
        self.c.clear()
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c), len(docs))
        self.assertEqual(len(self.c.find()), len(docs))
        self.assertEqual(
            {doc['a'] for doc in docs},
            {doc['a'] for doc in self.c.find()})

    def test_find_integer(self):
        self.assertEqual(len(self.c.find()), 0)
        self.assertEqual(list(self.c.find()), [])
        self.assertEqual(len(self.c.find({'a': 0})), 0)
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c.find()), len(docs))
        self.assertEqual(len(self.c.find({'a': 0})), 1)
        self.assertEqual(len(self.c.find({'a': 0.0})), 0)
        self.assertEqual(list(self.c.find({'a': 0}))[0], docs[0])
        self.assertEqual(len(self.c.find({'a': -1})), 0)
        self.assertEqual(len(self.c.find({'a.b': 0})), 0)
        self.assertEqual(len(self.c.find(limit=5)), 5)
        self.assertEqual(len(self.c.find({'a': {'$type': 'int'}})), 10)
        self.assertEqual(len(self.c.find({'a': {'$type': 'float'}})), 0)
        del self.c[docs[0]['_id']]
        self.assertEqual(len(self.c.find({'a': 0})), 0)

    def test_find_float(self):
        self.assertEqual(len(self.c.find()), 0)
        self.assertEqual(list(self.c.find()), [])
        self.assertEqual(len(self.c.find({'a': 0})), 0)
        docs = [dict(a=float(i)) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c.find()), len(docs))
        self.assertEqual(len(self.c.find({'a': 0})), 0)
        self.assertEqual(len(self.c.find({'a': 0.0})), 1)
        self.assertEqual(list(self.c.find({'a': 0.0}))[0], docs[0])
        self.assertEqual(len(self.c.find({'a': -1})), 0)
        self.assertEqual(len(self.c.find({'a.b': 0})), 0)
        self.assertEqual(len(self.c.find(limit=5)), 5)
        self.assertEqual(len(self.c.find({'a': {'$type': 'int'}})), 0)
        self.assertEqual(len(self.c.find({'a': {'$type': 'float'}})), 10)
        del self.c[docs[0]['_id']]
        self.assertEqual(len(self.c.find({'a': 0})), 0)

    def test_find_int_float(self):
        id_float = self.c.insert_one({'a': float(1.0)})
        id_int = self.c.insert_one({'a': 1})
        self.assertEqual(len(self.c.find({'a': {'$type': 'float'}})), 1)
        self.assertEqual(len(self.c.find({'a': {'$type': 'int'}})), 1)
        self.assertEqual(self.c.find_one({'a': {'$type': 'float'}})['_id'], id_float)
        self.assertEqual(self.c.find_one({'a': {'$type': 'int'}})['_id'], id_int)

        # Reversing order
        self.c.clear()
        id_int = self.c.insert_one({'a': 1})
        id_float = self.c.insert_one({'a': float(1.0)})
        self.assertEqual(len(self.c.find({'a': {'$type': 'int'}})), 1)
        self.assertEqual(len(self.c.find({'a': {'$type': 'float'}})), 1)
        self.assertEqual(self.c.find_one({'a': {'$type': 'int'}})['_id'], id_int)
        self.assertEqual(self.c.find_one({'a': {'$type': 'float'}})['_id'], id_float)

    def test_find_with_dots(self):
        with self.assertRaises(InvalidKeyError):
            self.assertEqual(len(self.c.find()), 0)
            self.assertEqual(list(self.c.find()), [])
            self.assertEqual(len(self.c.find({'a.b': 0})), 0)
            docs = [{'a.b': i} for i in range(10)]
            self.c.update(docs)
            self.assertEqual(len(self.c.find()), len(docs))
            self.assertEqual(len(self.c.find({'a.b': 0})), 1)
            self.assertEqual(list(self.c.find({'a.b': 0}))[0], docs[0])
            self.assertEqual(len(self.c.find({'a.b': -1})), 0)
            docs = [{'a': {'b': i}} for i in range(10)]
            self.c.update(docs)
            self.assertEqual(len(self.c.find()), 2 * len(docs))
            self.assertEqual(len(self.c.find({'a.b': 0})), 2)
            self.assertEqual(len(self.c.find({'a.b': -1})), 0)

    def test_find_types(self):
        # Note: All of the iterables will be normalized to lists!
        t = [1, 1.0, '1', [1], tuple([1])]
        for i, t in enumerate(t):
            self.c.clear()
            doc = self.c[str(i)] = dict(a=t)
            self.assertEqual(list(self.c.find(doc)), [self.c[str(i)]])

    def test_find_one(self):
        self.assertIsNone(self.c.find_one())
        self.c.insert_one(dict())
        self.assertIsNotNone(self.c.find_one())
        self.assertEqual(len(self.c.find()), 1)

    def test_find_nested(self):
        docs = [dict(a=dict(b=i)) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c.find()), len(docs))
        self.assertEqual(len(self.c.find({'a.b': 0})), 1)
        self.assertEqual(len(self.c.find({'a': {'b': 0}})), 1)
        self.assertEqual(list(self.c.find({'a.b': 0}))[0], docs[0])
        del self.c[docs[0]['_id']]
        self.assertEqual(len(self.c.find({'a.b': 0})), 0)
        self.assertEqual(len(self.c.find({'a': {'b': 0}})), 0)

    def test_nested_lists(self):
        docs = [dict(a=[[[i]]]) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c.find()), len(docs))
        self.assertEqual(len(self.c.find({'a': [[[-1]]]})), 0)
        self.assertEqual(len(self.c.find({'a': [[[0]]]})), 1)

    def test_replace_one_simple(self):
        self.assertEqual(len(self.c), 0)
        doc = {'_id': '0', 'a': 0}
        self.c.replace_one({'_id': '0'}, doc, upsert=False)
        self.assertEqual(len(self.c), 0)
        self.c.replace_one({'_id': '0'}, doc, upsert=True)
        self.assertEqual(len(self.c), 1)
        self.c.replace_one({'_id': '0'}, doc, upsert=True)
        self.assertEqual(len(self.c), 1)

    def test_replace_one(self):
        docs = [dict(a=i) for i in range(10)]
        docs_ = [dict(a=-i) for i in range(10)]
        self.c.update(docs)
        for doc, doc_ in zip(docs, docs_):
            self.c.replace_one(doc, doc_)
        self.assertEqual(len(self.c), len(docs_))
        self.assertEqual(len(self.c.find()), len(docs_))
        self.assertEqual(
            set((doc['a'] for doc in docs_)),
            set((doc['a'] for doc in self.c.find())))

    def test_delete(self):
        self.c.delete_many({})
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c), len(docs))
        self.c.delete_many({'a': 0})
        self.assertEqual(len(self.c), len(docs) - 1)
        self.c.delete_many({})
        self.assertEqual(len(self.c), 0)

    def test_delete_one(self):
        self.c.delete_one({})
        docs = [dict(a=i) for i in range(10)]
        self.c.update(docs)
        self.assertEqual(len(self.c), len(docs))
        self.c.delete_one({'a': 0})
        self.assertEqual(len(self.c), len(docs) - 1)
        self.c.delete_one({})
        self.assertEqual(len(self.c), len(docs) - 2)

    def test_find_exists_operator(self):
        self.assertEqual(len(self.c), 0)
        data = OrderedDict((
            ('a', True),
            ('b', 'b'),
            ('c', 0),
            ('d', 0.1),
            ('e', dict(a=0)),
            ('f', dict(a='b')),
            ('g', [0, 'a', True])))

        # Test without data
        for key in data:
            self.assertEqual(len(self.c.find({key: {'$exists': False}})), len(self.c))
            self.assertEqual(len(self.c.find({key: {'$exists': True}})), 0)
            self.assertEqual(len(self.c.find({'{}.$exists'.format(key): False})), len(self.c))
            self.assertEqual(len(self.c.find({'{}.$exists'.format(key): True})), 0)

        # Test for nested cases
        self.assertEqual(len(self.c.find({'e.a.$exists': True})), 0)
        self.assertEqual(len(self.c.find({'e.a.$exists': False})), 0)
        self.assertEqual(len(self.c.find({'e.a': {'$exists': True}})), 0)
        self.assertEqual(len(self.c.find({'e.a': {'$exists': False}})), 0)
        self.assertEqual(len(self.c.find({'f.a.$exists': True})), 0)
        self.assertEqual(len(self.c.find({'f.a.$exists': False})), 0)
        self.assertEqual(len(self.c.find({'f.a': {'$exists': True}})), 0)
        self.assertEqual(len(self.c.find({'f.a': {'$exists': False}})), 0)

        # Test with data
        for key, value in data.items():
            self.c.insert_one({key: value})
        self.c.insert_one({'e': -1})  # heterogeneous nesting

        for key in data:
            n = 2 if key == 'e' else 1
            self.assertEqual(len(self.c.find({key: {'$exists': False}})), len(self.c) - n)
            self.assertEqual(len(self.c.find({key: {'$exists': True}})), n)
            self.assertEqual(len(self.c.find({'{}.$exists'.format(key): False})), len(self.c) - n)
            self.assertEqual(len(self.c.find({'{}.$exists'.format(key): True})), n)

        # Test for nested cases
        self.assertEqual(len(self.c.find({'e.$exists': True})), 2)
        self.assertEqual(len(self.c.find({'e.a.$exists': True})), 1)
        self.assertEqual(len(self.c.find({'e.a.$exists': False})), len(self.c) - 1)
        self.assertEqual(len(self.c.find({'e.a': {'$exists': True}})), 1)
        self.assertEqual(len(self.c.find({'e.a': {'$exists': False}})), len(self.c) - 1)
        self.assertEqual(len(self.c.find({'f.a.$exists': True})), 1)
        self.assertEqual(len(self.c.find({'f.a.$exists': False})), len(self.c) - 1)
        self.assertEqual(len(self.c.find({'f.a': {'$exists': True}})), 1)
        self.assertEqual(len(self.c.find({'f.a': {'$exists': False}})), len(self.c) - 1)

    def test_find_arithmetic_operators(self):
        self.assertEqual(len(self.c), 0)
        for expr, n in ARITHMETIC_EXPRESSIONS:
            self.assertEqual(len(self.c.find({'a': expr})), 0)
        self.c.update(ARITHMETIC_DOCS)
        self.assertEqual(len(self.c), len(ARITHMETIC_DOCS))
        for expr, n in ARITHMETIC_EXPRESSIONS:
            self.assertEqual(len(self.c.find({'a': expr})), n)

    def test_find_near(self):
        self.assertEqual(len(self.c), 0)
        # find 0 items in empty collection
        self.assertEqual(self.c.find({'a': {'$near': [10]}}).count(), 0)
        self.assertEqual(self.c.find({'a': {'$near': [10, 100]}}).count(), 0)
        self.assertEqual(self.c.find({'a': {'$near': [10, 100, 100]}}).count(), 0)
        self.assertEqual(self.c.find({'a': {'$near': (10)}}).count(), 0)
        self.assertEqual(self.c.find({'a': {'$near': (10, 100)}}).count(), 0)
        self.assertEqual(self.c.find({'a': {'$near': (10, 100, 100)}}).count(), 0)
        self.c.update(ARITHMETIC_DOCS)
        self.assertEqual(len(self.c), len(ARITHMETIC_DOCS))
        # test known cases with lists and tuples
        self.assertEqual(self.c.find({'a': {'$near': [10]}}).count(), 1)
        self.assertEqual(self.c.find({'a': {'$near': (10)}}).count(), 1)
        self.assertEqual(self.c.find({'a': {'$near': [10]}}).count(),
                         self.c.find({'a': {'$near': (10)}}).count())
        self.assertEqual(self.c.find({'a': {'$near': [10]}}).count(),
                         self.c.find({'a': {'$near': 10}}).count())
        self.assertEqual(self.c.find({'a': {'$near': [10, 0.5]}}).count(), 16)
        self.assertEqual(self.c.find({'a': {'$near': (10, 0.5)}}).count(), 16)
        self.assertEqual(self.c.find({'a': {'$near': [10, 0.5, 0.0]}}).count(), 16)
        self.assertEqual(self.c.find({'a': {'$near': (10, 0.5, 0.0)}}).count(), 16)
        # increasing abs_tol should increase # of jobs found
        self.assertGreater(self.c.find({'a': {'$near': [10, 0.5, 11]}}).count(),
                           self.c.find({'a': {'$near': [10, 0.5]}}).count())
        self.assertEqual(self.c.find({'a': {'$near': [10.5, 0.005]}}).count(), 0)
        self.assertEqual(self.c.find({'a': {'$near': (10.5, 0.005)}}).count(), 0)
        # test with lists that are too long
        with self.assertRaises(ValueError):
            self.c.find({'a': {'$near': [10, 0.5, 1, 1]}})
        with self.assertRaises(ValueError):
            self.c.find({'a': {'$near': [10, 0.5, 1, 1, 5]}})
        with self.assertRaises(ValueError):
            self.c.find({'a': {'$near': (10, 0.5, 1, 1)}})
        with self.assertRaises(ValueError):
            self.c.find({'a': {'$near': (10, 0.5, 1, 1, 5)}})

    def test_find_array_operators(self):
        self.assertEqual(len(self.c), 0)
        for expr, n in ARRAY_EXPRESSIONS:
            self.assertEqual(len(self.c.find({'a': expr})), 0)
        self.c.update(ARITHMETIC_DOCS)
        self.assertEqual(len(self.c), len(ARITHMETIC_DOCS))
        for expr, n in ARRAY_EXPRESSIONS:
            self.assertEqual(len(self.c.find({'a': expr})), n)

    def test_find_regular_expression(self):
        self.assertEqual(len(self.c), 0)
        self.assertEqual(len(self.c.find({'a': {'$regex': 'foo'}})), 0)
        self.assertEqual(len(self.c.find({'a': {'$regex': 'hello'}})), 0)
        self.c.update([{'a': 'hello world'}])
        self.assertEqual(len(self.c.find({'a': {'$regex': 'foo'}})), 0)
        self.assertEqual(len(self.c.find({'a': {'$regex': 'hello'}})), 1)
        self.assertEqual(len(self.c.find({'a': {'$regex': 'hello world'}})), 1)

    def test_find_type_expression(self):
        self.assertEqual(len(self.c), 0)
        types = [(1, 'int'), (1.0, 'float'), ('1.0', 'str'), (True, 'bool'), (None, 'null')]
        for (v, t) in types:
            self.assertEqual(len(self.c.find({'a': {'$type': t}})), 0)
        for i, (v, t) in enumerate(types):
            self.c.insert_one({str(i): v})
        self.assertEqual(len(self.c), len(types))
        for i, (v, t) in enumerate(types):
            self.assertEqual(len(self.c.find({str(i): {'$type': t}})), 1)

    def test_find_type_integer_values_identical_keys(self):
        self.c.insert_one({'a': 1})
        self.c.insert_one({'a': 1.0})
        self.assertEqual(len(self.c.find({'a': {'$type': 'int'}})), 1)
        self.assertEqual(len(self.c.find({'a': {'$type': 'float'}})), 1)

    def test_find_where_expression(self):
        self.assertEqual(len(self.c), 0)
        self.assertEqual(len(self.c.find({'a': {'$where': 'lambda x: x < 42'}})), 0)
        self.c.update(ARITHMETIC_DOCS)
        self.assertEqual(len(self.c.find({'a': {'$where': 'lambda x: x < 42'}})), 42)

    def test_find_logical_operators(self):
        self.assertEqual(len(self.c), 0)
        for expr, expectation in LOGICAL_EXPRESSIONS:
            if not isinstance(expectation, int):
                with self.assertRaises(expectation):
                    self.c.find(expr)
            else:
                self.assertEqual(len(self.c.find(expr)), 0)
        self.c.update(ARITHMETIC_DOCS)
        self.assertEqual(len(self.c), len(ARITHMETIC_DOCS))
        for expr, expectation in LOGICAL_EXPRESSIONS:
            if not isinstance(expectation, int):
                with self.assertRaises(expectation):
                    self.c.find(expr)
            else:
                self.assertEqual(len(self.c.find(expr)), expectation)
                self.assertEqual(len(self.c.find({'$not': expr})), N - expectation)
                self.assertEqual(len(self.c.find({'$not': {'$not': expr}})), expectation)


class CompressedCollectionTest(CollectionTest):

    def setUp(self):
        self.c = Collection.open(filename=':memory:', compresslevel=9)

    def test_compression(self):
        # Fill with data
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        self.c.flush()

        # Create uncompressed copy to compare to.
        c2 = Collection(self.c)
        c2.flush()

        self.assertIsInstance(self.c._file, io.BytesIO)
        size_compressed = len(self.c._file.getvalue())
        self.assertGreater(size_compressed, 0)
        size_uncompressed = len(c2._file.getvalue().encode('utf-8'))
        self.assertGreater(size_uncompressed, 0)
        compresslevel = size_uncompressed / size_compressed
        self.assertGreater(compresslevel, 1.0)


class FileCollectionTestReadOnly(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_collection_')
        self._fn_collection = os.path.join(self._tmp_dir.name, 'test.txt')
        self.addCleanup(self._tmp_dir.cleanup)
        with Collection.open(self._fn_collection, 'w') as c:
            c.update([dict(_id=str(i)) for i in range(10)])

    def test_read(self):
        c = Collection.open(self._fn_collection, mode='r')
        self.assertEqual(len(list(c)), 10)
        self.assertEqual(len(list(c)), 10)
        self.assertEqual(len(c.find()), 10)
        c.close()

    def test_write_on_readonly(self):
        c = Collection.open(self._fn_collection, mode='r')
        self.assertEqual(len(list(c)), 10)
        c.insert_one(dict())
        self.assertEqual(len(list(c)), 11)
        with self.assertRaises(io.UnsupportedOperation):
            c.flush()
        with self.assertRaises(io.UnsupportedOperation):
            c.close()
        with self.assertRaises(RuntimeError):
            c.find()


class FileCollectionTest(CollectionTest):
    mode = 'w'
    filename = 'test.txt'

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_collection_')
        self._fn_collection = os.path.join(self._tmp_dir.name, self.filename)
        self.addCleanup(self._tmp_dir.cleanup)
        self.c = Collection.open(self._fn_collection, mode=self.mode)
        self.addCleanup(self.c.close)

    def test_write_and_flush(self):
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        self.c.flush()
        self.assertGreater(os.path.getsize(self._fn_collection), 0)

    def test_write_flush_and_reopen(self):
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        self.c.flush()
        self.assertGreater(os.path.getsize(self._fn_collection), 0)

        with Collection.open(self._fn_collection) as c:
            self.assertEqual(len(c), len(docs))
            for doc in self.c:
                self.assertIn(doc['_id'], c)


class BinaryFileCollectionTest(CollectionTest):
    mode = 'wb'


class FileCollectionAppendTest(FileCollectionTest):
    mode = 'a'

    def test_file_size(self):
        docs = [dict(_id=str(i)) for i in range(10)]

        with open(self._fn_collection) as f:
            self.assertEqual(len(list(f)), 0)
        with Collection.open(self._fn_collection) as c:
            c.update(docs)
        with open(self._fn_collection) as f:
            self.assertEqual(len(list(f)), len(docs))
        with Collection.open(self._fn_collection) as c:
            self.assertEqual(len(c), len(docs))
            for doc in docs:
                c.replace_one({'_id': doc['_id']}, doc)
        with Collection.open(self._fn_collection) as c:
            self.assertEqual(len(c), len(docs))
        with open(self._fn_collection) as f:
            self.assertEqual(len(list(f)), len(docs))


class BinaryFileCollectionAppendTest(FileCollectionAppendTest):
    mode = 'ab'


class FileCollectionAppendPlusTest(FileCollectionAppendTest):
    mode = 'a+'


class BinaryFileCollectionAppendPlusTest(FileCollectionAppendTest):
    mode = 'ab+'


class ZippedFileCollectionTest(FileCollectionTest):
    filename = 'test.txt.gz'
    mode = 'wb'

    def test_compression_level(self):
        docs = [dict(_id=str(i)) for i in range(10)]
        self.c.update(docs)
        self.c.flush()
        fn_txt = self._fn_collection + '.txt'
        with Collection.open(fn_txt) as c_text:
            c_text.update(self.c)
        size_txt = os.path.getsize(fn_txt)
        size_gz = os.path.getsize(self._fn_collection)
        self.assertGreater(size_txt, 0)
        self.assertGreater(size_gz, 0)
        compresslevel = size_txt / size_gz
        self.assertGreater(compresslevel, 1.0)


class ZippedFileCollectionAppendTest(ZippedFileCollectionTest):
    filename = 'test.txt.gz'
    mode = 'ab'


class ZippedFileCollectionAppendPlusTest(ZippedFileCollectionAppendTest):
    mode = 'ab'


if __name__ == '__main__':
    unittest.main()
