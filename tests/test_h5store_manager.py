# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import unittest
import pickle

from signac.core.h5store import H5StoreManager
from signac.common import six
if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


class TestH5StoreManager(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='h5store_')
        self.addCleanup(self._tmp_dir.cleanup)
        self.store = H5StoreManager(prefix=self._tmp_dir.name)
        with open(os.path.join(self._tmp_dir.name, 'other_file.txt'), 'w') as file:
            file.write(r'blank\n')

    def test_repr(self):
        self.assertEqual(eval(repr(self.store)), self.store)

    def test_str(self):
        self.assertEqual(eval(str(self.store)), self.store)

    def test_set(self):
        self.assertEqual(len(self.store), 0)
        self.assertNotIn('test', self.store)
        for value in ('', [], {}):
            with self.assertRaises(ValueError):
                self.store['test'] = value
        for value in (True, 0, 0.0, 1, 1.0, None):
            with self.assertRaises(TypeError):
                self.store['test'] = value
        for value in ('abc'):
            with self.assertRaises(ValueError):
                self.store['test'] = value

        # Assigning a dictionary is the intended use case
        self.store['test'] = dict(foo=True)
        self.assertEqual(len(self.store), 1)
        self.assertIn('test', self.store)

    def test_set_iterable(self):
        self.assertEqual(len(self.store), 0)
        self.assertNotIn('test', self.store)
        self.store['test'] = list(dict(foo=True).items())
        self.assertEqual(len(self.store), 1)
        self.assertIn('test', self.store)

    def test_set_get(self):
        self.assertEqual(len(self.store), 0)
        self.assertNotIn('test', self.store)
        self.store['test']['foo'] = 'bar'
        self.assertIn('test', self.store)
        self.assertEqual(len(self.store), 1)
        self.assertIn('foo', self.store['test'])

    def test_del(self):
        self.assertEqual(len(self.store), 0)
        self.assertNotIn('test', self.store)
        self.store['test']['foo'] = 'bar'
        self.assertIn('test', self.store)
        self.assertEqual(len(self.store), 1)
        self.assertIn('foo', self.store['test'])
        with self.assertRaises(KeyError):
            del self.store['invalid']
        del self.store['test']
        self.assertEqual(len(self.store), 0)
        self.assertNotIn('test', self.store)

    def test_iteration(self):
        keys = ['foo', 'bar', 'baz']
        for key in keys:
            self.store[key] = dict(test=True)
        self.assertEqual(list(sorted(keys)), list(sorted(self.store)))
        self.assertEqual(list(sorted(keys)), list(sorted(self.store.keys())))

    def test_contains(self):
        keys = ['foo', 'bar', 'baz']
        for key in keys:
            self.assertNotIn(key, self.store)
        for key in keys:
            self.store[key] = dict(test=True)
        for key in keys:
            self.assertIn(key, self.store)

    def test_pickle(self):
        self.assertEqual(pickle.loads(pickle.dumps(self.store)), self.store)


if __name__ == '__main__':
    unittest.main()
