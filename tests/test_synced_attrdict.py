# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import uuid
from copy import copy, deepcopy
from itertools import chain

from signac.core.attrdict import SyncedAttrDict as SAD
from signac.core.synceddict import _SyncedDict
from signac.common import six
if six.PY2:
    from collections import MutableMapping
else:
    from collections.abc import MutableMapping


class _SyncPoint(object):

    def __init__(self):
        self.reset()

    def reset(self):
        self._loaded = 0
        self._saved = 0

    @property
    def loaded(self):
        ret = self._loaded
        self._loaded = 0
        return ret

    @property
    def saved(self):
        ret = self._saved
        self._saved = 0
        return ret

    def load(self):
        self._loaded += 1

    def save(self):
        self._saved += 1


class SyncedAttrDictTest(unittest.TestCase):

    def get_testdata(self):
        return str(uuid.uuid4())

    def get_sad(self, initialdata=None):
        self.sync_point = _SyncPoint()
        return SAD(initialdata, parent=self.sync_point)

    def assert_no_read_write(self):
        self.assertEqual(self.sync_point.loaded, 0)
        self.assertEqual(self.sync_point.saved, 0)

    def assert_only_read(self, num=1):
        self.assertEqual(self.sync_point.loaded, num)
        self.assertEqual(self.sync_point.saved, 0)

    def assert_only_write(self, num=1):
        self.assertEqual(self.sync_point.loaded, 0)
        self.assertEqual(self.sync_point.saved, num)

    def assert_read_write(self, num_read=1, num_write=1):
        self.assertEqual(self.sync_point.loaded, num_read)
        self.assertEqual(self.sync_point.saved, num_write)

    def test_init(self):
        SAD()
        SAD(dict(a=0))
        self.get_sad()

    def test_is_object_and_mapping(self):
        self.assertTrue(isinstance(_SyncedDict(), object))
        self.assertTrue(isinstance(_SyncedDict(), MutableMapping))
        self.assertTrue(isinstance(self.get_sad(), _SyncedDict))

    def test_str(self):
        sad = self.get_sad()
        self.assertEqual(str(sad), str(dict(sad())))
        sad['a'] = 0
        self.assertEqual(str(sad), str(dict(sad())))
        sad['a'] = {'b': 0}
        self.assertEqual(str(sad), str(dict(sad())))

    def test_repr(self):
        sad = self.get_sad()
        repr(sad)

    def test_call(self):
        sad = self.get_sad()
        sad['a'] = 0
        self.assertEqual(sad, dict(a=0))
        self.assertEqual(sad(), dict(a=0))

    def test_set_get(self):
        sad = self.get_sad()
        key = 'setget'
        d = self.get_testdata()
        self.assertFalse(bool(sad))
        self.assertEqual(len(sad), 0)
        self.assertNotIn(key, sad)
        self.assertFalse(key in sad)
        sad[key] = d
        self.assertTrue(bool(sad))
        self.assertEqual(len(sad), 1)
        self.assertIn(key, sad)
        self.assertTrue(key in sad)
        self.assertEqual(sad[key], d)
        self.assertEqual(sad.get(key), d)

    def test_set_get_explicit_nested(self):
        sad = self.get_sad()
        key = 'setgetexplicitnested'
        d = self.get_testdata()
        self.assertFalse(bool(sad))
        self.assertEqual(len(sad), 0)
        self.assertNotIn(key, sad)
        self.assertFalse(key in sad)
        sad.setdefault('a', dict())
        child1 = sad['a']
        child2 = sad['a']
        self.assertFalse(child1)
        self.assertFalse(child2)
        child1[key] = d
        self.assertTrue(child1)
        self.assertTrue(child2)
        self.assertIn(key, child1)
        self.assertIn(key, child2)
        self.assertEqual(child1, child2)
        self.assertEqual(child1[key], d)
        self.assertEqual(child2[key], d)

    def test_copy_value(self):
        sad = self.get_sad()
        key = 'copy_value'
        key2 = 'copy_value2'
        d = self.get_testdata()
        self.assertNotIn(key, sad)
        self.assertNotIn(key2, sad)
        sad[key] = d
        self.assertIn(key, sad)
        self.assertEqual(sad[key], d)
        self.assertNotIn(key2, sad)
        sad[key2] = sad[key]
        self.assertIn(key, sad)
        self.assertEqual(sad[key], d)
        self.assertIn(key2, sad)
        self.assertEqual(sad[key2], d)

    def test_copy_as_dict_sync(self):
        sad = self.get_sad()
        self.assert_no_read_write()
        sad['a'] = {'b': 0}
        self.assert_only_write()
        sad2 = copy(sad)
        sad3 = deepcopy(sad)
        self.assertEqual(sad, sad2)
        self.assert_only_read(2)
        self.assertEqual(sad, sad3)
        self.assert_only_read(1)
        sad.a.b = 1
        self.assert_read_write(1, 1)
        self.assertEqual(sad.a.b, 1)
        self.assert_only_read(2)
        self.assertEqual(sad2.a.b, 1)
        self.assert_only_read(2)
        self.assertEqual(sad3.a.b, 0)
        self.assert_only_read(0)

    def test_iter(self):
        sad = self.get_sad()
        key1 = 'iter1'
        key2 = 'iter2'
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = {key1: d1, key2: d2}
        sad.update(d)
        self.assertIn(key1, sad)
        self.assertIn(key2, sad)
        for i, key in enumerate(sad):
            self.assertIn(key, d)
            self.assertEqual(d[key], sad[key])
        self.assertEqual(i, 1)

    def test_delete(self):
        sad = self.get_sad()
        key = 'delete'
        d = self.get_testdata()
        sad[key] = d
        self.assertEqual(len(sad), 1)
        self.assertEqual(sad[key], d)
        del sad[key]
        self.assertEqual(len(sad), 0)
        with self.assertRaises(KeyError):
            sad[key]

    def test_update(self):
        sad = self.get_sad()
        key = 'update'
        d = {key: self.get_testdata()}
        sad.update(d)
        self.assertEqual(len(sad), 1)
        self.assertEqual(sad[key], d[key])

    def test_clear(self):
        sad = self.get_sad()
        key = 'clear'
        d = self.get_testdata()
        sad[key] = d
        self.assertEqual(len(sad), 1)
        self.assertEqual(sad[key], d)
        sad.clear()
        self.assertEqual(len(sad), 0)

    def test_copy_as_dict(self):
        sad = self.get_sad()
        key = 'copy'
        d = self.get_testdata()
        sad[key] = d
        copy = dict(sad)
        self.assertEqual(copy, sad)
        self.assertEqual(copy, sad())
        del sad
        self.assertTrue(key in copy)
        self.assertEqual(copy[key], d)

    def test_set_get_sync(self):
        sad = self.get_sad()
        self.assert_no_read_write()
        key = 'setget'
        d = self.get_testdata()
        self.assertFalse(bool(sad))
        self.assert_only_read()
        self.assertEqual(len(sad), 0)
        self.assert_only_read()
        self.assertNotIn(key, sad)
        self.assert_only_read()
        self.assertFalse(key in sad)
        self.assert_only_read()
        sad[key] = d
        self.assert_only_write()

    def test_iter_sync(self):
        sad = self.get_sad()
        self.assert_no_read_write()
        key1 = 'iter1'
        key2 = 'iter2'
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = {key1: d1, key2: d2}
        sad.update(d)
        self.assert_read_write()
        self.assertIn(key1, sad)
        self.assert_only_read()
        self.assertIn(key2, sad)
        self.assert_only_read()
        for i, key in enumerate(sad):
            self.assertIn(key, d)
            self.assertEqual(d[key], sad[key])
        self.assertEqual(i, 1)
        self.assert_only_read(3)

    def test_delete_sync(self):
        sad = self.get_sad()
        key = 'delete'
        d = self.get_testdata()
        sad[key] = d
        self.assert_only_write()
        self.assertEqual(len(sad), 1)
        self.assert_only_read()
        self.assertEqual(sad[key], d)
        self.assert_only_read()
        del sad[key]
        self.assert_read_write()
        self.assertEqual(len(sad), 0)
        self.assert_only_read()
        with self.assertRaises(KeyError):
            sad[key]
            self.assert_only_read()

    def test_update_sync(self):
        sad = self.get_sad()
        key = 'update'
        d = {key: self.get_testdata()}
        sad.update(d)
        self.assert_read_write()
        self.assertEqual(len(sad), 1)
        self.assert_only_read()
        self.assertEqual(sad[key], d[key])
        self.assert_only_read()

    def test_clear_sync(self):
        sad = self.get_sad()
        key = 'clear'
        d = self.get_testdata()
        sad[key] = d
        self.assert_only_write()
        sad.clear()
        self.assert_only_write()
        self.assertEqual(len(sad), 0)
        self.assert_only_read()

    def test_copy(self):
        sad = self.get_sad()
        key = 'copy'
        d = self.get_testdata()
        sad[key] = d
        self.assert_only_write()
        copy = dict(sad)
        self.assert_only_read(2)
        self.assertEqual(copy, sad)
        self.assert_only_read()
        self.assertEqual(copy, sad())
        self.assert_only_read()
        del sad
        self.assertTrue(key in copy)
        self.assertEqual(copy[key], d)

    def test_set_get_attr_sync(self):
        sad = self.get_sad()
        self.assertEqual(len(sad), 0)
        self.assert_only_read()
        self.assertNotIn('a', sad)
        self.assert_only_read()
        with self.assertRaises(KeyError):
            sad.a
        self.assert_only_read()
        a = 0
        sad.a = a
        self.assert_only_write()
        self.assertEqual(len(sad), 1)
        self.assert_only_read()
        self.assertIn('a', sad)
        self.assert_only_read()
        self.assertEqual(sad.a, a)
        self.assert_only_read()
        self.assertEqual(sad['a'], a)
        self.assert_only_read()
        self.assertEqual(sad()['a'], a)
        self.assert_only_read()
        a = 1
        sad.a = a
        self.assert_only_write()
        self.assertEqual(len(sad), 1)
        self.assert_only_read()
        self.assertIn('a', sad)
        self.assert_only_read()
        self.assertEqual(sad.a, a)
        self.assert_only_read()
        self.assertEqual(sad['a'], a)
        self.assert_only_read()
        self.assertEqual(sad()['a'], a)
        self.assert_only_read()

        def check_nested(a, b):
            self.assertEqual(len(sad), 1)
            self.assert_only_read()
            self.assertEqual(len(sad.a), 1)
            self.assert_only_read(2)
            self.assertIn('a', sad)
            self.assert_only_read()
            self.assertIn('b', sad.a)
            self.assert_only_read(2)
            self.assertEqual(sad.a, a)
            self.assert_only_read(2)
            self.assertEqual(sad['a']['b'], b)
            self.assert_only_read(2)
            self.assertEqual(sad.a.b, b)
            self.assert_only_read(2)
            self.assertEqual(sad.a(), a)
            self.assert_only_read(2)
            self.assertEqual(sad['a'], a)
            self.assert_only_read(2)
            self.assertEqual(sad()['a'], a)
            self.assert_only_read(1)
            self.assertEqual(sad()['a']['b'], b)
            self.assert_only_read(1)
            self.assertEqual(sad['a']()['b'], b)
            self.assert_only_read(2)

        sad.a = {'b': 0}
        self.assert_only_write()
        check_nested({'b': 0}, 0)
        sad.a.b = 1
        self.assert_read_write()
        check_nested({'b': 1}, 1)
        sad['a'] = {'b': 2}
        self.assert_only_write()
        check_nested({'b': 2}, 2)
        sad['a']['b'] = 3
        self.assert_read_write()
        check_nested({'b': 3}, 3)

    def test_attr_reference_modification(self):
        sad = self.get_sad()
        self.assertEqual(len(sad), 0)
        self.assertNotIn('a', sad)
        with self.assertRaises(KeyError):
            sad.a
        pairs = [(0, 1), (0.0, 1.0), ('0', '1'), (False, True)]
        dict_pairs = [(dict(c=a), dict(c=b)) for a, b in pairs]
        for A, B in chain(pairs, dict_pairs):
            sad.a = A
            a = sad.a
            self.assertEqual(a, A)
            self.assertEqual(sad.a, A)
            a = B
            self.assertEqual(a, B)
            self.assertEqual(sad.a, A)
            a = sad['a']
            self.assertEqual(a, A)
            self.assertEqual(sad.a, A)
            a = B
            self.assertEqual(a, B)
            self.assertEqual(sad.a, A)

            # with nested values
            sad['a'] = dict(b=A)
            self.assertEqual(sad.a.b, A)
            b = sad.a.b
            self.assertEqual(b, A)
            self.assertEqual(sad.a.b, A)
            b = B
            self.assertEqual(b, B)
            self.assertEqual(sad.a.b, A)
            b = sad['a']['b']
            self.assertEqual(b, A)
            self.assertEqual(sad.a.b, A)
            b = B
            self.assertEqual(b, B)
            self.assertEqual(sad.a.b, A)
            b = sad['a'].b
            self.assertEqual(b, A)
            self.assertEqual(sad.a.b, A)
            b = B
            self.assertEqual(b, B)
            self.assertEqual(sad.a.b, A)

    def test_suspend_sync(self):
        sad = self.get_sad()
        self.assertEqual(len(sad), 0)
        self.assert_only_read()
        with sad._suspend_sync():
            sad['a'] = 0
            self.assertEqual(len(sad), 1)
        self.assert_only_read(0)
        self.assertEqual(len(sad), 1)
        self.assert_only_read()
        with sad._suspend_sync():
            with sad._suspend_sync():
                sad['a'] = 1
                self.assertEqual(len(sad), 1)
        self.assert_only_read(0)
        self.assertEqual(len(sad), 1)
        self.assert_only_read()


if __name__ == '__main__':
    unittest.main()
