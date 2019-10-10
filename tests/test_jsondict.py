# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import unittest
import uuid
from tempfile import TemporaryDirectory

from signac.core.jsondict import JSONDict
from signac.errors import InvalidKeyError
from signac.errors import KeyTypeError


FN_DICT = 'jsondict.json'


def testdata():
    return str(uuid.uuid4())


class BaseJSONDictTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        self._fn_dict = os.path.join(self._tmp_dir.name, FN_DICT)
        self.addCleanup(self._tmp_dir.cleanup)


class JSONDictTest(BaseJSONDictTest):

    def get_json_dict(self):
        return JSONDict(filename=self._fn_dict)

    def get_testdata(self):
        return str(uuid.uuid4())

    def test_init(self):
        self.get_json_dict()

    def test_set_get(self):
        jsd = self.get_json_dict()
        key = 'setget'
        d = self.get_testdata()
        jsd.clear()
        self.assertFalse(bool(jsd))
        self.assertEqual(len(jsd), 0)
        self.assertNotIn(key, jsd)
        self.assertFalse(key in jsd)
        jsd[key] = d
        self.assertTrue(bool(jsd))
        self.assertEqual(len(jsd), 1)
        self.assertIn(key, jsd)
        self.assertTrue(key in jsd)
        self.assertEqual(jsd[key], d)
        self.assertEqual(jsd.get(key), d)

    def test_set_get_explicit_nested(self):
        jsd = self.get_json_dict()
        key = 'setgetexplicitnested'
        d = self.get_testdata()
        jsd.setdefault('a', dict())
        child1 = jsd['a']
        child2 = jsd['a']
        self.assertEqual(child1, child2)
        self.assertEqual(type(child1), type(child2))
        self.assertEqual(child1._parent, child2._parent)
        self.assertEqual(id(child1._parent), id(child2._parent))
        self.assertEqual(id(child1), id(child2))
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
        jsd = self.get_json_dict()
        key = 'copy_value'
        key2 = 'copy_value2'
        d = self.get_testdata()
        self.assertNotIn(key, jsd)
        self.assertNotIn(key2, jsd)
        jsd[key] = d
        self.assertIn(key, jsd)
        self.assertEqual(jsd[key], d)
        self.assertNotIn(key2, jsd)
        jsd[key2] = jsd[key]
        self.assertIn(key, jsd)
        self.assertEqual(jsd[key], d)
        self.assertIn(key2, jsd)
        self.assertEqual(jsd[key2], d)

    def test_iter(self):
        jsd = self.get_json_dict()
        key1 = 'iter1'
        key2 = 'iter2'
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = {key1: d1, key2: d2}
        jsd.update(d)
        self.assertIn(key1, jsd)
        self.assertIn(key2, jsd)
        for i, key in enumerate(jsd):
            self.assertIn(key, d)
            self.assertEqual(d[key], jsd[key])
        self.assertEqual(i, 1)

    def test_delete(self):
        jsd = self.get_json_dict()
        key = 'delete'
        d = self.get_testdata()
        jsd[key] = d
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d)
        del jsd[key]
        self.assertEqual(len(jsd), 0)
        with self.assertRaises(KeyError):
            jsd[key]
        jsd[key] = d
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d)
        del jsd.delete
        self.assertEqual(len(jsd), 0)
        with self.assertRaises(KeyError):
            jsd[key]

    def test_update(self):
        jsd = self.get_json_dict()
        key = 'update'
        d = {key: self.get_testdata()}
        jsd.update(d)
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d[key])

    def test_clear(self):
        jsd = self.get_json_dict()
        key = 'clear'
        d = self.get_testdata()
        jsd[key] = d
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d)
        jsd.clear()
        self.assertEqual(len(jsd), 0)

    def test_reopen(self):
        jsd = self.get_json_dict()
        key = 'reopen'
        d = self.get_testdata()
        jsd[key] = d
        jsd.save()
        del jsd  # possibly unsafe
        jsd2 = self.get_json_dict()
        jsd2.load()
        self.assertEqual(len(jsd2), 1)
        self.assertEqual(jsd2[key], d)

    def test_copy_as_dict(self):
        jsd = self.get_json_dict()
        key = 'copy'
        d = self.get_testdata()
        jsd[key] = d
        copy = dict(jsd)
        del jsd
        self.assertTrue(key in copy)
        self.assertEqual(copy[key], d)

    def test_reopen2(self):
        jsd = self.get_json_dict()
        key = 'reopen'
        d = self.get_testdata()
        jsd[key] = d
        del jsd  # possibly unsafe
        jsd2 = self.get_json_dict()
        self.assertEqual(len(jsd2), 1)
        self.assertEqual(jsd2[key], d)

    def test_write_invalid_type(self):
        class Foo(object):
            pass

        jsd = self.get_json_dict()
        key = 'write_invalid_type'
        d = self.get_testdata()
        jsd[key] = d
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d)
        d2 = Foo()
        with self.assertRaises(TypeError):
            jsd[key + '2'] = d2
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d)

    def test_buffered_read_write(self):
        jsd = self.get_json_dict()
        jsd2 = self.get_json_dict()
        self.assertEqual(jsd, jsd2)
        key = 'buffered_read_write'
        d = self.get_testdata()
        d2 = self.get_testdata()
        self.assertEqual(len(jsd), 0)
        self.assertEqual(len(jsd2), 0)
        with jsd.buffered() as b:
            b[key] = d
            self.assertEqual(b[key], d)
            self.assertEqual(len(b), 1)
            self.assertEqual(len(jsd2), 0)
        self.assertEqual(len(jsd), 1)
        self.assertEqual(len(jsd2), 1)
        with jsd2.buffered() as b2:
            b2[key] = d2
            self.assertEqual(len(jsd), 1)
            self.assertEqual(len(b2), 1)
            self.assertEqual(jsd[key], d)
            self.assertEqual(b2[key], d2)
        self.assertEqual(jsd[key], d2)
        self.assertEqual(jsd2[key], d2)
        with jsd.buffered() as b:
            del b[key]
            self.assertNotIn(key, b)
        self.assertNotIn(key, jsd)

    def test_keys_with_dots(self):
        jsd = self.get_json_dict()
        with self.assertRaises(InvalidKeyError):
            jsd['a.b'] = None

    def test_keys_valid_type(self):
        jsd = self.get_json_dict()

        class MyStr(str):
            pass
        for key in ('key', MyStr('key'), 0, None, True):
            d = jsd[key] = self.get_testdata()
            self.assertIn(str(key), jsd)
            self.assertEqual(jsd[str(key)], d)

    def test_keys_invalid_type(self):
        jsd = self.get_json_dict()

        class A:
            pass
        for key in (0.0, A(), (1, 2, 3)):
            with self.assertRaises(KeyTypeError):
                jsd[key] = self.get_testdata()
        for key in ([], {}, dict()):
            with self.assertRaises(TypeError):
                jsd[key] = self.get_testdata()


class JSONDictWriteConcernTest(JSONDictTest):

    def get_json_dict(self):
        return JSONDict(filename=self._fn_dict, write_concern=True)


class JSONDictNestedDataTest(JSONDictTest):

    def get_testdata(self):
        return dict(a=super(JSONDictNestedDataTest, self).get_testdata())


class JSONDictNestedDataWriteConcernTest(JSONDictNestedDataTest, JSONDictWriteConcernTest):

    pass


if __name__ == '__main__':
    unittest.main()
