import os
import unittest
import tempfile
import uuid

from compdb.core.jsondict import JSonDict

FN_DICT = 'jsondict.json'

def testdata():
    return str(uuid.uuid4())

class BaseJSonDictTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='jsondict_')
        self._fn_dict = os.path.join(self._tmp_dir.name, FN_DICT)
        self.addCleanup(self._tmp_dir.cleanup)

class JSonDictTest(BaseJSonDictTest):

    def get_json_dict(self):
        return JSonDict(self._fn_dict)

    def test_init(self):
        self.get_json_dict()

    def test_set_get(self):
        jsd = self.get_json_dict()
        key = 'setget'
        d = testdata()
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

    def test_copy_value(self):
        jsd = self.get_json_dict()
        key = 'copy_value'
        key2 = 'copy_value2'
        d = testdata()
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
        d1 = testdata()
        d2 = testdata()
        d = {key1:d1, key2: d2}
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
        d = testdata()
        jsd[key] = d
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d)
        del jsd[key]
        self.assertEqual(len(jsd), 0)
        with self.assertRaises(KeyError):
            jsd[key]

    def test_update(self):
        jsd = self.get_json_dict()
        key = 'update'
        d = {key: testdata()}
        jsd.update(d)
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d[key])

    def test_clear(self):
        jsd = self.get_json_dict()
        key = 'clear'
        d = testdata()
        jsd[key] = d
        self.assertEqual(len(jsd), 1)
        self.assertEqual(jsd[key], d)
        jsd.clear()
        self.assertEqual(len(jsd), 0)

    def test_reopen(self):
        jsd = self.get_json_dict()
        key = 'reopen'
        d = testdata()
        jsd[key] = d
        jsd.save()
        del jsd # possibly unsafe
        jsd2 = self.get_json_dict()
        jsd2.load()
        self.assertEqual(len(jsd2), 1)
        self.assertEqual(jsd2[key], d)

class SynchronizedDictTest(JSonDictTest):

    def get_json_dict(self):
        return JSonDict(self._fn_dict, synchronized=True)

    def test_reopen(self):
        jsd = self.get_json_dict()
        key = 'reopen'
        d = testdata()
        jsd[key] = d
        del jsd # possibly unsafe
        jsd2 = self.get_json_dict()
        self.assertEqual(len(jsd2), 1)
        self.assertEqual(jsd2[key], d)

class SynchronizedWithWriteConcern(SynchronizedDictTest):
    
    def get_json_dict(self):
        return JSonDict(self._fn_dict, synchronized=True, write_concern=True)

if __name__ == '__main__':
    unittest.main()
