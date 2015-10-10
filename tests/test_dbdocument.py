import unittest
import warnings
from contextlib import contextmanager

import pymongo
import signac
from signac.core.dbdocument import DBDocument

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')

def testdata():
    import uuid
    return uuid.uuid4()

@contextmanager
def document(collection = None):
    from pymongo.errors import OperationFailure
    if collection is None:
        collection = get_collection()
    _id = None
    try:
        _id = collection.insert_one({}).inserted_id
        yield _id
    except Exception:
        raise
    finally:
        try:
            collection.delete_one({'_id': _id})
        except OperationFailure:
            pass

def get_collection():
    db = signac.get_db('testing', hostname='testing')
    return db['test_dbdocument']

@contextmanager
def get_dbdoc(hostname = 'testing', id_ = None):
    if id_ is None:
        with document() as id_:
            dbdoc = DBDocument(hostname, 'testing', 'dbdocument', id_, connect_timeout_ms = 1000)
            with dbdoc as x:
                yield x
            x.remove()
    else:
        dbdoc = DBDocument(hostname, 'testing', 'dbdocument', id_, connect_timeout_ms = 1000)
        with dbdoc as x:
            yield x
        x.remove()

@unittest.skipIf(pymongo.version_tuple[0] < 3, "Test requires pymongo version >= 3.x")
class TestDBDocument(unittest.TestCase):
    
    def test_construction(self):
        get_dbdoc()

    def test_save_and_load(self):
        key = "test_save_and_load"
        data = testdata()
        with get_dbdoc() as dbdoc:
            dbdoc[key] = data
            rb = dbdoc[key]
            self.assertEqual(rb, data)
            self.assertIn(key, dbdoc)
            self.assertNotIn('abc', dbdoc)

    def test_not_open(self):
        with document() as id_:
            dbdoc = DBDocument('testing', 'testing', 'test_dbdocument', id_)
            with self.assertRaises(RuntimeError):
                dbdoc['key'] = 'data'
            with self.assertRaises(RuntimeError):
                dbdoc['key']
            dbdoc.clear()
            #with self.assertRaises(RuntimeError):
            with self.assertRaises(AttributeError):
                dbdoc.close()
            dbdoc.remove()

    def test_open_closing(self):
        with document() as id_:
            dbdoc = DBDocument('testing', 'testing', 'test_dbdocument', id_)
            dbdoc.open()
            dbdoc.clear()
            dbdoc.close()
            dbdoc.remove()

    def test_get(self):
        key = "test_get"
        data = testdata()
        with get_dbdoc() as dbdoc:
            dbdoc[key] = data
            self.assertEqual(dbdoc.get(key), data)
            self.assertIsNone(dbdoc.get('abc'))
            self.assertEqual(dbdoc.get('abc', 123), 123)

        with get_dbdoc():
            self.assertEqual(dbdoc[key], data)
            self.assertEqual(dbdoc.get(key), data)

    def test_iteration(self):
        num_entries = 100
        pairs = [(str(i), testdata()) for i in range(num_entries)]
        with get_dbdoc() as dbdoc:
            for k in dbdoc:
                assert False
            for k, v in pairs:
                dbdoc[k] = v
            for k in dbdoc:
                if k != '_id':
                    self.assertIn(k, set((k for k,v in pairs)))

    def test_clear(self):
        num_entries = 2
        pairs = [(str(i), testdata()) for i in range(num_entries)]
        with get_dbdoc() as dbdoc:
            for k, v in pairs:
                dbdoc[k] = v
            for k in dbdoc:
                if k != '_id':
                    self.assertIn(k, set((k for k,v in pairs)))
            dbdoc.clear()
            for k in dbdoc:
                self.assertEqual(k, '_id')

    def test_update(self):
        key = 'test_update'
        data = testdata()
        with get_dbdoc() as dbdoc:
            dbdoc.update(dict(key=data))
            self.assertEqual(dbdoc.get(key), data)

    def test_update_from(self):
        key = 'test_update_from'
        data = testdata()
        with get_dbdoc() as dbdoc:
            mydict = dict()
            dbdoc[key] = data
            self.assertIn(dbdoc, key)
            mydict.update(dbdoc)
            self.assertIn(mydict, key)

    #def test_bad_host(self):
    #    import os
    #    import tempfile
    #    key = "test_bad_host"
    #    data = testdata()
    #    cwd = os.getcwd()
    #    try:
    #        with tempfile.TemporaryDirectory() as tmpdir:
    #            os.chdir(tmpdir)
    #            with get_dbdoc(host = 'example.com') as dbdoc:
    #                dbdoc[key] = data
    #                rb = dbdoc[key]
    #                self.assertEqual(rb, data)
    #                self.assertIn(key, dbdoc)
    #                self.assertNotIn('abc', dbdoc)

    #            with dbdoc:
    #                self.assertIn(key, dbdoc)
    #                self.assertEqual(dbdoc.get(key), data)

    #            id_ = dbdoc._id
    #            with get_dbdoc(id_ = dbdoc._id) as valid_dbdoc:
    #                self.assertIn(key, valid_dbdoc)
    #                self.assertEqual(valid_dbdoc.get(key), data)
    #    except:
    #        raise
    #    finally:
    #        os.chdir(cwd)

if __name__ == '__main__':
    unittest.main()
