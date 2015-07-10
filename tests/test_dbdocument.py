import unittest
from contextlib import contextmanager

from compdb.core.dbdocument import DBDocument

import warnings
warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='compdb')

import pymongo

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
    from pymongo import MongoClient
    from compdb.core.config import load_config
    config = load_config()
    client = MongoClient(config['database_host'])
    db = client['testing']
    return db['test_dbdocument']

@contextmanager
def get_dbdoc(host = None, id_ = None):
    from compdb.core.config import load_config
    config = load_config()
    if host is None:
        host = config['database_host']
    if id_ is None:
        with document() as id_:
            dbdoc = DBDocument(host, 'testing', 'dbdocument', id_, connect_timeout_ms = config['connect_timeout_ms'])
            with dbdoc as x:
                yield x
            x.remove()
    else:
        dbdoc = DBDocument(host, 'testing', 'dbdocument', id_, connect_timeout_ms = config['connect_timeout_ms'])
        with dbdoc as x:
            yield x
        x.remove()

@unittest.skipIf(pymongo.version_tuple[0] < 3, "Test requires pymongo version >= 3.x")
class TestDBDocument(unittest.TestCase):
    
    def test_construction(self):
        dbdoc = get_dbdoc()

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
            dbdoc = DBDocument(
                'localhost', 'testing', 'dbdocument', id_)
            with self.assertRaises(RuntimeError):
                dbdoc['key'] = 'data'
            with self.assertRaises(RuntimeError):
                data = dbdoc['key']
            dbdoc.clear()
            #with self.assertRaises(RuntimeError):
            with self.assertRaises(AttributeError):
                dbdoc.close()
            dbdoc.remove()

    def test_open_closing(self):
        with document() as id_:
            dbdoc = DBDocument(
                'localhost', 'testing', 'dbdocument', id_)
            dbdoc.open()
            dbdoc.clear()
            dbdoc.close()
            dbdoc.remove()

    def test_get(self):
        key = "test_get"
        data = testdata()
        with get_dbdoc():
            dbdoc[key] = data
            self.assertEqual(dbdoc.get(key), data)
            self.assertIsNone(dbdoc.get('abc'))
            self.assertEqual(dbdoc.get('abc', 123), 123)

        with get_dbdoc():
            self.assertEqual(dbdoc[key], data)
            self.assertEqual(dbdoc.get(key), data)

    def test_iteration(self):
        key = 'test_iteration'
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
        key = 'test_clear'
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
