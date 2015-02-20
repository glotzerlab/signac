import unittest
from contextlib import contextmanager

from compdb.core.dbdocument import DBDocument

def testdata():
    import uuid
    return uuid.uuid4()

@contextmanager
def document(collection = None):
    if collection is None:
        collection = get_collection()
    try:
        _id = collection.save({}, new = True)
        yield _id
    except Exception:
        raise
    finally:
        collection.remove({'_id': _id})

def get_collection():
    from pymongo import MongoClient
    client = MongoClient()
    db = client['testing']
    return db['test_dbdocument']

@contextmanager
def get_dbdoc():
    with document() as id_:
        dbdoc = DBDocument(get_collection(), id_)
        yield dbdoc
    

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

    def test_get(self):
        key = "test_get"
        data = testdata()
        with get_dbdoc() as dbdoc:
            dbdoc[key] = data
            self.assertEqual(dbdoc.get(key), data)
            self.assertIsNone(dbdoc.get('abc'))
            self.assertEqual(dbdoc.get('abc', 123), 123)

    def test_iteration(self):
        key = 'test_iteration'
        num_entries = 100
        pairs = [(str(i), testdata()) for i in range(num_entries)]
        with get_dbdoc() as dbdoc:
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

if __name__ == '__main__':
    unittest.main()
