import unittest
from contextlib import contextmanager

from compdb.core.mongodb_set import MongoDBSet

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

import warnings
warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='compdb')

def testdata():
    import uuid
    return str(uuid.uuid4())

def get_item():
    return {'my_item': testdata()}

def get_collection_handle(_id):
    from compdb.core.config import load_config
    from pymongo import MongoClient
    config = load_config()
    client = MongoClient(config['database_host'])
    db = client['testing']
    if _id is None:
        _id = uuid.uuid4()
    collection_name = 'test_mongodb_mongoset_{}'.format(_id)
    return db[collection_name]

@contextmanager
def get_collection(_id = None):
    if _id is None:
        import uuid
        _id = uuid.uuid4()
    collection = get_collection_handle(_id)
    try:
        yield collection
    except Exception:
        raise
    finally:
        collection.drop()

@contextmanager
def get_mongoset(id_set = None):
    with get_collection(id_set) as collection:
        yield MongoDBSet(collection)

@unittest.skipIf(not PYMONGO_3, 'skip requires pymongo version >= 3.0.x')
class MongoDBSetTest(unittest.TestCase):

    def test_init(self):
        with get_collection() as collection:
            mongoset = MongoDBSet(collection)

    def test_add_and_pop(self):
        item = get_item()
        with get_mongoset() as mongoset:
            mongoset.add(item)
            item2 = mongoset.pop()
            self.assertEqual(item, item2)
            with self.assertRaises(KeyError):
                mongoset.pop()

    def test_add_and_pop_multiple(self):
        item1 = get_item()
        item2 = get_item()
        with get_mongoset() as mongoset:
            mongoset.add(item1)
            mongoset.add(item1)
            mongoset.add(item2)
            mongoset.add(item2)
            c_item1 = mongoset.pop()
            c_item2 = mongoset.pop()
            self.assertIn(c_item1, (item1, item2))
            self.assertIn(c_item2, (item1, item2))
            with self.assertRaises(KeyError):
                mongoset.pop()
    
    def test_contains(self):
        item1 = get_item()
        item2 = get_item()
        with get_mongoset() as mongoset:
            self.assertNotIn(item1, mongoset)
            self.assertNotIn(item2, mongoset)
            mongoset.add(item1)
            self.assertIn(item1, mongoset)
            self.assertNotIn(item2, mongoset)
            mongoset.add(item2)
            self.assertIn(item1, mongoset)
            self.assertIn(item2, mongoset)
            r_item1 = mongoset.pop()
            self.assertEqual(item1, r_item1)
            self.assertNotIn(item1, mongoset)
            self.assertIn(item2, mongoset)
            r_item2 = mongoset.pop()
            self.assertEqual(item2, r_item2)
            self.assertNotIn(item1, mongoset)
            self.assertNotIn(item2, mongoset)

    def test_remove(self):
        item1 = get_item()
        item2 = get_item()
        with get_mongoset() as mongoset:
            self.assertNotIn(item1, mongoset)
            self.assertNotIn(item2, mongoset)
            mongoset.add(item1)
            self.assertIn(item1, mongoset)
            with self.assertRaises(KeyError):
                mongoset.remove(item2)
            mongoset.discard(item2)
            mongoset.remove(item1)
            self.assertNotIn(item1, mongoset)
            mongoset.add(item2)
            self.assertIn(item2, mongoset)
            mongoset.discard(item2)
            self.assertNotIn(item2, mongoset)

    def test_clear(self):
        item1 = get_item()
        item2 = get_item()
        with get_mongoset() as mongoset:
            self.assertNotIn(item1, mongoset)
            self.assertNotIn(item2, mongoset)
            mongoset.add(item1)
            mongoset.add(item2)
            self.assertIn(item1, mongoset)
            self.assertIn(item2, mongoset)
            mongoset.clear()
            self.assertNotIn(item1, mongoset)
            self.assertNotIn(item2, mongoset)

    def test_len(self):
        item1 = get_item()
        item2 = get_item()
        with get_mongoset() as mongoset:
            self.assertEqual(0, len(mongoset))
            mongoset.add(item1)
            self.assertEqual(1, len(mongoset))
            mongoset.add(item2)
            self.assertEqual(2, len(mongoset))
            mongoset.remove(item1)
            self.assertEqual(1, len(mongoset))
            mongoset.discard(item2)
            self.assertEqual(0, len(mongoset))

if __name__ == '__main__':
    unittest.main()
