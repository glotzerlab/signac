
import unittest
import uuid
TESTING_DB = 'testing_matdb'
TEST_TOKEN = {'test_token': str(uuid.uuid4())}

def get_db():
    from pymongo import MongoClient
    from compdb.db.conversion import basic_network
    from compdb.db.database import Database
    client = MongoClient()
    return Database(
        db = client[TESTING_DB],
        adapter_network = basic_network())

def get_test_data():
    import uuid
    return str(uuid.uuid4())

def get_test_metadata():
    ret = {'testing': 'test'}
    ret.update(TEST_TOKEN)
    return ret

def get_test_record():
    return get_test_metadata(), get_test_data()

class CustomFloat(object): # Class for conversion testing.
    def __init__(self, value):
        self._value = value

def custom_to_float(custom):
    return float(custom._value)

class DBTest(unittest.TestCase):

    def setUp(self):
        db = get_db()
        metadata, data = get_test_record()
        db.insert_one(metadata, data)

    def tearDown(self):
        db = get_db()
        db.delete_many(TEST_TOKEN)
    
    def test_find_one(self):
        db = get_db()
        data = db._find_one(get_test_metadata())
        self.assertIsNotNone(data)

    def test_method_filter(self):
        db = get_db()

        def foo(x):
            return 'foo'

        docs = list(db.find(TEST_TOKEN))
        f_foo = {foo: 'foo'}
        f_foo.update(TEST_TOKEN)
        docs_foo = list(db.find(f_foo))
        self.assertTrue(docs)
        self.assertEqual(len(docs), len(docs_foo))

        f_bar = {foo: 'bar'}
        f_bar.update(TEST_TOKEN)
        docs_bar = list(db.find(f_bar))
        self.assertEqual(len(docs_bar), 0)

    def test_method_adapter(self):
        from compdb.db import conversion
        db = get_db()
        metadata = get_test_metadata()

        custom_adapter = conversion.make_adapter(
            CustomFloat, float, custom_to_float)
        db.add_adapter(custom_adapter)

        data = [42, 42.0, '42', CustomFloat(42.0)]
        for d in data:
            db.insert_one(metadata, d)

        def foo(x):
            from math import sqrt
            assert isinstance(x, int)
            return sqrt(x)
        foo_method = conversion.DBMethod(foo, expects = int)

        f = {foo_method: {'$lt': 7}}
        f.update(TEST_TOKEN)
        docs = list(db.find(TEST_TOKEN))
        self.assertTrue(docs)
        docs_foo = list(db.find(f))
        for doc in docs_foo:
            print(doc)
        self.assertEqual(len(docs_foo), len(data))
        f_implicit_conversion = {foo: {'$lt': 7}}
        f_implicit_conversion.update(TEST_TOKEN)
        docs_foo_nc = list(db.find(f_implicit_conversion))
        self.assertEqual(len(docs_foo_nc), len(data))

if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG)
    unittest.main() 
