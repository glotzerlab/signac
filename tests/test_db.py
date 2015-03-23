import unittest
import networkx as nx
import uuid

TESTING_DB = 'testing_matdb'
TEST_TOKEN = {'test_token': str(uuid.uuid4())}

def basic_network():
    import uuid
    from compdb.db.conversion import add_adapter_to_network, make_adapter
    an = nx.DiGraph()
    an.add_nodes_from([int, float, str, uuid.UUID])
    add_adapter_to_network(an, make_adapter(int, float))
    add_adapter_to_network(an, make_adapter(float, int))
    # to make it interesting...
    #add_adapter_to_network(an, make_adapter(int, str))
    add_adapter_to_network(an, make_adapter(str, int))
    add_adapter_to_network(an, make_adapter(float, str))
    add_adapter_to_network(an, make_adapter(uuid.UUID, str))
    return an

def draw_network(network):
    from matplotlib import pyplot as plt
    plot = nx.draw(basic_network(), with_labels = True)
    plt.show()

def get_db():
    from pymongo import MongoClient
    from compdb.db.database import Database
    client = MongoClient()
    db = Database(db = client[TESTING_DB])
    db.adapter_network = basic_network()
    return db

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
        import os
        os.environ['COMPDB_AUTHOR_NAME'] = 'compdb_test_author'
        os.environ['COMPDB_AUTHOR_EMAIL'] = 'testauthor@example.com'
        os.environ['COMPDB_DATABASE_HOST'] = 'localhost'
        db = get_db()
        metadata, data = get_test_record()
        db.insert_one(metadata, data)

    def tearDown(self):
        db = get_db()
        db.delete_many(TEST_TOKEN)
    
    def test_find_one(self):
        db = get_db()
        data = db.find_one(get_test_metadata())
        self.assertIsNotNone(data)

    def test_insert_without_data(self):
        db = get_db()
        meta = get_test_metadata()
        data = get_test_data()
        meta['extra'] = data
        db.insert_one(meta)
        doc = db.find_one(meta)
        self.assertIsNotNone(doc)
        self.assertEqual(doc['extra'], data)

    def test_replace_one(self):
        db = get_db()
        data = db.find_one(get_test_metadata())
        self.assertIsNotNone(data)
        test_data = get_test_data()
        db.replace_one(get_test_metadata(), test_data)
        data2 = db.find_one(get_test_metadata())
        self.assertIsNotNone(data2)
        self.assertEqual(data2['data'], test_data)

    def test_update_one(self):
        db = get_db()
        data = db.find_one(get_test_metadata())
        self.assertIsNotNone(data)
        test_data = get_test_data()
        db.update_one(get_test_metadata(), test_data)
        data2 = db.find_one(get_test_metadata())
        self.assertIsNotNone(data2)
        self.assertEqual(data2['data'], test_data)

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
        foo_method = conversion.make_db_method(foo, int)

        f = {foo_method: {'$lt': 7}}
        f.update(TEST_TOKEN)
        docs = list(db.find(TEST_TOKEN))
        self.assertTrue(docs)
        docs_foo = list(db.find(f))
        self.assertEqual(len(docs_foo), len(data))
        f_implicit_conversion = {foo: {'$lt': 7}}
        f_implicit_conversion.update(TEST_TOKEN)
        docs_foo_nc = list(db.find(f_implicit_conversion))
        self.assertEqual(len(docs_foo_nc), 1)
        bullshit = {'bullshit': True}
        f_logic = {'$and': [{'$or': [f, bullshit]}, f]}
        docs_logic = list(db.find(f_logic))
        self.assertEqual(len(docs_logic), len(data))
        doc_logic = db.find_one(f_logic)
        self.assertIsNotNone(doc_logic)

    def test_filter_logic(self):
        db = get_db()
        bullshit = {'bullshit': True}
        data = db.find_one(
            {'$or': [get_test_metadata(), bullshit]})
        self.assertIsNotNone(data)
        data = db.find_one(
            {'$and': [get_test_metadata(),
                {'$or': [get_test_metadata(), bullshit]}]})
        self.assertIsNotNone(data)

if __name__ == '__main__':
    unittest.main() 
