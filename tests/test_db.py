import unittest
import os
import uuid
import unittest
import warnings
from math import sqrt

import networkx as nx
import pymongo
import gridfs

import compdb
from compdb.db import conversion
from compdb.db.conversion import add_adapter_to_network, make_adapter
from compdb.db.database import Database
from compdb.core.config import load_config
from compdb.core.dbclient_connector import DBClientConnector

PYMONGO_3 = pymongo.version_tuple[0] == 3
TESTING_DB = 'testing_compmatdb'
TEST_TOKEN = {'test_token': str(uuid.uuid4())}

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='compdb')


def basic_network():
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

DB = None

def _get_db(config = None):
    if config is None:
        config = load_config()
    connector = DBClientConnector(config, prefix = 'database_')
    connector.connect()
    connector.authenticate()
    def get_gridfs(project_id):
        return gridfs.GridFS(connector.client[TESTING_DB])
    return Database(db=connector.client[TESTING_DB], get_gridfs=get_gridfs)

def get_db(config=None):
    if config is not None:
        return _get_db(config=config)
    else:
        global DB
        if DB is None:
            DB = _get_db()
        return DB

def get_test_data():
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

class BaseDBTest(unittest.TestCase):

    def setUp(self):
        os.environ['COMPDB_AUTHOR_NAME'] = 'compdb_test_author'
        os.environ['COMPDB_AUTHOR_EMAIL'] = 'testauthor@example.com'
        os.environ['COMPDB_PROJECT'] = 'compdb_db_test_project'
        self.config = compdb.core.config.load_config()
        self.db = get_db(config=self.config)
        metadata, data = get_test_record()
        self.db.insert_one(metadata, data)

    def tearDown(self):
        self.db.delete_many(TEST_TOKEN)

class DBTest(BaseDBTest):
    
    def test_find_one(self):
        db = get_db()
        data = db.find_one(get_test_metadata())
        self.assertIsNotNone(data)

    def test_find(self):
        db = get_db()
        docs = db.find(get_test_metadata())
        self.assertGreaterEqual(docs.count(), 1)
        iterated = False
        for doc in docs:
            iterated=True
            self.assertIsNotNone(doc)
        self.assertTrue(iterated)
    
    def test_find_rewind(self):
        db = get_db()
        docs = db.find(get_test_metadata())
        self.assertGreaterEqual(docs.count(), 1)
        iterated = False
        for doc in docs:
            iterated = True
            self.assertIsNotNone(doc)
        self.assertTrue(iterated)
        iterated = False
        for doc in docs:
            iterated=True
        self.assertFalse(iterated)
        docs.rewind()
        for doc in docs:
            iterated=True
            self.assertIsNotNone(doc)
        self.assertTrue(iterated)

    def test_insert_without_data(self):
        db = get_db()
        meta = get_test_metadata()
        data = get_test_data()
        meta['extra'] = data
        db.insert_one(meta)
        doc = db.find_one(meta)
        self.assertIsNotNone(doc)
        self.assertEqual(doc['extra'], data)

    def test_insert_with_data(self):
        db = get_db()
        meta = get_test_metadata()
        meta['withdata'] = True
        data = get_test_data()
        db.insert_one(meta, data)
        doc = db.find_one(meta)
        self.assertIsNotNone(doc)
        self.assertEqual(doc['data'], data)

    def test_delete_many(self):
        db = get_db()
        db.delete_many({}) # deleting all records
        doc = db.find_one()
        self.assertIsNone(doc)

    def test_delete_one(self):
        db = get_db()
        meta = get_test_metadata()
        data = get_test_data()
        db.delete_many(meta)
        db.insert_one(meta)
        doc = db.find_one(meta)
        self.assertIsNotNone(doc)
        db.delete_one(meta)
        doc = db.find_one(meta)
        self.assertIsNone(doc)
        db.insert_one(meta, data)
        doc = db.find_one(meta)
        self.assertIsNotNone(doc)
        db.delete_one(meta)
        doc = db.find_one(meta)
        self.assertIsNone(doc)

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
        db = get_db()
        metadata = get_test_metadata()

        custom_adapter = conversion.make_adapter(
            CustomFloat, float, custom_to_float)
        db.add_adapter(custom_adapter)

        data = [42, 42.0, '42', CustomFloat(42.0)]
        for d in data:
            db.insert_one(metadata, d)

        def foo(x):
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
        doc_logic = db.find_one({'$and': [bullshit, f_logic]})
        self.assertIsNone(doc_logic)

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
        data = db.find_one(
            {'$and': [get_test_metadata(), bullshit]})
        self.assertIsNone(data)
        data = db.find_one(
            {'$and': [{'$or': [get_test_metadata(), bullshit]}]})
        self.assertIsNotNone(data)

    def test_aggregate(self):
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

        pipe = [
            {'$match': TEST_TOKEN},
            {'$match': {foo_method: {'$lt': 7}}}]
        docs = list(db.aggregate(pipe))
        self.assertTrue(docs)
        self.assertEqual(len(docs), len(data))
        pipe.append(
            {'$project': {
                '_id': False,
                'foo': foo_method}})
        docs = list(db.aggregate(pipe))
        self.assertTrue(docs)
        for doc in docs:
            self.assertTrue('foo' in doc)

class DBSecurityTest(BaseDBTest):

    def test_modify_user_filter(self):
        db = get_db()
        meta = get_test_metadata()
        #data = get_test_data()
        meta['author_name'] = 'impostor'
        with self.assertRaises(KeyError):
            db.insert_one(meta)
        del meta['author_name']
        meta['author_email'] = 'impostor@example.org'
        with self.assertRaises(KeyError):
            db.insert_one(meta)

    def test_delete_global_data(self):
        db = get_db()
        num_docs_before = len(list(db.find()))
        assert num_docs_before > 0
        author_name_original = db.config['author_name']
        try:
            db.config['author_name'] = 'impostor_delete'
            db.delete_many({})
            num_docs_after = len(list(db.find()))
            self.assertEqual(num_docs_before, num_docs_after)
            db.delete_one({})
            num_docs_after = len(list(db.find()))
            self.assertEqual(num_docs_before, num_docs_after)
        finally:
            db.config['author_name'] = author_name_original

    def test_modify_global_data(self):
        db = get_db()
        author_name_original = db.config['author_name']
        meta = get_test_metadata()
        db.insert_one(meta)
        doc_original = db.find_one(meta)
        assert not doc_original is None
        try:
            db.config['author_name'] = 'impostor_modification'
            del meta['author_name']
            data = get_test_data()
            num_docs_before = len(list(db.find()))
            result = db.replace_one(meta, data)
            if PYMONGO_3:
                self.assertEqual(result.matched_count, 0)
                self.assertEqual(result.modified_count, 0)
            else:
                self.assertIsNone(result)
            num_docs_after = len(list(db.find()))
            self.assertEqual(num_docs_before, num_docs_after)
            self.assertIsNone(db.find_one(meta))
            doc_check = db.find_one({'_id': doc_original['_id']})
            assert not doc_check is None
            result = db.update_one(meta, data)
            if PYMONGO_3:
                self.assertEqual(result.matched_count, 0)
                self.assertEqual(result.modified_count, 0)
            else:
                self.assertEqual(result['ok'], 1)
                self.assertEqual(result['nModified'], 0)
            self.assertIsNone(db.find_one(meta))
            doc_check = db.find_one({'_id': doc_original['_id']})
            self.assertEqual(doc_original, doc_check)
        finally:
            db.config['author_name'] = author_name_original

if __name__ == '__main__':
    unittest.main() 
