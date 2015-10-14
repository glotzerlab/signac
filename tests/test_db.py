import unittest
import os
import uuid
import warnings
from math import sqrt

import networkx as nx
import pymongo

import signac
from signac.contrib import conversion
from signac.contrib.conversion import add_adapter_to_network, make_adapter

PYMONGO_3 = pymongo.version_tuple[0] == 3
TESTING_DB = 'testing_signacdb'

warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')

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

DB = None

def get_signac_db_handle(host='testing', config=None, clear=False):
    global DB
    #if clear:
    DB = None
    if config is None:
        config = signac.common.config.load_config()
        config['project'] = 'testing'
        config['signacdb']['database'] = 'signacdbtesting'
        config['author_name'] = 'test_author'
        config['author_email'] = 'testauthor@example.com'
        config.verify()
    if DB is None:
        DB = signac.db.connect(host='testing', config=config)
    return DB

def get_test_data():
    return str(uuid.uuid4())

class CustomFloat(object): # Class for conversion testing.
    def __init__(self, value):
        self._value = value

def custom_to_float(custom):
    return float(custom._value)

class BaseDBTest(unittest.TestCase):

    def setUp(self):
        self.test_token = {'test_token': str(uuid.uuid4())}
        os.environ['COMPDB_AUTHOR_NAME'] = 'signac_test_author'
        os.environ['COMPDB_AUTHOR_EMAIL'] = 'testauthor@example.com'
        os.environ['COMPDB_PROJECT'] = 'signac_db_test_project'
        self.signac_db = get_signac_db_handle()
        metadata, data = self.get_test_record()
        self.signac_db.insert_one(metadata, data)
        self.addCleanup(self.clear_db)

    def clear_db(self):
        self.signac_db.delete_many(self.test_token)

    def get_test_metadata(self):
        ret = {'testing': 'test'}
        ret.update(self.test_token)
        return ret

    def get_test_record(self):
        return self.get_test_metadata(), get_test_data()


class DBTest(BaseDBTest):
    
    def test_find_one(self):
        signac_db = get_signac_db_handle()
        data = signac_db.find_one(self.get_test_metadata())
        self.assertIsNotNone(data)

    def test_find(self):
        signac_db = get_signac_db_handle()
        docs = signac_db.find(self.get_test_metadata())
        self.assertGreaterEqual(docs.count(), 1)
        iterated = False
        for doc in docs:
            iterated=True
            self.assertIsNotNone(doc)
        self.assertTrue(iterated)
    
    def test_find_rewind(self):
        signac_db = get_signac_db_handle()
        docs = signac_db.find(self.get_test_metadata())
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
        signac_db = get_signac_db_handle()
        meta = self.get_test_metadata()
        data = get_test_data()
        meta['extra'] = data
        signac_db.insert_one(meta)
        doc = signac_db.find_one(meta)
        self.assertIsNotNone(doc)
        self.assertEqual(doc['extra'], data)

    def test_insert_with_data(self):
        signac_db = get_signac_db_handle()
        meta = self.get_test_metadata()
        meta['withdata'] = True
        data = get_test_data()
        signac_db.insert_one(meta, data)
        doc = signac_db.find_one(meta)
        self.assertIsNotNone(doc)
        self.assertEqual(doc['data'], data)

    def test_delete_many(self):
        signac_db = get_signac_db_handle()
        signac_db.delete_many({}) # deleting all records
        doc = signac_db.find_one()
        self.assertIsNone(doc)

    def test_delete_one(self):
        signac_db = get_signac_db_handle()
        meta = self.get_test_metadata()
        data = get_test_data()
        signac_db.delete_many(meta)
        signac_db.insert_one(meta)
        doc = signac_db.find_one(meta)
        self.assertIsNotNone(doc)
        signac_db.delete_one(meta)
        doc = signac_db.find_one(meta)
        self.assertIsNone(doc)
        signac_db.insert_one(meta, data)
        doc = signac_db.find_one(meta)
        self.assertIsNotNone(doc)
        signac_db.delete_one(meta)
        doc = signac_db.find_one(meta)
        self.assertIsNone(doc)

    def test_replace_one(self):
        signac_db = get_signac_db_handle()
        data = signac_db.find_one(self.get_test_metadata())
        self.assertIsNotNone(data)
        test_data = get_test_data()
        signac_db.replace_one(self.get_test_metadata(), test_data)
        data2 = signac_db.find_one(self.get_test_metadata())
        self.assertIsNotNone(data2)
        self.assertEqual(data2['data'], test_data)

    def test_update_one(self):
        signac_db = get_signac_db_handle()
        data = signac_db.find_one(self.get_test_metadata())
        self.assertIsNotNone(data)
        test_data = get_test_data()
        signac_db.update_one(self.get_test_metadata(), test_data)
        data2 = signac_db.find_one(self.get_test_metadata())
        self.assertIsNotNone(data2)
        self.assertEqual(data2['data'], test_data)

    def test_method_filter(self):
        signac_db = get_signac_db_handle()

        def foo(x):
            return 'foo'

        docs = list(signac_db.find(self.test_token))
        f_foo = {foo: 'foo'}
        f_foo.update(self.test_token)
        docs_foo = list(signac_db.find(f_foo))
        self.assertTrue(docs)
        self.assertEqual(len(docs), len(docs_foo))

        f_bar = {foo: 'bar'}
        f_bar.update(self.test_token)
        docs_bar = list(signac_db.find(f_bar))
        self.assertEqual(len(docs_bar), 0)

    def test_method_adapter(self):
        signac_db = get_signac_db_handle()
        metadata = self.get_test_metadata()

        custom_adapter = conversion.make_adapter(
            CustomFloat, float, custom_to_float)
        signac_db.add_adapter(custom_adapter)

        data = [42, 42.0, '42', CustomFloat(42.0)]
        for d in data:
            signac_db.insert_one(metadata, d)

        def foo(x):
            assert isinstance(x, int)
            return sqrt(x)
        foo_method = conversion.make_db_method(foo, int)

        f = {foo_method: {'$lt': 7}}
        f.update(self.test_token)
        docs = list(signac_db.find(self.test_token))
        self.assertTrue(docs)
        docs_foo = list(signac_db.find(f))
        self.assertEqual(len(docs_foo), len(data))
        bullshit = {'bullshit': True}
        f_logic = {'$and': [{'$or': [f, bullshit]}, f]}
        docs_logic = list(signac_db.find(f_logic))
        self.assertEqual(len(docs_logic), len(data))
        doc_logic = signac_db.find_one(f_logic)
        self.assertIsNotNone(doc_logic)
        doc_logic = signac_db.find_one({'$and': [bullshit, f_logic]})
        self.assertIsNone(doc_logic)

    #@unittest.skip("Currently defunct.")
    def test_multiple_conversion_paths(self):
        metadata = self.get_test_metadata()

        class Intermediate(object):
            def __init__(self, value):
                self._value = value

        def foo(x):
            assert isinstance(x, int)
            return sqrt(x)
        foo_method = conversion.make_db_method(foo, int)

        # We need to potentially remove the custom adapter from previous tests.
        def attempt_remove_adapter(name):
            try:
                del signac.contrib.conversion.Adapter.registry[name]
            except KeyError:
                pass
        attempt_remove_adapter("<class '__main__.CustomFloat'>_to_<class 'float'>")
        attempt_remove_adapter("<class 'test_db.CustomFloat'>_to_<class 'float'>")

        signac_db = get_signac_db_handle(clear=True)
        signac_db.delete_many({})
        data = [42, 42.0, '42', CustomFloat(42.0)]
        for d in data:
            signac_db.insert_one(metadata, d)

        f = {foo_method: {'$lt': 7}}
        f.update(metadata)
        f.update(self.test_token)
        docs = list(signac_db.find(self.test_token))
        self.assertTrue(docs)
        docs_foo = list(signac_db.find(f))
        for item in docs_foo:
            print(item)
        self.assertEqual(len(docs_foo), len(data)-1)

        def custom_to_float_defunct(custom):
            assert 0
        custom_defunct_adapter = conversion.make_adapter(
            CustomFloat, float, custom_to_float_defunct, w=conversion.WEIGHT_DISCOURAGED)
        def custom_to_intermediate(custom):
            return Intermediate(custom)
        custom_to_intermediate_adapter = conversion.make_adapter(
            CustomFloat, Intermediate, custom_to_intermediate)
        def intermediate_to_float(intermediate):
            intermediate_to_float.num_called += 1
            return float(intermediate._value._value)
        intermediate_to_float.num_called=0
        intermediate_to_float_adapter = conversion.make_adapter(
            Intermediate, float, intermediate_to_float)

        signac_db.add_adapter(custom_defunct_adapter)
        signac_db.add_adapter(custom_to_intermediate_adapter)
        signac_db.add_adapter(intermediate_to_float_adapter)

        docs_foo = list(signac_db.find(f))
        self.assertEqual(len(docs_foo), len(data))
        self.assertEqual(intermediate_to_float.num_called, 1)
        custom_adapter = conversion.make_adapter(
            CustomFloat, float, custom_to_float)
        signac_db.add_adapter(custom_adapter)
        docs_foo = list(signac_db.find(f))
        self.assertEqual(len(docs_foo), len(data))
        self.assertEqual(intermediate_to_float.num_called, 1)

    def test_filter_logic(self):
        signac_db = get_signac_db_handle()
        bullshit = {'bullshit': True}
        data = signac_db.find_one(
            {'$or': [self.get_test_metadata(), bullshit]})
        self.assertIsNotNone(data)
        data = signac_db.find_one(
            {'$and': [self.get_test_metadata(),
                {'$or': [self.get_test_metadata(), bullshit]}]})
        self.assertIsNotNone(data)
        data = signac_db.find_one(
            {'$and': [self.get_test_metadata(), bullshit]})
        self.assertIsNone(data)
        data = signac_db.find_one(
            {'$and': [{'$or': [self.get_test_metadata(), bullshit]}]})
        self.assertIsNotNone(data)

    def test_aggregate(self):
        signac_db = get_signac_db_handle()
        metadata = self.get_test_metadata()
        custom_adapter = conversion.make_adapter(
            CustomFloat, float, custom_to_float)
        signac_db.add_adapter(custom_adapter)

        data = [42, 42.0, '42', CustomFloat(42.0)]
        for d in data:
            signac_db.insert_one(metadata, d)

        def foo(x):
            assert isinstance(x, int)
            return sqrt(x)
        foo_method = conversion.make_db_method(foo, int)

        pipe = [
            {'$match': self.test_token},
            {'$match': {foo_method: {'$lt': 7}}}]
        docs = list(signac_db.aggregate(pipe))
        self.assertTrue(docs)
        self.assertEqual(len(docs), len(data))
        pipe.append(
            {'$project': {
                '_id': False,
                'foo': foo_method}})
        docs = list(signac_db.aggregate(pipe))
        self.assertTrue(docs)
        for doc in docs:
            self.assertTrue('foo' in doc)

class DBSecurityTest(BaseDBTest):

    def test_modify_user_filter(self):
        signac_db = get_signac_db_handle()
        meta = self.get_test_metadata()
        #data = get_test_data()
        meta['author_name'] = 'impostor'
        with self.assertRaises(KeyError):
            signac_db.insert_one(meta)
        del meta['author_name']
        meta['author_email'] = 'impostor@example.org'
        with self.assertRaises(KeyError):
            signac_db.insert_one(meta)

    def test_delete_global_data(self):
        signac_db = get_signac_db_handle()
        num_docs_before = len(list(signac_db.find()))
        assert num_docs_before > 0
        author_name_original = signac_db.config['author_name']
        try:
            signac_db.config['author_name'] = 'impostor_delete'
            signac_db.delete_many({})
            num_docs_after = len(list(signac_db.find()))
            self.assertEqual(num_docs_before, num_docs_after)
            signac_db.delete_one({})
            num_docs_after = len(list(signac_db.find()))
            self.assertEqual(num_docs_before, num_docs_after)
        finally:
            signac_db.config['author_name'] = author_name_original

    def test_modify_global_data(self):
        signac_db = get_signac_db_handle()
        author_name_original = signac_db.config['author_name']
        meta = self.get_test_metadata()
        signac_db.insert_one(meta)
        doc_original = signac_db.find_one(meta)
        assert not doc_original is None
        try:
            signac_db.config['author_name'] = 'impostor_modification'
            del meta['author_name']
            data = get_test_data()
            num_docs_before = len(list(signac_db.find()))
            result = signac_db.replace_one(meta, data)
            if PYMONGO_3:
                self.assertEqual(result.matched_count, 0)
                self.assertEqual(result.modified_count, 0)
            else:
                self.assertIsNone(result)
            num_docs_after = len(list(signac_db.find()))
            self.assertEqual(num_docs_before, num_docs_after)
            self.assertIsNone(signac_db.find_one(meta))
            doc_check = signac_db.find_one({'_id': doc_original['_id']})
            assert not doc_check is None
            result = signac_db.update_one(meta, data)
            if PYMONGO_3:
                self.assertEqual(result.matched_count, 0)
                self.assertEqual(result.modified_count, 0)
            else:
                self.assertEqual(result['ok'], 1)
                self.assertEqual(result['nModified'], 0)
            self.assertIsNone(signac_db.find_one(meta))
            doc_check = signac_db.find_one({'_id': doc_original['_id']})
            self.assertEqual(doc_original, doc_check)
        finally:
            signac_db.config['author_name'] = author_name_original

if __name__ == '__main__':
    unittest.main() 
