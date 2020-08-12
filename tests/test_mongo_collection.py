# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from signac.core.collection_mongodb import MongoDict
from signac.core.collection_mongodb import MongoList
from test_synced_collection import TestJSONDict
from test_synced_collection import TestJSONList

try:
    import pymongo
    try:
        # test the mongodb server
        MongoClient = pymongo.MongoClient()
        tmp_collection = MongoClient['test_db']['test']
        tmp_collection.insert_one({'test': '0'})
        ret = tmp_collection.find_one({'test': '0'})
        assert ret['test'] == '0'
        tmp_collection.drop()
        PYMONGO = True
    except (pymongo.errors.ServerSelectionTimeoutError, AssertionError):
        PYMONGO = False
except ImportError:
    PYMONGO = False


@pytest.mark.skipif(not PYMONGO, reason='test requires the pymongo package and mongodb server')
class TestMongoDict(TestJSONDict):

    @pytest.fixture
    def synced_dict(self, request):
        self._client = MongoClient
        self._name = 'test'
        self._collection = self._client.test_db.test_dict
        yield MongoDict(name=self._name, collection= self._collection)
        self._collection.drop()

    def store(self, data):
        data_to_insert = {'MongoDict::name': self._name, 'data': data}
        self._collection.replace_one({'MongoDict::name': self._name}, data_to_insert)


@pytest.mark.skipif(not PYMONGO, reason='test requires the pymongo package and mongodb server')
class TestMongoList(TestJSONList):

    @pytest.fixture
    def synced_list(self, request):
        self._client = MongoClient
        self._name = 'test'
        self._collection = self._client.test_db.test_list
        yield MongoList(name=self._name, collection=self._collection)
        self._collection.drop()

    def store(self, data):
        data_to_insert = {'MongoList::name': self._name, 'data': data}
        self._collection.replace_one({'MongoList::name': self._name}, data_to_insert)
