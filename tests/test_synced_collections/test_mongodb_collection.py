# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from synced_collection_test import SyncedDictTest, SyncedListTest

from signac.core.synced_collections.collection_mongodb import (
    MongoDBCollection,
    MongoDBDict,
    MongoDBList,
)

try:
    import pymongo

    try:
        # Test the mongodb server. Set a short timeout so that tests don't
        # appear to hang while waiting for a connection.
        MongoClient = pymongo.MongoClient(serverSelectionTimeoutMS=1000)
        tmp_collection = MongoClient["test_db"]["test"]
        tmp_collection.insert_one({"test": "0"})
        ret = tmp_collection.find_one({"test": "0"})
        assert ret["test"] == "0"
        tmp_collection.drop()
        PYMONGO = True
    except (pymongo.errors.ServerSelectionTimeoutError, AssertionError):
        PYMONGO = False
except ImportError:
    PYMONGO = False


class MongoDBCollectionTest:

    _backend_collection = MongoDBCollection

    def store(self, data):
        data_to_insert = {**self._uid, "data": data}
        self._collection.replace_one(self._uid, data_to_insert)

    @pytest.fixture(autouse=True)
    def synced_collection(self, request):
        self._client = MongoClient
        self._uid = {"MongoDBCollection::name": "test"}
        self._collection = self._client.test_db.test_dict
        self._backend_kwargs = {"uid": self._uid, "collection": self._collection}
        yield self._collection_type(**self._backend_kwargs)
        self._collection.drop()

    @pytest.fixture
    def synced_collection_positional(self):
        """Fixture that initializes the object using positional arguments."""
        self._client = MongoClient
        self._uid = {"MongoDBCollection::name": "test"}
        self._collection = self._client.test_db.test_dict
        yield self._collection_type(self._collection, self._uid)

    def test_collection(self, synced_collection):
        assert synced_collection.collection == self._collection

    def test_uid(self, synced_collection):
        assert synced_collection.uid == {"MongoDBCollection::name": "test"}


@pytest.mark.skipif(
    not PYMONGO, reason="test requires the pymongo package and mongodb server"
)
class TestMongoDBDict(MongoDBCollectionTest, SyncedDictTest):

    _collection_type = MongoDBDict


@pytest.mark.skipif(
    not PYMONGO, reason="test requires the pymongo package and mongodb server"
)
class TestMongoDBList(MongoDBCollectionTest, SyncedListTest):

    _collection_type = MongoDBList
