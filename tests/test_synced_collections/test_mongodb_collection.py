# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from synced_collection_test import SyncedDictTest, SyncedListTest

from signac.synced_collections.backends.collection_mongodb import (
    MongoDBDict,
    MongoDBList,
)

try:
    import pymongo

    try:
        # Test the mongodb server. Set a short timeout so that tests don't
        # appear to hang while waiting for a connection.
        mongo_client = pymongo.MongoClient(serverSelectionTimeoutMS=1000)
        tmp_collection = mongo_client["test_db"]["test"]
        tmp_collection.insert_one({"test": "0"})
        ret = tmp_collection.find_one({"test": "0"})
        assert ret["test"] == "0"
        tmp_collection.drop()
        PYMONGO = True
    except (pymongo.errors.ServerSelectionTimeoutError, AssertionError):
        PYMONGO = False
except ImportError:
    PYMONGO = False


try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


class MongoDBCollectionTest:

    _uid = {"MongoDBCollection::name": "test"}

    def store(self, synced_collection, data):
        data_to_insert = {**synced_collection.uid, "data": data}
        synced_collection.collection.replace_one(synced_collection.uid, data_to_insert)

    @pytest.fixture
    def synced_collection(self, request):
        yield self._collection_type(
            uid=self._uid, collection=mongo_client.test_db.test_dict
        )
        mongo_client.test_db.test_dict.drop()

    @pytest.fixture
    def synced_collection_positional(self):
        """Fixture that initializes the object using positional arguments."""
        yield self._collection_type(mongo_client.test_db.test_dict, self._uid)
        mongo_client.test_db.test_dict.drop()

    def test_uid(self, synced_collection):
        assert synced_collection.uid == self._uid


@pytest.mark.skipif(
    not PYMONGO, reason="test requires the pymongo package and mongodb server"
)
class TestMongoDBDict(MongoDBCollectionTest, SyncedDictTest):
    # BSON does not support >8-byte ints, and these types may be larger
    # depending on the architecture being tested. Programatically remove larger
    # types since this may be architecture-dependent.
    if NUMPY:
        NUMPY_INT_TYPES = [
            dtype
            for dtype in SyncedListTest.NUMPY_INT_TYPES
            if isinstance(dtype, numpy.number)
            and numpy.log2(numpy.iinfo(dtype).max) / 8 <= 8
        ]

    _collection_type = MongoDBDict


@pytest.mark.skipif(
    not PYMONGO, reason="test requires the pymongo package and mongodb server"
)
class TestMongoDBList(MongoDBCollectionTest, SyncedListTest):
    # BSON does not support >8-byte ints, and these types may be larger
    # depending on the architecture being tested. Programatically remove larger
    # types since this may be architecture-dependent.
    if NUMPY:
        NUMPY_INT_TYPES = [
            dtype
            for dtype in SyncedListTest.NUMPY_INT_TYPES
            if isinstance(dtype, numpy.number)
            and numpy.log2(numpy.iinfo(dtype).max) / 8 <= 8
        ]

    _collection_type = MongoDBList
