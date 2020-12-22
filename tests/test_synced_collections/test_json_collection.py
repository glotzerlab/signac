# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import os
import json
from tempfile import TemporaryDirectory

from signac.core.synced_collections.collection_json import JSONCollection
from signac.core.synced_collections.collection_json import JSONDict
from signac.core.synced_collections.collection_json import JSONList

from synced_collection_test import SyncedDictTest, SyncedListTest


class JSONCollectionTest:

    _backend = 'signac.core.synced_collections.collection_json'
    _backend_collection = JSONCollection
    _write_concern = False

    def store(self, data):
        with open(self._fn_, 'wb') as file:
            file.write(json.dumps(data).encode())

    @pytest.fixture(autouse=True)
    def synced_collection(self):
        self._tmp_dir = TemporaryDirectory(prefix='json_')
        self._fn_ = os.path.join(self._tmp_dir.name, 'test.json')
        self._backend_kwargs = {
            'filename': self._fn_, 'write_concern': self._write_concern
        }
        yield self._collection_type(**self._backend_kwargs)
        self._tmp_dir.cleanup()


class TestJSONDict(JSONCollectionTest, SyncedDictTest):

    _collection_type = JSONDict


class TestJSONList(JSONCollectionTest, SyncedListTest):

    _collection_type = JSONList


class TestJSONDictWriteConcern(TestJSONDict):
    _write_concern = True


class TestJSONListWriteConcern(TestJSONList):
    _write_concern = True
