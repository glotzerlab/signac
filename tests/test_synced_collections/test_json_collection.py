# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import os
from tempfile import TemporaryDirectory

import pytest
from synced_collection_test import SyncedDictTest, SyncedListTest

from signac.core.synced_collections.collection_json import (
    JSONCollection,
    JSONDict,
    JSONList,
)


class JSONCollectionTest:

    _backend = "signac.core.synced_collections.collection_json"
    _backend_collection = JSONCollection
    _write_concern = False
    _fn = "test.json"

    def store(self, data):
        with open(self._fn_, "wb") as file:
            file.write(json.dumps(data).encode())

    @pytest.fixture(autouse=True)
    def synced_collection(self):
        self._tmp_dir = TemporaryDirectory(prefix="json_")
        self._fn_ = os.path.join(self._tmp_dir.name, self._fn)
        self._backend_kwargs = {
            "filename": self._fn_,
            "write_concern": self._write_concern,
        }
        yield self._collection_type(**self._backend_kwargs)
        self._tmp_dir.cleanup()

    @pytest.fixture
    def synced_collection_positional(self):
        """Fixture that initializes the object using positional arguments."""
        self._tmp_dir = TemporaryDirectory(prefix="json_")
        self._fn_ = os.path.join(self._tmp_dir.name, "test2.json")
        yield self._collection_type(self._fn_, self._write_concern)
        self._tmp_dir.cleanup()

    def test_filename(self, synced_collection):
        assert os.path.basename(synced_collection.filename) == self._fn


class TestJSONDict(JSONCollectionTest, SyncedDictTest):

    _collection_type = JSONDict


class TestJSONList(JSONCollectionTest, SyncedListTest):

    _collection_type = JSONList


class TestJSONDictWriteConcern(TestJSONDict):
    _write_concern = True


class TestJSONListWriteConcern(TestJSONList):
    _write_concern = True
