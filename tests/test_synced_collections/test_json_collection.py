# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import os
from tempfile import TemporaryDirectory

import pytest
from attr_dict_test import AttrDictTest, AttrListTest
from synced_collection_test import SyncedDictTest, SyncedListTest

from signac.synced_collections.backends.collection_json import (
    JSONAttrDict,
    JSONAttrList,
    JSONCollection,
    JSONDict,
    JSONList,
)


class JSONCollectionTest:

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

    # The following test tests the support for non-str keys
    # for JSON backend which will be removed in version 2.0.
    # See issue: https://github.com/glotzerlab/signac/issues/316.
    def test_keys_non_str_valid_type(self, synced_collection, testdata):
        for key in (0, None, True):
            with pytest.deprecated_call(match="Use of.+as key is deprecated"):
                synced_collection[key] = testdata
            assert str(key) in synced_collection
            assert synced_collection[str(key)] == testdata


class TestJSONList(JSONCollectionTest, SyncedListTest):

    _collection_type = JSONList


class TestJSONDictWriteConcern(TestJSONDict):
    _write_concern = True


class TestJSONListWriteConcern(TestJSONList):
    _write_concern = True


class TestJSONAttrDict(TestJSONDict, AttrDictTest):

    _collection_type = JSONAttrDict


class TestJSONAttrList(TestJSONList, AttrListTest):

    _collection_type = JSONAttrList
