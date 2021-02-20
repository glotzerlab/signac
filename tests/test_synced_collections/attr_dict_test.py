# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Tests to be used for dictionaries supporting attr-based access."""

import pytest

from signac.errors import InvalidKeyError, KeyTypeError


class AttrDictTest:
    def test_attr_dict(self, synced_collection, testdata):
        key = "test"
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert key in synced_collection
        assert synced_collection[key] == testdata
        assert synced_collection.get(key) == testdata
        assert synced_collection.test == testdata
        del synced_collection.test
        assert len(synced_collection) == 0
        assert key not in synced_collection
        key = "test2"
        synced_collection.test2 = testdata
        assert len(synced_collection) == 1
        assert key in synced_collection
        assert synced_collection[key] == testdata
        assert synced_collection.get(key) == testdata
        assert synced_collection.test2 == testdata
        with pytest.raises(AttributeError):
            synced_collection.not_exist

        # deleting a protected attribute
        synced_collection._load()
        del synced_collection._root
        # deleting _root will lead to recursion as _root is treated as key
        # _load() will check for _root and __getattr__ will call __getitem__
        # which calls _load()
        with pytest.raises(RecursionError):
            synced_collection._load()

    def test_keys_with_dots(self, synced_collection):
        with pytest.raises(InvalidKeyError):
            synced_collection["a.b"] = None
        with pytest.raises(KeyTypeError):
            synced_collection[0.0] = None


class AttrListTest:
    """Test that dicts contained in AttrList classes are AttrDicts."""

    def test_attr_list(self, synced_collection, testdata):
        synced_collection.append({})
        nested_synced_dict = synced_collection[0]

        key = "test"
        nested_synced_dict[key] = testdata
        assert len(nested_synced_dict) == 1
        assert key in nested_synced_dict
        assert nested_synced_dict[key] == testdata
        assert nested_synced_dict.get(key) == testdata
        assert nested_synced_dict.test == testdata
        del nested_synced_dict.test
        assert len(nested_synced_dict) == 0
        assert key not in nested_synced_dict
        key = "test2"
        nested_synced_dict.test2 = testdata
        assert len(nested_synced_dict) == 1
        assert key in nested_synced_dict
        assert nested_synced_dict[key] == testdata
        assert nested_synced_dict.get(key) == testdata
        assert nested_synced_dict.test2 == testdata
        with pytest.raises(AttributeError):
            nested_synced_dict.not_exist
