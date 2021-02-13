# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from synced_collection_test import SyncedDictTest, SyncedListTest

from signac.synced_collections.backends.collection_zarr import ZarrDict, ZarrList

try:
    import numcodecs  # zarr depends on numcodecs
    import zarr

    ZARR = True
except ImportError:
    ZARR = False


class ZarrCollectionTest:

    _name = "test"

    def store(self, synced_collection, data):
        dataset = synced_collection.group.require_dataset(
            "test",
            overwrite=True,
            shape=1,
            dtype="object",
            object_codec=numcodecs.JSON(),
        )
        dataset[0] = data

    @pytest.fixture(autouse=True)
    def synced_collection(self, tmpdir):
        yield self._collection_type(
            name=self._name, group=zarr.group(zarr.DirectoryStore(tmpdir))
        )

    @pytest.fixture
    def synced_collection_positional(self, tmpdir):
        """Fixture that initializes the object using positional arguments."""
        yield self._collection_type(zarr.group(zarr.DirectoryStore(tmpdir)), self._name)

    def test_name(self, synced_collection):
        assert synced_collection.name == self._name


@pytest.mark.skipif(not ZARR, reason="test requires the zarr package")
class TestZarrDict(ZarrCollectionTest, SyncedDictTest):
    _collection_type = ZarrDict


@pytest.mark.skipif(not ZARR, reason="test requires the zarr package")
class TestZarrList(ZarrCollectionTest, SyncedListTest):
    _collection_type = ZarrList
