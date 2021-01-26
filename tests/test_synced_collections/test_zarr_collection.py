# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from tempfile import TemporaryDirectory

import pytest
from synced_collection_test import SyncedDictTest, SyncedListTest

from signac.synced_collections.collection_zarr import ZarrCollection, ZarrDict, ZarrList

try:
    import numcodecs  # zarr depends on numcodecs
    import zarr

    ZARR = True
except ImportError:
    ZARR = False


class ZarrCollectionTest:

    _backend_collection = ZarrCollection

    def store(self, data):
        dataset = self._group.require_dataset(
            "test",
            overwrite=True,
            shape=1,
            dtype="object",
            object_codec=numcodecs.JSON(),
        )
        dataset[0] = data

    @pytest.fixture(autouse=True)
    def synced_collection(self):
        self._tmp_dir = TemporaryDirectory(prefix="zarr_")
        self._group = zarr.group(zarr.DirectoryStore(self._tmp_dir.name))
        self._name = "test"
        self._backend_kwargs = {"name": self._name, "group": self._group}
        yield self._collection_type(**self._backend_kwargs)
        self._tmp_dir.cleanup()

    @pytest.fixture
    def synced_collection_positional(self):
        """Fixture that initializes the object using positional arguments."""
        self._tmp_dir = TemporaryDirectory(prefix="zarr_")
        self._group = zarr.group(zarr.DirectoryStore(self._tmp_dir.name))
        self._name = "test"
        yield self._collection_type(self._group, self._name)
        self._tmp_dir.cleanup()

    def test_group(self, synced_collection):
        assert synced_collection.group == self._group

    def test_name(self, synced_collection):
        assert synced_collection.name == "test"


@pytest.mark.skipif(not ZARR, reason="test requires the zarr package")
class TestZarrDict(ZarrCollectionTest, SyncedDictTest):
    _collection_type = ZarrDict


@pytest.mark.skipif(not ZARR, reason="test requires the zarr package")
class TestZarrList(ZarrCollectionTest, SyncedListTest):
    _collection_type = ZarrList
