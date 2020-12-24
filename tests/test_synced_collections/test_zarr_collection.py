# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from tempfile import TemporaryDirectory

from signac.core.synced_collections.collection_zarr import ZarrCollection
from signac.core.synced_collections.collection_zarr import ZarrDict
from signac.core.synced_collections.collection_zarr import ZarrList
from synced_collection_test import SyncedDictTest, SyncedListTest

try:
    import zarr
    import numcodecs  # zarr depends on numcodecs
    ZARR = True
except ImportError:
    ZARR = False


class ZarrCollectionTest:

    _backend = 'signac.core.synced_collections.collection_zarr'
    _backend_collection = ZarrCollection

    def store(self, data):
        dataset = self._group.require_dataset(
            'test', overwrite=True, shape=1, dtype='object',
            object_codec=numcodecs.JSON())
        dataset[0] = data

    @pytest.fixture(autouse=True)
    def synced_collection(self):
        self._tmp_dir = TemporaryDirectory(prefix='zarr_')
        self._group = zarr.group(zarr.DirectoryStore(self._tmp_dir.name))
        self._name = 'test'
        self._backend_kwargs = {'name': self._name, 'group': self._group}
        yield self._collection_type(**self._backend_kwargs)
        self._tmp_dir.cleanup()

    @pytest.fixture
    def synced_collection_positional(self):
        """Fixture that initializes the object using positional arguments."""
        self._tmp_dir = TemporaryDirectory(prefix='zarr_')
        self._group = zarr.group(zarr.DirectoryStore(self._tmp_dir.name))
        self._name = 'test'
        yield self._collection_type(self._name, self._group)
        self._tmp_dir.cleanup()


@pytest.mark.skipif(not ZARR, reason='test requires the zarr package')
class TestZarrDict(ZarrCollectionTest, SyncedDictTest):
    _collection_type = ZarrDict


@pytest.mark.skipif(not ZARR, reason='test requires the zarr package')
class TestZarrList(ZarrCollectionTest, SyncedListTest):
    _collection_type = ZarrList
