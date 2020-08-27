# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from tempfile import TemporaryDirectory

from signac.core.collection_zarr import ZarrDict
from signac.core.collection_zarr import ZarrList
from test_synced_collection import TestJSONDict
from test_synced_collection import TestJSONList

try:
    import zarr
    import numcodecs  # zarr depends on numcodecs
    ZARR = True
except ImportError:
    ZARR = False


@pytest.mark.skipif(not ZARR, reason='test requires the zarr package')
class TestZarrDict(TestJSONDict):

    @pytest.fixture(autouse=True)
    def synced_dict(self):
        self._tmp_dir = TemporaryDirectory(prefix='zarrdict_')
        self._group = zarr.group(zarr.DirectoryStore(self._tmp_dir.name))
        self._name = 'test'
        yield ZarrDict(name=self._name, group=self._group)
        self._tmp_dir.cleanup()

    def store(self, data):
        dataset = self._group.require_dataset(
            'test', overwrite=True, shape=1, dtype='object', object_codec=numcodecs.JSON())
        dataset[0] = data

    @pytest.mark.skip(reason='zarr does not support non-str key type.')
    def test_keys_non_str_valid_type():
        pass


@pytest.mark.skipif(not ZARR, reason='test requires the zarr package')
class TestZarrList(TestJSONList):

    @pytest.fixture(autouse=True)
    def synced_list(self):
        self._tmp_dir = TemporaryDirectory(prefix='zarrlist_')
        self._group = zarr.group(zarr.DirectoryStore(self._tmp_dir.name))
        self._name = 'test'
        yield ZarrList(name=self._name, group=self._group)
        self._tmp_dir.cleanup()

    def store(self, data):
        dataset = self._group.require_dataset(
            'test', overwrite=True, shape=1, dtype='object', object_codec=numcodecs.JSON())
        dataset[0] = data
