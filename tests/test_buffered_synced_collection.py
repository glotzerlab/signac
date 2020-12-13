# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import os
import json
from tempfile import TemporaryDirectory

from signac.core.synced_collection import SyncedCollection
from signac.core.collection_json import BufferedJSONDict
from signac.core.collection_json import BufferedJSONList
from signac.core.buffered_synced_collection import buffer_reads_writes
from signac.core.errors import MetadataError, BufferedError

from test_synced_collection import TestJSONDict, TestJSONList

FN_JSON = 'test.json'


class TestJSONCollectionBase:

    # this fixture sets temporary directory for tests
    @pytest.fixture(autouse=True)
    def synced_collection(self):
        self._tmp_dir = TemporaryDirectory(prefix='synced_collection_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        yield
        self._tmp_dir.cleanup()

    def test_from_base_json(self):
        sd = SyncedCollection.from_base(
            filename=self._fn_,
            data={'a': 0}, backend='signac.core.collection_json.buffered')
        assert isinstance(sd, BufferedJSONDict)
        assert 'a' in sd
        assert sd['a'] == 0

    def test_from_base_no_backend(self):
        with pytest.raises(ValueError):
            SyncedCollection.from_base(data={'a': 0})


class TestBufferedJSONDict(TestJSONDict):
    """Tests of buffering JSONDicts."""

    @pytest.fixture
    def synced_dict(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        self._backend_kwargs = {'filename': self._fn_, 'write_concern': False}
        tmp = BufferedJSONDict(**self._backend_kwargs)
        yield tmp
        self._tmp_dir.cleanup()

    def test_buffered(self, synced_dict, testdata):
        """Test basic per-instance buffering behavior."""
        assert len(synced_dict) == 0
        synced_dict['buffered'] = testdata
        assert 'buffered' in synced_dict
        assert synced_dict['buffered'] == testdata
        with synced_dict.buffered():
            assert 'buffered' in synced_dict
            assert synced_dict['buffered'] == testdata
            synced_dict['buffered2'] = 1
            assert 'buffered2' in synced_dict
            assert synced_dict['buffered2'] == 1
        assert len(synced_dict) == 2
        assert 'buffered2' in synced_dict
        assert synced_dict['buffered2'] == 1
        with synced_dict.buffered():
            del synced_dict['buffered']
            assert len(synced_dict) == 1
            assert 'buffered' not in synced_dict
        assert len(synced_dict) == 1
        assert 'buffered' not in synced_dict
        assert 'buffered2' in synced_dict
        assert synced_dict['buffered2'] == 1

        # Explicitly check that the file has not been changed when buffering.
        raw_dict = synced_dict.to_base()
        with synced_dict.buffered():
            synced_dict['buffered3'] = 1
            with open(synced_dict._filename) as f:
                on_disk_dict = json.load(f)
            assert 'buffered3' not in on_disk_dict
            assert on_disk_dict == raw_dict

        with open(synced_dict._filename) as f:
            on_disk_dict = json.load(f)
        assert 'buffered3' in on_disk_dict
        assert on_disk_dict == synced_dict

    def test_two_buffered(self, synced_dict, testdata):
        """Test that a non-buffered copy is not modified."""
        synced_dict['buffered'] = testdata
        synced_dict2 = BufferedJSONDict(filename=synced_dict._filename)

        # Check that the non-buffered object is not modified.
        with synced_dict.buffered():
            synced_dict['buffered2'] = 1
            assert 'buffered2' not in synced_dict2

    def test_two_buffered_modify_unbuffered(self, synced_dict, testdata):
        """Test that in-memory changes raise errors in buffered mode."""
        synced_dict['buffered'] = testdata
        synced_dict2 = BufferedJSONDict(filename=synced_dict._filename)

        # Check that the non-buffered object is not modified.
        with pytest.raises(MetadataError):
            with synced_dict.buffered():
                synced_dict['buffered2'] = 1
                synced_dict2['buffered2'] = 2
                assert synced_dict['buffered2'] == 1
                synced_dict['buffered2'] = 3
                assert synced_dict2['buffered2'] == 2
                synced_dict2['buffered2'] = 3
                assert synced_dict['buffered2'] == 3

        # TODO: If client code catches errors raised due to invalid data in the
        # buffer then attempts to continue, the buffer will be in an invalid
        # state, and any future attempt to leave buffered mode will trigger
        # another flush that will error. I'm not sure what the best way to deal
        # with this problem is. The easiest option is probably to clear the
        # cache whenever an error occurs so that the buffer is back in a valid
        # state.
        from signac.core import buffered_synced_collection
        for backend in buffered_synced_collection._BUFFERED_BACKENDS:
            try:
                backend._cache.clear()
            except AttributeError:
                pass

    def test_two_buffered_modify_unbuffered_first(self, synced_dict, testdata):
        # TODO: What is the expected behavior in this test? Data is only loaded
        # into the buffer the first time anything happens, so if we enter the
        # buffered context for one collection then modify an unbuffered
        # collection within that context _before_ doing anything with the
        # buffered collection, the buffered version will overwrite it. This
        # behavior feels slightly unexpected, but I don't know if there's any
        # way to fix it. For object-local buffering, we could load into the
        # buffer when entering the context instead of waiting until the first
        # call to load, but for global buffering there's no equivalent.
        synced_dict['buffered'] = testdata
        synced_dict2 = BufferedJSONDict(filename=synced_dict._filename)

        # Check that the non-buffered object is not modified.
        # with pytest.raises(
        with synced_dict.buffered():
            synced_dict2['buffered2'] = 1
            assert 'buffered2' not in synced_dict
            synced_dict['buffered2'] = 3
        assert synced_dict == {'buffered': testdata, 'buffered2': 3}

    def test_global_buffered(self, synced_dict, testdata):
        assert len(synced_dict) == 0
        synced_dict['buffered'] = testdata
        assert 'buffered' in synced_dict
        assert synced_dict['buffered'] == testdata
        with buffer_reads_writes():
            assert 'buffered' in synced_dict
            assert synced_dict['buffered'] == testdata
            synced_dict['buffered2'] = 1
            assert 'buffered2' in synced_dict
            assert synced_dict['buffered2'] == 1
        assert len(synced_dict) == 2
        assert 'buffered2' in synced_dict
        assert synced_dict['buffered2'] == 1
        with buffer_reads_writes():
            del synced_dict['buffered']
            assert len(synced_dict) == 1
            assert 'buffered' not in synced_dict
        assert len(synced_dict) == 1
        assert 'buffered' not in synced_dict
        assert 'buffered2' in synced_dict
        assert synced_dict['buffered2'] == 1

        with pytest.raises(BufferedError):
            with buffer_reads_writes():
                synced_dict['buffered2'] = 2
                self.store({'test': 1})
                assert synced_dict['buffered2'] == 2
        assert 'test' in synced_dict
        assert synced_dict['test'] == 1


class TestBufferedJSONList(TestJSONList):
    """Tests of buffering JSONLists."""

    @pytest.fixture
    def synced_list(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsonlist_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        self._backend_kwargs = {'filename': self._fn_}
        yield BufferedJSONList(**self._backend_kwargs)
        self._tmp_dir.cleanup()

    def test_buffered(self, synced_list):
        synced_list.extend([1, 2, 3])
        assert len(synced_list) == 3
        assert synced_list == [1, 2, 3]
        with synced_list.buffered():
            assert len(synced_list) == 3
            assert synced_list == [1, 2, 3]
            synced_list[0] = 4
            assert len(synced_list) == 3
            assert synced_list == [4, 2, 3]
        assert len(synced_list) == 3
        assert synced_list == [4, 2, 3]
        with synced_list.buffered():
            assert len(synced_list) == 3
            assert synced_list == [4, 2, 3]
            del synced_list[0]
            assert len(synced_list) == 2
            assert synced_list == [2, 3]
        assert len(synced_list) == 2
        assert synced_list == [2, 3]

        # Explicitly check that the file has not been changed when buffering.
        raw_list = synced_list.to_base()
        with synced_list.buffered():
            synced_list.append(10)
            with open(synced_list._filename) as f:
                on_disk_list = json.load(f)
            assert 10 not in on_disk_list
            assert on_disk_list == raw_list

        with open(synced_list._filename) as f:
            on_disk_list = json.load(f)
        assert 10 in on_disk_list
        assert on_disk_list == synced_list

    def test_global_buffered(self, synced_list):
        assert len(synced_list) == 0
        with buffer_reads_writes():
            synced_list.reset([1, 2, 3])
            assert len(synced_list) == 3
        assert len(synced_list) == 3
        assert synced_list == [1, 2, 3]
        with buffer_reads_writes():
            assert len(synced_list) == 3
            assert synced_list == [1, 2, 3]
            synced_list[0] = 4
            assert len(synced_list) == 3
            assert synced_list == [4, 2, 3]
        assert len(synced_list) == 3
        assert synced_list == [4, 2, 3]

        # metacheck failure
        with pytest.raises(BufferedError):
            with buffer_reads_writes():
                synced_list.reset([1])
                assert synced_list == [1]
                self.store([1, 2, 3])
                assert synced_list == [1]
        assert len(synced_list) == 3
        assert synced_list == [1, 2, 3]
