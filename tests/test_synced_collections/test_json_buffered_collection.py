# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import itertools
import json
import os
import time
from tempfile import TemporaryDirectory

import pytest
from test_json_collection import JSONCollectionTest, TestJSONDict, TestJSONList

from signac.core.synced_collections.buffered_collection import buffer_all
from signac.core.synced_collections.collection_json import (
    BufferedJSONCollection,
    BufferedJSONDict,
    BufferedJSONList,
    MemoryBufferedJSONCollection,
    MemoryBufferedJSONDict,
    MemoryBufferedJSONList,
)
from signac.core.synced_collections.errors import BufferedError, MetadataError


class BufferedJSONCollectionTest(JSONCollectionTest):

    _backend_collection = BufferedJSONCollection

    def load(self, collection):
        """Load the data corresponding to a SyncedCollection from disk."""
        with open(collection._filename) as f:
            return json.load(f)


class TestBufferedJSONDict(BufferedJSONCollectionTest, TestJSONDict):
    """Tests of buffering JSONDicts."""

    _collection_type = BufferedJSONDict  # type: ignore

    @pytest.fixture
    def synced_collection2(self):
        _tmp_dir2 = TemporaryDirectory(prefix="jsondict2_")
        _fn_2 = os.path.join(_tmp_dir2.name, "test2.json")
        _backend_kwargs2 = {"filename": _fn_2, "write_concern": False}
        tmp = self._collection_type(**_backend_kwargs2)
        yield tmp
        _tmp_dir2.cleanup()

    def test_buffered(self, synced_collection, testdata):
        """Test basic per-instance buffering behavior."""
        assert len(synced_collection) == 0
        synced_collection["buffered"] = testdata
        assert "buffered" in synced_collection
        assert synced_collection["buffered"] == testdata
        with synced_collection.buffered():
            assert "buffered" in synced_collection
            assert synced_collection["buffered"] == testdata
            synced_collection["buffered2"] = 1
            assert "buffered2" in synced_collection
            assert synced_collection["buffered2"] == 1
        assert len(synced_collection) == 2
        assert "buffered2" in synced_collection
        assert synced_collection["buffered2"] == 1
        with synced_collection.buffered():
            del synced_collection["buffered"]
            assert len(synced_collection) == 1
            assert "buffered" not in synced_collection
        assert len(synced_collection) == 1
        assert "buffered" not in synced_collection
        assert "buffered2" in synced_collection
        assert synced_collection["buffered2"] == 1

        # Explicitly check that the file has not been changed when buffering.
        raw_dict = synced_collection()
        with synced_collection.buffered():
            synced_collection["buffered3"] = 1
            on_disk_dict = self.load(synced_collection)
            assert "buffered3" not in on_disk_dict
            assert on_disk_dict == raw_dict

        on_disk_dict = self.load(synced_collection)
        assert "buffered3" in on_disk_dict
        assert on_disk_dict == synced_collection

    def test_two_buffered(self, synced_collection, testdata):
        """Test that a non-buffered copy is not modified."""
        synced_collection["buffered"] = testdata
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with synced_collection.buffered():
            synced_collection["buffered2"] = 1
            assert "buffered2" not in synced_collection2

    def test_two_buffered_modify_unbuffered(self, synced_collection, testdata):
        """Test that in-memory changes raise errors in buffered mode."""
        synced_collection["buffered"] = testdata
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with pytest.raises(MetadataError):
            with synced_collection.buffered():
                synced_collection["buffered2"] = 1
                synced_collection2["buffered2"] = 2
                assert synced_collection["buffered2"] == 1
                synced_collection["buffered2"] = 3
                assert synced_collection2["buffered2"] == 2
                synced_collection2["buffered2"] = 3
                assert synced_collection["buffered2"] == 3

    def test_two_buffered_modify_unbuffered_first(self, synced_collection, testdata):
        # TODO: What is the expected behavior in this test? Data is only loaded
        # into the buffer the first time anything happens, so if we enter the
        # buffered context for one collection then modify an unbuffered
        # collection within that context _before_ doing anything with the
        # buffered collection, the buffered version will overwrite it. This
        # behavior feels slightly unexpected, but I don't know if there's any
        # way to fix it. For object-local buffering, we could load into the
        # buffer when entering the context instead of waiting until the first
        # call to load, but for global buffering there's no equivalent.
        synced_collection["buffered"] = testdata
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with synced_collection.buffered():
            synced_collection2["buffered2"] = 1
            assert "buffered2" not in synced_collection
            synced_collection["buffered2"] = 3
        assert synced_collection == {"buffered": testdata, "buffered2": 3}

    def test_global_buffered(self, synced_collection, testdata):
        assert len(synced_collection) == 0
        synced_collection["buffered"] = testdata
        assert "buffered" in synced_collection
        assert synced_collection["buffered"] == testdata
        with buffer_all():
            assert "buffered" in synced_collection
            assert synced_collection["buffered"] == testdata
            synced_collection["buffered2"] = 1
            assert "buffered2" in synced_collection
            assert synced_collection["buffered2"] == 1
        assert len(synced_collection) == 2
        assert "buffered2" in synced_collection
        assert synced_collection["buffered2"] == 1
        with buffer_all():
            del synced_collection["buffered"]
            assert len(synced_collection) == 1
            assert "buffered" not in synced_collection
        assert len(synced_collection) == 1
        assert "buffered" not in synced_collection
        assert "buffered2" in synced_collection
        assert synced_collection["buffered2"] == 1

        with pytest.raises(BufferedError):
            with buffer_all():
                synced_collection["buffered2"] = 2
                self.store({"test": 1})
                assert synced_collection["buffered2"] == 2
        assert "test" in synced_collection
        assert synced_collection["test"] == 1

    def test_nested_same_collection(self, synced_collection):
        """Test nesting global buffering."""
        assert len(synced_collection) == 0

        for outer_buffer, inner_buffer in itertools.product(
            [synced_collection.buffered, buffer_all], repeat=2
        ):
            err_msg = (
                f"outer_buffer: {outer_buffer.__qualname__}, "
                f"inner_buffer: {inner_buffer.__qualname__}"
            )
            synced_collection.reset({"outside": 1})
            with outer_buffer():
                synced_collection["inside_first"] = 2
                with inner_buffer():
                    synced_collection["inside_second"] = 3

                on_disk_dict = self.load(synced_collection)
                assert "inside_first" not in on_disk_dict, err_msg
                assert "inside_second" not in on_disk_dict, err_msg
                assert "inside_first" in synced_collection, err_msg
                assert "inside_second" in synced_collection, err_msg

        assert self.load(synced_collection) == synced_collection

    def test_nested_different_collections(self, synced_collection, synced_collection2):
        """Test nested buffering for different collections."""
        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0

        synced_collection["outside"] = 1
        synced_collection2["outside"] = 1
        with synced_collection.buffered():
            synced_collection["inside_first"] = 2
            on_disk_dict = self.load(synced_collection)
            assert "inside_first" in synced_collection
            assert "inside_first" not in on_disk_dict

            synced_collection2["inside_first"] = 2
            on_disk_dict2 = self.load(synced_collection2)
            assert "inside_first" in synced_collection2
            assert "inside_first" in on_disk_dict2

            with buffer_all():
                synced_collection["inside_second"] = 3
                synced_collection2["inside_second"] = 3

                on_disk_dict = self.load(synced_collection)
                assert "inside_second" in synced_collection
                assert "inside_second" not in on_disk_dict
                on_disk_dict2 = self.load(synced_collection2)
                assert "inside_second" in synced_collection2
                assert "inside_second" not in on_disk_dict2

            on_disk_dict = self.load(synced_collection)
            on_disk_dict2 = self.load(synced_collection2)

            assert "inside_first" in synced_collection
            assert "inside_first" not in on_disk_dict

            assert "inside_second" in synced_collection
            assert "inside_second" not in on_disk_dict
            assert "inside_second" in synced_collection2
            assert "inside_second" in on_disk_dict2

        on_disk_dict = self.load(synced_collection)
        on_disk_dict2 = self.load(synced_collection2)

        assert "inside_first" in synced_collection
        assert "inside_first" in on_disk_dict

        assert "inside_second" in synced_collection
        assert "inside_second" in on_disk_dict
        assert "inside_second" in synced_collection2
        assert "inside_second" in on_disk_dict2

    def test_nested_copied_collection(self, synced_collection):
        """Test modifying two collections pointing to the same data."""
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0

        synced_collection["outside"] = 1
        with synced_collection.buffered():
            synced_collection["inside_first"] = 2

            on_disk_dict = self.load(synced_collection)
            assert synced_collection["inside_first"] == 2
            assert "inside_first" not in on_disk_dict

            with buffer_all():
                synced_collection["inside_second"] = 3
                synced_collection2["inside_second"] = 4

                on_disk_dict = self.load(synced_collection)
                assert synced_collection["inside_second"] == 4
                assert synced_collection2["inside_second"] == 4
                assert "inside_second" not in on_disk_dict

            on_disk_dict = self.load(synced_collection)
            assert on_disk_dict["inside_second"] == 4

    @pytest.mark.skip("Not currently sure what the expected behavior is.")
    def test_nested_copied_collection_invalid(self, synced_collection):
        """Test the behavior of invalid modifications of copied objects."""
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0

        synced_collection["outside"] = 1
        finished = False
        with pytest.raises(MetadataError):
            with synced_collection.buffered():
                synced_collection["inside_first"] = 2
                # TODO: Currently, modifying synced_collection2 here causes
                # problems.  It is unbuffered, so it directly writes to file.
                # Then, when entering global buffering in the context below,
                # synced_collection2 sees that synced_collection has already
                # saved data for this file to the buffer, so it loads that
                # data, which also means that synced_collection2 becomes
                # associated with the metadata stored when synced_collection
                # entered buffered mode. As a result, when the global buffering
                # exits, we see metadata errors because synced_collection2 lost
                # track of the fact that it saved changes to filemade prior to
                # entering the global buffer. We _could_ fix this by changing
                # the behavior of _load_buffer to not load the data from the
                # cache if it exists, if the object is new to
                # cached_collections then we would save a new version. However,
                # I'm not sure that's the correct answer. Is there a true
                # canonical source of truth in this scenario?
                synced_collection2["inside_first"] = 3

                on_disk_dict = self.load(synced_collection)
                assert synced_collection["inside_first"] == 2
                assert on_disk_dict["inside_first"] == 3

                with buffer_all():
                    synced_collection["inside_second"] = 3
                    synced_collection2["inside_second"] = 4

                    on_disk_dict = self.load(synced_collection)
                    assert synced_collection["inside_second"] == 4
                    assert synced_collection2["inside_second"] == 4
                    assert "inside_second" not in on_disk_dict

                on_disk_dict = self.load(synced_collection)
                assert on_disk_dict["inside_second"] == 4
                # Check that all the checks ran before the assertion failure.
                finished = True
        assert finished

    def test_buffer_flush(self, synced_collection):
        """Test that the buffer gets flushed when enough data is written."""
        original_buffer_capacity = self._collection_type.get_buffer_capacity()

        assert self._collection_type.get_current_buffer_size() == 0
        self._collection_type.set_buffer_capacity(20)

        # Ensure that the file exists on disk by executing a clear operation so
        # that load operations work as expected.
        assert len(synced_collection) == 0
        synced_collection.clear()

        with synced_collection.buffered():
            synced_collection["foo"] = 1
            assert self._collection_type.get_current_buffer_size() == len(
                repr(synced_collection)
            )
            assert synced_collection != self.load(synced_collection)

            # Add a long enough value to force a flush.
            synced_collection["bar"] = 100
            assert self._collection_type.get_current_buffer_size() == 0

            # Make sure the file on disk now matches.
            assert synced_collection == self.load(synced_collection)

        # Reset buffer capacity for other tests.
        self._collection_type.set_buffer_capacity(original_buffer_capacity)


class TestBufferedJSONList(BufferedJSONCollectionTest, TestJSONList):
    """Tests of buffering JSONLists."""

    _collection_type = BufferedJSONList  # type: ignore

    def test_buffered(self, synced_collection):
        synced_collection.extend([1, 2, 3])
        assert len(synced_collection) == 3
        assert synced_collection == [1, 2, 3]
        with synced_collection.buffered():
            assert len(synced_collection) == 3
            assert synced_collection == [1, 2, 3]
            synced_collection[0] = 4
            assert len(synced_collection) == 3
            assert synced_collection == [4, 2, 3]
        assert len(synced_collection) == 3
        assert synced_collection == [4, 2, 3]
        with synced_collection.buffered():
            assert len(synced_collection) == 3
            assert synced_collection == [4, 2, 3]
            del synced_collection[0]
            assert len(synced_collection) == 2
            assert synced_collection == [2, 3]
        assert len(synced_collection) == 2
        assert synced_collection == [2, 3]

        # Explicitly check that the file has not been changed when buffering.
        raw_list = synced_collection()
        with synced_collection.buffered():
            synced_collection.append(10)
            on_disk_list = self.load(synced_collection)
            assert 10 not in on_disk_list
            assert on_disk_list == raw_list

        on_disk_list = self.load(synced_collection)
        assert 10 in on_disk_list
        assert on_disk_list == synced_collection

    def test_global_buffered(self, synced_collection):
        assert len(synced_collection) == 0
        with buffer_all():
            synced_collection.reset([1, 2, 3])
            assert len(synced_collection) == 3
        assert len(synced_collection) == 3
        assert synced_collection == [1, 2, 3]
        with buffer_all():
            assert len(synced_collection) == 3
            assert synced_collection == [1, 2, 3]
            synced_collection[0] = 4
            assert len(synced_collection) == 3
            assert synced_collection == [4, 2, 3]
        assert len(synced_collection) == 3
        assert synced_collection == [4, 2, 3]

        # metacheck failure
        with pytest.raises(BufferedError):
            with buffer_all():
                synced_collection.reset([1])
                assert synced_collection == [1]
                # Unfortunately the resolution of os.stat is
                # platform dependent and may not always be
                # high enough for our check to work. Since
                # this unit test is artificially simple we
                # must add some amount of minimum waiting time
                # to ensure that the change in time will be
                # detected.
                time.sleep(0.01)
                self.store([1, 2, 3])
                assert synced_collection == [1]
        assert len(synced_collection) == 3
        assert synced_collection == [1, 2, 3]


class TestMemoryBufferedJSONDict(TestBufferedJSONDict):
    """Tests of MemoryBufferedJSONDicts."""

    _backend_collection = MemoryBufferedJSONCollection  # type: ignore
    _collection_type = MemoryBufferedJSONDict  # type: ignore

    def test_buffer_flush(self, synced_collection, synced_collection2):
        """Test that the buffer gets flushed when enough data is written."""
        return


class TestMemoryBufferedJSONList(TestBufferedJSONList):
    """Tests of MemoryBufferedJSONLists."""

    _backend_collection = MemoryBufferedJSONCollection  # type: ignore
    _collection_type = MemoryBufferedJSONList  # type: ignore


class TestBufferedJSONDictWriteConcern(TestBufferedJSONDict):
    _write_concern = True


class TestBufferedJSONListWriteConcern(TestBufferedJSONList):
    _write_concern = True
