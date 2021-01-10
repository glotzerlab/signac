# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import itertools
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory

import pytest
from test_json_collection import JSONCollectionTest, TestJSONDict, TestJSONList

from signac.core.synced_collections.buffered_collection import (
    BufferedCollection,
    buffer_all,
)
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
        with open(collection.filename) as f:
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
        with synced_collection.buffered:
            assert "buffered" in synced_collection
            assert synced_collection["buffered"] == testdata
            synced_collection["buffered2"] = 1
            assert "buffered2" in synced_collection
            assert synced_collection["buffered2"] == 1
        assert len(synced_collection) == 2
        assert "buffered2" in synced_collection
        assert synced_collection["buffered2"] == 1
        with synced_collection.buffered:
            del synced_collection["buffered"]
            assert len(synced_collection) == 1
            assert "buffered" not in synced_collection
        assert len(synced_collection) == 1
        assert "buffered" not in synced_collection
        assert "buffered2" in synced_collection
        assert synced_collection["buffered2"] == 1

        # Explicitly check that the file has not been changed when buffering.
        raw_dict = synced_collection()
        with synced_collection.buffered:
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
        with synced_collection.buffered:
            synced_collection["buffered2"] = 1
            assert "buffered2" not in synced_collection2

    def test_two_buffered_modify_unbuffered(self, synced_collection, testdata):
        """Test that in-memory changes raise errors in buffered mode."""
        synced_collection["buffered"] = testdata
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with pytest.raises(MetadataError):
            with synced_collection.buffered:
                synced_collection["buffered2"] = 1
                synced_collection2["buffered2"] = 2
                assert synced_collection["buffered2"] == 1
                synced_collection["buffered2"] = 3
                assert synced_collection2["buffered2"] == 2
                synced_collection2["buffered2"] = 3
                assert synced_collection["buffered2"] == 3

    def test_two_buffered_modify_unbuffered_first(self, synced_collection, testdata):
        synced_collection["buffered"] = testdata
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with synced_collection.buffered:
            synced_collection2["buffered2"] = 1
            assert "buffered2" in synced_collection
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
            [synced_collection.buffered, BufferedCollection.buffer_all], repeat=2
        ):
            err_msg = (
                f"outer_buffer: {type(outer_buffer).__qualname__}, "
                f"inner_buffer: {type(inner_buffer).__qualname__}"
            )
            synced_collection.reset({"outside": 1})
            with outer_buffer:
                synced_collection["inside_first"] = 2
                with inner_buffer:
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
        with synced_collection.buffered:
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
        with synced_collection.buffered:
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

    @pytest.mark.skip("This is an example of unsupported (and undefined) behavior).")
    def test_nested_copied_collection_invalid(self, synced_collection):
        """Test the behavior of invalid modifications of copied objects."""
        synced_collection2 = self._collection_type(filename=synced_collection._filename)

        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0

        synced_collection["outside"] = 1
        finished = False
        with pytest.raises(MetadataError):
            with synced_collection.buffered:
                synced_collection["inside_first"] = 2
                # Modifying synced_collection2 here causes problems. It is
                # unbuffered, so it directly writes to file.  Then, when
                # entering global buffering in the context below,
                # synced_collection2 sees that synced_collection has already
                # saved data for this file to the buffer, so it loads that
                # data, which also means that synced_collection2 becomes
                # associated with the metadata stored when synced_collection
                # entered buffered mode. As a result, when the global buffering
                # exits, we see metadata errors because synced_collection2 lost
                # track of the fact that it saved changes to the file made
                # prior to entering the global buffer. While this case could be
                # given some specific behavior, there's no obvious canonical
                # source of truth here, so we simply choose to skip it
                # altogether.
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

        try:
            self._collection_type.set_buffer_capacity(20)

            # Ensure that the file exists on disk by executing a clear operation so
            # that load operations work as expected.
            assert len(synced_collection) == 0
            synced_collection.clear()

            with synced_collection.buffered:
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
        finally:
            # Reset buffer capacity for other tests.
            self._collection_type.set_buffer_capacity(original_buffer_capacity)

            # To avoid confusing test failures later, make sure the buffer is
            # truly flushed correctly.
            assert self._collection_type.get_current_buffer_size() == 0

    def multithreaded_buffering_test(self, op):
        """Test that buffering in a multithreaded context is safe for different operations.

        This method encodes the logic for the test, but can be used to test different
        operations on the dict.
        """
        original_buffer_capacity = self._collection_type.get_buffer_capacity()
        try:
            # Choose some arbitrarily low value that will ensure intermittent
            # forced buffer flushes.
            new_buffer_capacity = 20
            self._collection_type.set_buffer_capacity(new_buffer_capacity)

            with TemporaryDirectory(
                prefix="jsondict_buffered_multithreaded"
            ) as tmp_dir:
                with buffer_all():
                    num_dicts = 100
                    dicts = []
                    dict_data = []
                    for i in range(num_dicts):
                        fn = os.path.join(tmp_dir, f"test_dict{i}.json")
                        dicts.append(self._collection_type(filename=fn))
                        dict_data.append({str(j): j for j in range(i)})

                    num_threads = 10
                    try:
                        with ThreadPoolExecutor(max_workers=num_threads) as executor:
                            list(executor.map(op, dicts, dict_data))
                    except KeyError as e:
                        raise RuntimeError(
                            "Buffering in parallel failed due to different threads "
                            "simultaneously modifying the buffer."
                        ) from e

                    # First validate inside buffer.
                    assert all(dicts[i] == dict_data[i] for i in range(num_dicts))
                # Now validate outside buffer.
                assert all(dicts[i] == dict_data[i] for i in range(num_dicts))
        finally:
            # Reset buffer capacity for other tests in case this fails.
            self._collection_type.set_buffer_capacity(original_buffer_capacity)

            # To avoid confusing test failures later, make sure the buffer is
            # truly flushed correctly.
            assert self._collection_type.get_current_buffer_size() == 0

    def test_multithreaded_buffering_setitem(self):
        """Test setitem in a multithreaded buffering context."""

        def setitem_dict(sd, data):
            for k, v in data.items():
                sd[k] = v

        self.multithreaded_buffering_test(setitem_dict)

    def test_multithreaded_buffering_update(self):
        """Test update in a multithreaded buffering context."""

        def update_dict(sd, data):
            sd.update(data)

        self.multithreaded_buffering_test(update_dict)

    def test_multithreaded_buffering_reset(self):
        """Test reset in a multithreaded buffering context."""

        def reset_dict(sd, data):
            sd.reset(data)

        self.multithreaded_buffering_test(reset_dict)

    def test_multithreaded_buffering_clear(self):
        """Test clear in a multithreaded buffering context.

        Since clear requires ending up with an empty dict, it's easier to
        write a separate test from the others.
        """
        original_buffer_capacity = self._collection_type.get_buffer_capacity()
        try:
            # Choose some arbitrarily low value that will ensure intermittent
            # forced buffer flushes.
            new_buffer_capacity = 20
            self._collection_type.set_buffer_capacity(new_buffer_capacity)

            with TemporaryDirectory(
                prefix="jsondict_buffered_multithreaded"
            ) as tmp_dir:
                # Initialize the data outside the buffered context so that it's
                # already present on disk for testing both.
                num_dicts = 100
                dicts = []
                for i in range(num_dicts):
                    fn = os.path.join(tmp_dir, f"test_dict{i}.json")
                    dicts.append(self._collection_type(filename=fn))
                    dicts[-1].update({str(j): j for j in range(i)})

                with buffer_all():
                    num_threads = 10
                    try:
                        with ThreadPoolExecutor(max_workers=num_threads) as executor:
                            list(executor.map(lambda sd: sd.clear(), dicts))
                    except KeyError as e:
                        raise RuntimeError(
                            "Buffering in parallel failed due to different threads "
                            "simultaneously modifying the buffer."
                        ) from e

                    # First validate inside buffer.
                    assert all(not dicts[i] for i in range(num_dicts))
                # Now validate outside buffer.
                assert all(not dicts[i] for i in range(num_dicts))
        finally:
            # Reset buffer capacity for other tests in case this fails.
            self._collection_type.set_buffer_capacity(original_buffer_capacity)

            # To avoid confusing test failures later, make sure the buffer is
            # truly flushed correctly.
            assert self._collection_type.get_current_buffer_size() == 0

    def test_multithreaded_buffering_load(self):
        """Test loading data in a multithreaded buffering context.

        This test is primarily for verifying that multithreaded buffering does
        not lead to concurrency errors in flushing data from the buffer due to
        too many loads. This test is primarily for buffering methods with a maximum
        capacity, even for read-only operations.
        """
        original_buffer_capacity = self._collection_type.get_buffer_capacity()
        try:
            # Choose some arbitrarily low value that will ensure intermittent
            # forced buffer flushes.
            new_buffer_capacity = 1000
            self._collection_type.set_buffer_capacity(new_buffer_capacity)

            with TemporaryDirectory(
                prefix="jsondict_buffered_multithreaded"
            ) as tmp_dir:
                # Must initialize the data outside the buffered context so that
                # we only execute read operations inside the buffered context.
                num_dicts = 100
                dicts = []
                for i in range(num_dicts):
                    fn = os.path.join(tmp_dir, f"test_dict{i}.json")
                    dicts.append(self._collection_type(filename=fn))
                    # Go to i+1 so that every dict contains the 0 element.
                    dicts[-1].update({str(j): j for j in range(i + 1)})

                with buffer_all():
                    num_threads = 100
                    try:
                        with ThreadPoolExecutor(max_workers=num_threads) as executor:
                            list(executor.map(lambda sd: sd["0"], dicts * 5))
                    except KeyError as e:
                        raise RuntimeError(
                            "Buffering in parallel failed due to different threads "
                            "simultaneously modifying the buffer."
                        ) from e

        finally:
            # Reset buffer capacity for other tests in case this fails.
            self._collection_type.set_buffer_capacity(original_buffer_capacity)

            # To avoid confusing test failures later, make sure the buffer is
            # truly flushed correctly.
            assert self._collection_type.get_current_buffer_size() == 0

    def test_buffer_first_load(self, synced_collection):
        """Ensure that existing data is preserved if the first load is in buffered mode."""
        fn = synced_collection.filename
        write_concern = self._write_concern

        sc = self._collection_type(fn, write_concern)
        sc["foo"] = 1
        sc["bar"] = 2
        del sc

        sc = self._collection_type(fn, write_concern)
        with sc.buffered:
            sc["foo"] = 3

        assert "bar" in sc


class TestBufferedJSONList(BufferedJSONCollectionTest, TestJSONList):
    """Tests of buffering JSONLists."""

    _collection_type = BufferedJSONList  # type: ignore

    def test_buffered(self, synced_collection):
        synced_collection.extend([1, 2, 3])
        assert len(synced_collection) == 3
        assert synced_collection == [1, 2, 3]
        with synced_collection.buffered:
            assert len(synced_collection) == 3
            assert synced_collection == [1, 2, 3]
            synced_collection[0] = 4
            assert len(synced_collection) == 3
            assert synced_collection == [4, 2, 3]
        assert len(synced_collection) == 3
        assert synced_collection == [4, 2, 3]
        with synced_collection.buffered:
            assert len(synced_collection) == 3
            assert synced_collection == [4, 2, 3]
            del synced_collection[0]
            assert len(synced_collection) == 2
            assert synced_collection == [2, 3]
        assert len(synced_collection) == 2
        assert synced_collection == [2, 3]

        # Explicitly check that the file has not been changed when buffering.
        raw_list = synced_collection()
        with synced_collection.buffered:
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

    def multithreaded_buffering_test(self, op, requires_init):
        """Test that buffering in a multithreaded context is safe for different operations.

        This method encodes the logic for the test, but can be used to test different
        operations on the list.
        """
        original_buffer_capacity = self._collection_type.get_buffer_capacity()
        try:
            # Choose some arbitrarily low value that will ensure intermittent
            # forced buffer flushes.
            new_buffer_capacity = 20
            self._collection_type.set_buffer_capacity(new_buffer_capacity)

            with TemporaryDirectory(
                prefix="jsonlist_buffered_multithreaded"
            ) as tmp_dir:
                num_lists = 100
                lists = []
                list_data = []
                for i in range(num_lists):
                    # Initialize data with zeros, but prepare other data for
                    # updating in place.
                    fn = os.path.join(tmp_dir, f"test_list{i}.json")
                    lists.append(self._collection_type(filename=fn))
                    list_data.append([j for j in range(i)])
                    if requires_init:
                        lists[-1].extend([0 for j in range(i)])

                with buffer_all():
                    num_threads = 10
                    try:
                        with ThreadPoolExecutor(max_workers=num_threads) as executor:
                            list(executor.map(op, lists, list_data))
                    except KeyError as e:
                        raise RuntimeError(
                            "Buffering in parallel failed due to different threads "
                            "simultaneously modifying the buffer."
                        ) from e

                    # First validate inside buffer.
                    assert all(lists[i] == list_data[i] for i in range(num_lists))
                # Now validate outside buffer.
                assert all(lists[i] == list_data[i] for i in range(num_lists))
        finally:
            # Reset buffer capacity for other tests in case this fails.
            self._collection_type.set_buffer_capacity(original_buffer_capacity)

            # To avoid confusing test failures later, make sure the buffer is
            # truly flushed correctly.
            assert self._collection_type.get_current_buffer_size() == 0

    def test_multithreaded_buffering_setitem(self):
        """Test setitem in a multithreaded buffering context."""

        def setitem_list(sd, data):
            for i, val in enumerate(data):
                sd[i] = val

        self.multithreaded_buffering_test(setitem_list, True)

    def test_multithreaded_buffering_extend(self):
        """Test extend in a multithreaded buffering context."""

        def extend_list(sd, data):
            sd.extend(data)

        self.multithreaded_buffering_test(extend_list, False)

    def test_multithreaded_buffering_append(self):
        """Test append in a multithreaded buffering context."""

        def append_list(sd, data):
            for val in data:
                sd.append(val)

        self.multithreaded_buffering_test(append_list, False)

    def test_multithreaded_buffering_load(self):
        """Test loading data in a multithreaded buffering context.

        This test is primarily for verifying that multithreaded buffering does
        not lead to concurrency errors in flushing data from the buffer due to
        too many loads. This test is primarily for buffering methods with a maximum
        capacity, even for read-only operations.
        """
        original_buffer_capacity = self._collection_type.get_buffer_capacity()
        try:
            # Choose some arbitrarily low value that will ensure intermittent
            # forced buffer flushes.
            new_buffer_capacity = 1000
            self._collection_type.set_buffer_capacity(new_buffer_capacity)

            with TemporaryDirectory(
                prefix="jsonlist_buffered_multithreaded"
            ) as tmp_dir:
                # Must initialize the data outside the buffered context so that
                # we only execute read operations inside the buffered context.
                num_lists = 100
                lists = []
                for i in range(num_lists):
                    fn = os.path.join(tmp_dir, f"test_list{i}.json")
                    lists.append(self._collection_type(filename=fn))
                    # Go to i+1 so that every list contains the 0 element.
                    lists[-1].extend([j for j in range(i + 1)])

                with buffer_all():
                    num_threads = 100
                    try:
                        with ThreadPoolExecutor(max_workers=num_threads) as executor:
                            list(executor.map(lambda sd: sd[0], lists * 5))
                    except KeyError as e:
                        raise RuntimeError(
                            "Buffering in parallel failed due to different threads "
                            "simultaneously modifying the buffer."
                        ) from e

        finally:
            # Reset buffer capacity for other tests in case this fails.
            self._collection_type.set_buffer_capacity(original_buffer_capacity)

            # To avoid confusing test failures later, make sure the buffer is
            # truly flushed correctly.
            assert self._collection_type.get_current_buffer_size() == 0


class TestMemoryBufferedJSONDict(TestBufferedJSONDict):
    """Tests of MemoryBufferedJSONDicts."""

    _backend_collection = MemoryBufferedJSONCollection  # type: ignore
    _collection_type = MemoryBufferedJSONDict  # type: ignore

    def test_buffer_flush(self, synced_collection, synced_collection2):
        """Test that the buffer gets flushed when enough data is written."""
        original_buffer_capacity = self._collection_type.get_buffer_capacity()

        assert self._collection_type.get_current_buffer_size() == 0
        self._collection_type.set_buffer_capacity(1)

        # Ensure that the file exists on disk by executing a clear operation so
        # that load operations work as expected.
        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0
        synced_collection.clear()
        synced_collection2.clear()

        with buffer_all():
            synced_collection["foo"] = 1
            assert self._collection_type.get_current_buffer_size() == 1
            assert synced_collection != self.load(synced_collection)

            # This buffering mode is based on the number of files buffered, so
            # we need to write to the second collection.
            assert "bar" not in synced_collection2

            # Simply loading the second collection into memory shouldn't
            # trigger a flush, because it hasn't been modified and we flush
            # based on the total number of modifications.
            assert synced_collection != self.load(synced_collection)

            # Modifying the second collection should exceed buffer capacity and
            # trigger a flush.
            synced_collection2["bar"] = 2
            assert synced_collection == self.load(synced_collection)
            assert synced_collection2 == self.load(synced_collection2)

        # Reset buffer capacity for other tests.
        self._collection_type.set_buffer_capacity(original_buffer_capacity)


class TestMemoryBufferedJSONList(TestBufferedJSONList):
    """Tests of MemoryBufferedJSONLists."""

    _backend_collection = MemoryBufferedJSONCollection  # type: ignore
    _collection_type = MemoryBufferedJSONList  # type: ignore


class TestBufferedJSONDictWriteConcern(TestBufferedJSONDict):
    _write_concern = True


class TestBufferedJSONListWriteConcern(TestBufferedJSONList):
    _write_concern = True
