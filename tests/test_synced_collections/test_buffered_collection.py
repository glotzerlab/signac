# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import os
import json
from tempfile import TemporaryDirectory
import time
import itertools
import platform

<<<<<<< HEAD
from signac.core.synced_collections.collection_json import BufferedJSONCollection
=======
from signac.core.synced_collections.synced_collection import SyncedCollection
>>>>>>> feature/synced_collections
from signac.core.synced_collections.collection_json import BufferedJSONDict
from signac.core.synced_collections.collection_json import BufferedJSONList
from signac.core.synced_collections.buffered_collection import buffer_reads_writes
from signac.core.synced_collections.errors import MetadataError, BufferedError

<<<<<<< HEAD
from test_json_collection import JSONCollectionTest, TestJSONDict, TestJSONList
=======
from test_synced_collection import TestJSONDict, TestJSONList
>>>>>>> feature/synced_collections

FN_JSON = 'test.json'


PYPY = 'PyPy' in platform.python_implementation()


@pytest.mark.skipif(PYPY, reason="Buffered mode not supported for PyPy.")
<<<<<<< HEAD
class BufferedJSONCollectionTest(JSONCollectionTest):

    _backend = 'signac.core.synced_collections.collection_json.buffered'
    _backend_collection = BufferedJSONCollection

    def load(self, collection):
        """Load the data corresponding to a SyncedCollection from disk."""
=======
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
            data={'a': 0}, backend='signac.core.synced_collections.collection_json.buffered')
        assert isinstance(sd, BufferedJSONDict)
        assert 'a' in sd
        assert sd['a'] == 0

    def test_from_base_no_backend(self):
        with pytest.raises(ValueError):
            SyncedCollection.from_base(data={'a': 0})


@pytest.mark.skipif(PYPY, reason="Buffered mode not supported for PyPy.")
class BufferedJSONCollectionTest:
    def load(self, collection):
        """Load the data corresopnding to a SyncedCollection from disk."""
>>>>>>> feature/synced_collections
        with open(collection._filename) as f:
            return json.load(f)


@pytest.mark.skipif(PYPY, reason="Buffered mode not supported for PyPy.")
<<<<<<< HEAD
class TestBufferedJSONDict(BufferedJSONCollectionTest, TestJSONDict):
    """Tests of buffering JSONDicts."""

    _collection_type = BufferedJSONDict

    @pytest.fixture
    def synced_collection2(self):
=======
class TestBufferedJSONDict(TestJSONDict, BufferedJSONCollectionTest):
    """Tests of buffering JSONDicts."""

    @pytest.fixture
    def synced_dict(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        self._backend_kwargs = {'filename': self._fn_, 'write_concern': False}
        tmp = BufferedJSONDict(**self._backend_kwargs)
        yield tmp
        self._tmp_dir.cleanup()

    @pytest.fixture
    def synced_dict2(self):
>>>>>>> feature/synced_collections
        _tmp_dir2 = TemporaryDirectory(prefix='jsondict2_')
        _fn_2 = os.path.join(_tmp_dir2.name, 'test2.json')
        _backend_kwargs2 = {'filename': _fn_2, 'write_concern': False}
        tmp = BufferedJSONDict(**_backend_kwargs2)
        yield tmp
        _tmp_dir2.cleanup()

<<<<<<< HEAD
    def test_buffered(self, synced_collection, testdata):
        """Test basic per-instance buffering behavior."""
        assert len(synced_collection) == 0
        synced_collection['buffered'] = testdata
        assert 'buffered' in synced_collection
        assert synced_collection['buffered'] == testdata
        with synced_collection.buffered():
            assert 'buffered' in synced_collection
            assert synced_collection['buffered'] == testdata
            synced_collection['buffered2'] = 1
            assert 'buffered2' in synced_collection
            assert synced_collection['buffered2'] == 1
        assert len(synced_collection) == 2
        assert 'buffered2' in synced_collection
        assert synced_collection['buffered2'] == 1
        with synced_collection.buffered():
            del synced_collection['buffered']
            assert len(synced_collection) == 1
            assert 'buffered' not in synced_collection
        assert len(synced_collection) == 1
        assert 'buffered' not in synced_collection
        assert 'buffered2' in synced_collection
        assert synced_collection['buffered2'] == 1

        # Explicitly check that the file has not been changed when buffering.
        raw_dict = synced_collection.to_base()
        with synced_collection.buffered():
            synced_collection['buffered3'] = 1
            on_disk_dict = self.load(synced_collection)
            assert 'buffered3' not in on_disk_dict
            assert on_disk_dict == raw_dict

        on_disk_dict = self.load(synced_collection)
        assert 'buffered3' in on_disk_dict
        assert on_disk_dict == synced_collection

    def test_two_buffered(self, synced_collection, testdata):
        """Test that a non-buffered copy is not modified."""
        synced_collection['buffered'] = testdata
        synced_collection2 = BufferedJSONDict(
            filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with synced_collection.buffered():
            synced_collection['buffered2'] = 1
            assert 'buffered2' not in synced_collection2

    def test_two_buffered_modify_unbuffered(self, synced_collection, testdata):
        """Test that in-memory changes raise errors in buffered mode."""
        synced_collection['buffered'] = testdata
        synced_collection2 = BufferedJSONDict(
            filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with pytest.raises(MetadataError):
            with synced_collection.buffered():
                synced_collection['buffered2'] = 1
                synced_collection2['buffered2'] = 2
                assert synced_collection['buffered2'] == 1
                synced_collection['buffered2'] = 3
                assert synced_collection2['buffered2'] == 2
                synced_collection2['buffered2'] = 3
                assert synced_collection['buffered2'] == 3

    def test_two_buffered_modify_unbuffered_first(self, synced_collection,
                                                  testdata):
=======
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
            on_disk_dict = self.load(synced_dict)
            assert 'buffered3' not in on_disk_dict
            assert on_disk_dict == raw_dict

        on_disk_dict = self.load(synced_dict)
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

    def test_two_buffered_modify_unbuffered_first(self, synced_dict, testdata):
>>>>>>> feature/synced_collections
        # TODO: What is the expected behavior in this test? Data is only loaded
        # into the buffer the first time anything happens, so if we enter the
        # buffered context for one collection then modify an unbuffered
        # collection within that context _before_ doing anything with the
        # buffered collection, the buffered version will overwrite it. This
        # behavior feels slightly unexpected, but I don't know if there's any
        # way to fix it. For object-local buffering, we could load into the
        # buffer when entering the context instead of waiting until the first
        # call to load, but for global buffering there's no equivalent.
<<<<<<< HEAD
        synced_collection['buffered'] = testdata
        synced_collection2 = BufferedJSONDict(
            filename=synced_collection._filename)

        # Check that the non-buffered object is not modified.
        with synced_collection.buffered():
            synced_collection2['buffered2'] = 1
            assert 'buffered2' not in synced_collection
            synced_collection['buffered2'] = 3
        assert synced_collection == {'buffered': testdata, 'buffered2': 3}

    def test_global_buffered(self, synced_collection, testdata):
        assert len(synced_collection) == 0
        synced_collection['buffered'] = testdata
        assert 'buffered' in synced_collection
        assert synced_collection['buffered'] == testdata
        with buffer_reads_writes():
            assert 'buffered' in synced_collection
            assert synced_collection['buffered'] == testdata
            synced_collection['buffered2'] = 1
            assert 'buffered2' in synced_collection
            assert synced_collection['buffered2'] == 1
        assert len(synced_collection) == 2
        assert 'buffered2' in synced_collection
        assert synced_collection['buffered2'] == 1
        with buffer_reads_writes():
            del synced_collection['buffered']
            assert len(synced_collection) == 1
            assert 'buffered' not in synced_collection
        assert len(synced_collection) == 1
        assert 'buffered' not in synced_collection
        assert 'buffered2' in synced_collection
        assert synced_collection['buffered2'] == 1

        with pytest.raises(BufferedError):
            with buffer_reads_writes():
                synced_collection['buffered2'] = 2
                self.store({'test': 1})
                assert synced_collection['buffered2'] == 2
        assert 'test' in synced_collection
        assert synced_collection['test'] == 1

    def test_nested_same_collection(self, synced_collection):
        """Test nesting global buffering."""
        assert len(synced_collection) == 0

        for outer_buffer, inner_buffer in itertools.product(
                [synced_collection.buffered, buffer_reads_writes], repeat=2):
            err_msg = (f"outer_buffer: {outer_buffer.__qualname__}, "
                       f"inner_buffer: {inner_buffer.__qualname__}")
            synced_collection.reset({'outside': 1})
            with outer_buffer():
                synced_collection['inside_first'] = 2
                with inner_buffer():
                    synced_collection['inside_second'] = 3

                on_disk_dict = self.load(synced_collection)
                assert 'inside_first' not in on_disk_dict, err_msg
                assert 'inside_second' not in on_disk_dict, err_msg
                assert 'inside_first' in synced_collection, err_msg
                assert 'inside_second' in synced_collection, err_msg

        assert self.load(synced_collection) == synced_collection

    def test_nested_different_collections(self, synced_collection,
                                          synced_collection2):
        """Test nested buffering for different collections."""
        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0

        synced_collection['outside'] = 1
        synced_collection2['outside'] = 1
        with synced_collection.buffered():
            synced_collection['inside_first'] = 2
            on_disk_dict = self.load(synced_collection)
            assert 'inside_first' in synced_collection
            assert 'inside_first' not in on_disk_dict

            synced_collection2['inside_first'] = 2
            on_disk_dict2 = self.load(synced_collection2)
            assert 'inside_first' in synced_collection2
            assert 'inside_first' in on_disk_dict2

            with buffer_reads_writes():
                synced_collection['inside_second'] = 3
                synced_collection2['inside_second'] = 3

                on_disk_dict = self.load(synced_collection)
                assert 'inside_second' in synced_collection
                assert 'inside_second' not in on_disk_dict
                on_disk_dict2 = self.load(synced_collection2)
                assert 'inside_second' in synced_collection2
                assert 'inside_second' not in on_disk_dict2

            on_disk_dict = self.load(synced_collection)
            on_disk_dict2 = self.load(synced_collection2)

            assert 'inside_first' in synced_collection
            assert 'inside_first' not in on_disk_dict

            assert 'inside_second' in synced_collection
            assert 'inside_second' not in on_disk_dict
            assert 'inside_second' in synced_collection2
            assert 'inside_second' in on_disk_dict2

        on_disk_dict = self.load(synced_collection)
        on_disk_dict2 = self.load(synced_collection2)

        assert 'inside_first' in synced_collection
        assert 'inside_first' in on_disk_dict

        assert 'inside_second' in synced_collection
        assert 'inside_second' in on_disk_dict
        assert 'inside_second' in synced_collection2
        assert 'inside_second' in on_disk_dict2

    def test_nested_copied_collection(self, synced_collection):
        """Test modifying two collections pointing to the same data."""
        synced_collection2 = BufferedJSONDict(
            filename=synced_collection._filename)

        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0

        synced_collection['outside'] = 1
        with synced_collection.buffered():
            synced_collection['inside_first'] = 2

            on_disk_dict = self.load(synced_collection)
            assert synced_collection['inside_first'] == 2
            assert 'inside_first' not in on_disk_dict

            with buffer_reads_writes():
                synced_collection['inside_second'] = 3
                synced_collection2['inside_second'] = 4

                on_disk_dict = self.load(synced_collection)
                assert synced_collection['inside_second'] == 4
                assert synced_collection2['inside_second'] == 4
                assert 'inside_second' not in on_disk_dict

            on_disk_dict = self.load(synced_collection)
            assert on_disk_dict['inside_second'] == 4

    @pytest.mark.skip("Not currently sure what the expected behavior is.")
    def test_nested_copied_collection_invalid(self, synced_collection):
        """Test the behavior of invalid modifications of copied objects."""
        synced_collection2 = BufferedJSONDict(
            filename=synced_collection._filename)

        assert len(synced_collection) == 0
        assert len(synced_collection2) == 0

        synced_collection['outside'] = 1
        finished = False
        with pytest.raises(MetadataError):
            with synced_collection.buffered():
                synced_collection['inside_first'] = 2
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
                synced_collection2['inside_first'] = 3

                on_disk_dict = self.load(synced_collection)
                assert synced_collection['inside_first'] == 2
                assert on_disk_dict['inside_first'] == 3

                with buffer_reads_writes():
                    synced_collection['inside_second'] = 3
                    synced_collection2['inside_second'] = 4

                    on_disk_dict = self.load(synced_collection)
                    assert synced_collection['inside_second'] == 4
                    assert synced_collection2['inside_second'] == 4
                    assert 'inside_second' not in on_disk_dict

                on_disk_dict = self.load(synced_collection)
=======
        synced_dict['buffered'] = testdata
        synced_dict2 = BufferedJSONDict(filename=synced_dict._filename)

        # Check that the non-buffered object is not modified.
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

    def test_nested_same_collection(self, synced_dict):
        """Test nesting global buffering."""
        assert len(synced_dict) == 0

        for outer_buffer, inner_buffer in itertools.product(
                [synced_dict.buffered, buffer_reads_writes], repeat=2):
            err_msg = (f"outer_buffer: {outer_buffer.__qualname__}, "
                       f"inner_buffer: {inner_buffer.__qualname__}")
            synced_dict.reset({'outside': 1})
            with outer_buffer():
                synced_dict['inside_first'] = 2
                with inner_buffer():
                    synced_dict['inside_second'] = 3

                on_disk_dict = self.load(synced_dict)
                assert 'inside_first' not in on_disk_dict, err_msg
                assert 'inside_second' not in on_disk_dict, err_msg
                assert 'inside_first' in synced_dict, err_msg
                assert 'inside_second' in synced_dict, err_msg

        assert self.load(synced_dict) == synced_dict

    def test_nested_different_collections(self, synced_dict, synced_dict2):
        """Test nested buffering for different collections."""
        assert len(synced_dict) == 0
        assert len(synced_dict2) == 0

        synced_dict['outside'] = 1
        synced_dict2['outside'] = 1
        with synced_dict.buffered():
            synced_dict['inside_first'] = 2
            on_disk_dict = self.load(synced_dict)
            assert 'inside_first' in synced_dict
            assert 'inside_first' not in on_disk_dict

            synced_dict2['inside_first'] = 2
            on_disk_dict2 = self.load(synced_dict2)
            assert 'inside_first' in synced_dict2
            assert 'inside_first' in on_disk_dict2

            with buffer_reads_writes():
                synced_dict['inside_second'] = 3
                synced_dict2['inside_second'] = 3

                on_disk_dict = self.load(synced_dict)
                assert 'inside_second' in synced_dict
                assert 'inside_second' not in on_disk_dict
                on_disk_dict2 = self.load(synced_dict2)
                assert 'inside_second' in synced_dict2
                assert 'inside_second' not in on_disk_dict2

            on_disk_dict = self.load(synced_dict)
            on_disk_dict2 = self.load(synced_dict2)

            assert 'inside_first' in synced_dict
            assert 'inside_first' not in on_disk_dict

            assert 'inside_second' in synced_dict
            assert 'inside_second' not in on_disk_dict
            assert 'inside_second' in synced_dict2
            assert 'inside_second' in on_disk_dict2

        on_disk_dict = self.load(synced_dict)
        on_disk_dict2 = self.load(synced_dict2)

        assert 'inside_first' in synced_dict
        assert 'inside_first' in on_disk_dict

        assert 'inside_second' in synced_dict
        assert 'inside_second' in on_disk_dict
        assert 'inside_second' in synced_dict2
        assert 'inside_second' in on_disk_dict2

    def test_nested_copied_collection(self, synced_dict):
        """Test modifying two collections pointing to the same data."""
        synced_dict2 = BufferedJSONDict(filename=synced_dict._filename)

        assert len(synced_dict) == 0
        assert len(synced_dict2) == 0

        synced_dict['outside'] = 1
        with synced_dict.buffered():
            synced_dict['inside_first'] = 2

            on_disk_dict = self.load(synced_dict)
            assert synced_dict['inside_first'] == 2
            assert 'inside_first' not in on_disk_dict

            with buffer_reads_writes():
                synced_dict['inside_second'] = 3
                synced_dict2['inside_second'] = 4

                on_disk_dict = self.load(synced_dict)
                assert synced_dict['inside_second'] == 4
                assert synced_dict2['inside_second'] == 4
                assert 'inside_second' not in on_disk_dict

            on_disk_dict = self.load(synced_dict)
            assert on_disk_dict['inside_second'] == 4

    @pytest.mark.skip("Not currently sure what the expected behavior is.")
    def test_nested_copied_collection_invalid(self, synced_dict):
        """Test the behavior of invalid modifications of copied objects."""
        synced_dict2 = BufferedJSONDict(filename=synced_dict._filename)

        assert len(synced_dict) == 0
        assert len(synced_dict2) == 0

        synced_dict['outside'] = 1
        finished = False
        with pytest.raises(MetadataError):
            with synced_dict.buffered():
                synced_dict['inside_first'] = 2
                # TODO: Currently, modifying synced_dict2 here causes problems.
                # It is unbuffered, so it directly writes to file. Then, when
                # entering global buffering in the context below, synced_dict2
                # sees that synced_dict has already saved data for this file to
                # the buffer, so it loads that data, which also means that
                # synced_dict2 becomes associated with the metadata stored when
                # synced_dict entered buffered mode. As a result, when the
                # global buffering exits, we see metadata errors because
                # synced_dict2 lost track of the fact that it saved changes to
                # filemade prior to entering the global buffer. We _could_ fix
                # this by changing the behavior of _load_buffer to not load the
                # data from the cache if it exists, if the object is new to
                # cached_collections then we would save a new version. However,
                # I'm not sure that's the correct answer. Is there a true
                # canonical source of truth in this scenario?
                synced_dict2['inside_first'] = 3

                on_disk_dict = self.load(synced_dict)
                assert synced_dict['inside_first'] == 2
                assert on_disk_dict['inside_first'] == 3

                with buffer_reads_writes():
                    synced_dict['inside_second'] = 3
                    synced_dict2['inside_second'] = 4

                    on_disk_dict = self.load(synced_dict)
                    assert synced_dict['inside_second'] == 4
                    assert synced_dict2['inside_second'] == 4
                    assert 'inside_second' not in on_disk_dict

                on_disk_dict = self.load(synced_dict)
>>>>>>> feature/synced_collections
                assert on_disk_dict['inside_second'] == 4
                # Check that all the checks ran before the assertion failure.
                finished = True
        assert finished


@pytest.mark.skipif(PYPY, reason="Buffered mode not supported for PyPy.")
<<<<<<< HEAD
class TestBufferedJSONList(BufferedJSONCollectionTest, TestJSONList):
    """Tests of buffering JSONLists."""

    _collection_type = BufferedJSONList

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
        raw_list = synced_collection.to_base()
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
        with buffer_reads_writes():
            synced_collection.reset([1, 2, 3])
            assert len(synced_collection) == 3
        assert len(synced_collection) == 3
        assert synced_collection == [1, 2, 3]
        with buffer_reads_writes():
            assert len(synced_collection) == 3
            assert synced_collection == [1, 2, 3]
            synced_collection[0] = 4
            assert len(synced_collection) == 3
            assert synced_collection == [4, 2, 3]
        assert len(synced_collection) == 3
        assert synced_collection == [4, 2, 3]
=======
class TestBufferedJSONList(TestJSONList, BufferedJSONCollectionTest):
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
            on_disk_list = self.load(synced_list)
            assert 10 not in on_disk_list
            assert on_disk_list == raw_list

        on_disk_list = self.load(synced_list)
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
>>>>>>> feature/synced_collections

        # metacheck failure
        with pytest.raises(BufferedError):
            with buffer_reads_writes():
<<<<<<< HEAD
                synced_collection.reset([1])
                assert synced_collection == [1]
=======
                synced_list.reset([1])
                assert synced_list == [1]
>>>>>>> feature/synced_collections
                # Unfortunately the resolution of os.stat is
                # platform dependent and may not always be
                # high enough for our check to work. Since
                # this unit test is artificially simple we
                # must add some amount of minimum waiting time
                # to ensure that the change in time will be
                # detected.
                time.sleep(0.01)
                self.store([1, 2, 3])
<<<<<<< HEAD
                assert synced_collection == [1]
        assert len(synced_collection) == 3
        assert synced_collection == [1, 2, 3]


class TestBufferedJSONDictWriteConcern(TestBufferedJSONDict):
    _write_concern = True


class TestBufferedJSONListWriteConcern(TestBufferedJSONList):
    _write_concern = True
=======
                assert synced_list == [1]
        assert len(synced_list) == 3
        assert synced_list == [1, 2, 3]
>>>>>>> feature/synced_collections
