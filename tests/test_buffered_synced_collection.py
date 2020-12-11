# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import os
import json
from tempfile import TemporaryDirectory
from collections.abc import MutableMapping
from collections.abc import MutableSequence
from copy import deepcopy

from signac.core.synced_collection import SyncedCollection
# from signac.core.synced_collection import buffer_reads_writes as buffered
from signac.core.collection_json import BufferedJSONDict
from signac.core.collection_json import BufferedJSONList
# from signac.core.synced_collection import BufferException
# from signac.core.synced_collection import BufferedError
# from signac.core.buffers import FileBuffer
from signac.errors import InvalidKeyError
from signac.errors import KeyTypeError

from test_synced_collection import TestJSONDict, TestJSONList

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False

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

    @pytest.fixture
    def synced_dict(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        self._backend_kwargs = {'filename': self._fn_, 'write_concern': False}
        tmp = BufferedJSONDict(**self._backend_kwargs)
        yield tmp
        self._tmp_dir.cleanup()

#    def test_buffered(self, synced_dict, testdata):
#        if synced_dict._supports_buffering:
#            assert len(synced_dict) == 0
#            synced_dict['buffered'] = testdata
#            assert 'buffered' in synced_dict
#            assert synced_dict['buffered'] == testdata
#            with synced_dict.buffered():
#                assert 'buffered' in synced_dict
#                assert synced_dict['buffered'] == testdata
#                synced_dict['buffered2'] = 1
#                assert 'buffered2' in synced_dict
#                assert synced_dict['buffered2'] == 1
#            assert len(synced_dict) == 2
#            assert 'buffered2' in synced_dict
#            assert synced_dict['buffered2'] == 1
#            with synced_dict.buffered():
#                del synced_dict['buffered']
#                assert len(synced_dict) == 1
#                assert 'buffered' not in synced_dict
#            assert len(synced_dict) == 1
#            assert 'buffered' not in synced_dict
#            assert 'buffered2' in synced_dict
#            assert synced_dict['buffered2'] == 1
#        else:
#            with pytest.raises(BufferException):
#                with synced_dict.buffered():
#                    pass
#
#    def test_global_buffered(self, synced_dict, testdata):
#        assert len(synced_dict) == 0
#        synced_dict['buffered'] = testdata
#        assert 'buffered' in synced_dict
#        assert synced_dict['buffered'] == testdata
#        with buffered():
#            assert 'buffered' in synced_dict
#            assert synced_dict['buffered'] == testdata
#            synced_dict['buffered2'] = 1
#            assert 'buffered2' in synced_dict
#            assert synced_dict['buffered2'] == 1
#        assert len(synced_dict) == 2
#        assert 'buffered2' in synced_dict
#        assert synced_dict['buffered2'] == 1
#        with buffered():
#            del synced_dict['buffered']
#            assert len(synced_dict) == 1
#            assert 'buffered' not in synced_dict
#        assert len(synced_dict) == 1
#        assert 'buffered' not in synced_dict
#        assert 'buffered2' in synced_dict
#        assert synced_dict['buffered2'] == 1
#        if isinstance(synced_dict, FileBuffer):
#            # metacheck failure
#            with pytest.raises(BufferedError):
#                with buffered():
#                    synced_dict['buffered2'] = 2
#                    self.store({'test': 1})
#                    assert synced_dict['buffered2'] == 2
#            assert 'test' in synced_dict
#            assert synced_dict['test'] == 1
#            # skipping metacheck
#            with buffered(force_write=True):
#                synced_dict['test2'] = 1
#                assert synced_dict['test2'] == 1
#                self.store({'test': 2})
#                assert synced_dict['test2'] == 1
#            assert synced_dict['test2'] == 1


class TestBufferedJSONList(TestJSONList):

    @pytest.fixture
    def synced_list(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsonlist_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        self._backend_kwargs = {'filename': self._fn_}
        yield BufferedJSONList(**self._backend_kwargs)
        self._tmp_dir.cleanup()

#    def test_buffered(self, synced_list):
#        if synced_list._supports_buffering:
#            synced_list.extend([1, 2, 3])
#            assert len(synced_list) == 3
#            assert synced_list == [1, 2, 3]
#            with synced_list.buffered():
#                assert len(synced_list) == 3
#                assert synced_list == [1, 2, 3]
#                synced_list[0] = 4
#                assert len(synced_list) == 3
#                assert synced_list == [4, 2, 3]
#            assert len(synced_list) == 3
#            assert synced_list == [4, 2, 3]
#            with synced_list.buffered():
#                assert len(synced_list) == 3
#                assert synced_list == [4, 2, 3]
#                del synced_list[0]
#                assert len(synced_list) == 2
#                assert synced_list == [2, 3]
#            assert len(synced_list) == 2
#            assert synced_list == [2, 3]
#        else:
#            with pytest.raises(BufferException):
#                with synced_list.buffered():
#                    pass
#
#    def test_global_buffered(self, synced_list):
#        assert len(synced_list) == 0
#        with buffered():
#            synced_list.reset([1, 2, 3])
#            assert len(synced_list) == 3
#        assert len(synced_list) == 3
#        assert synced_list == [1, 2, 3]
#        with buffered():
#            assert len(synced_list) == 3
#            assert synced_list == [1, 2, 3]
#            synced_list[0] = 4
#            assert len(synced_list) == 3
#            assert synced_list == [4, 2, 3]
#        assert len(synced_list) == 3
#        assert synced_list == [4, 2, 3]
#        with buffered(force_write=True):
#            assert len(synced_list) == 3
#            assert synced_list == [4, 2, 3]
#            del synced_list[0]
#            assert len(synced_list) == 2
#            assert synced_list == [2, 3]
#        assert len(synced_list) == 2
#        assert synced_list == [2, 3]
#        if isinstance(synced_list, FileBuffer):
#            # metacheck failure
#            with pytest.raises(BufferedError):
#                with buffered():
#                    synced_list.reset([1])
#                    assert synced_list == [1]
#                    self.store([1, 2, 3])
#                    assert synced_list == [1]
#            assert len(synced_list) == 3
#            assert synced_list == [1, 2, 3]
#            # skipping metacheck
#            with buffered(force_write=True):
#                synced_list.reset([1])
#                assert synced_list == [1]
#                self.store([1, 2])
#                assert synced_list == [1]
#            assert synced_list == [1]
