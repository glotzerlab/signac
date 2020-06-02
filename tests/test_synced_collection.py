# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import uuid
from tempfile import TemporaryDirectory

from signac.core.collection_api import SyncedDict
from signac.core.collection_api import SyncedList
from signac.errors import InvalidKeyError
from signac.errors import KeyTypeError


def testdata():
    return str(uuid.uuid4())


class TestSyncedCollectionBase():

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        request.addfinalizer(self._tmp_dir.cleanup)

    def get_testdata(self):
        return str(uuid.uuid4())


class TestSyncedDict(TestSyncedCollectionBase):

    def get_synced_dict(self, data=None):
        return SyncedDict(data)

    def test_init(self):
        self.get_synced_dict()

    def test_set_get(self):
        sd = self.get_synced_dict()
        key = 'setget'
        d = self.get_testdata()
        sd.clear()
        assert not bool(sd)
        assert len(sd) == 0
        assert key not in sd
        sd[key] = d
        assert bool(sd)
        assert len(sd) == 1
        assert key in sd
        assert sd[key] == d
        assert sd.get(key) == d

    def test_set_get_explicit_nested(self):
        sd = self.get_synced_dict()
        key = 'setgetexplicitnested'
        d = self.get_testdata()
        sd.setdefault('a', dict())
        child1 = sd['a']
        child2 = sd['a']
        assert child1 == child2
        assert isinstance(child1, type(child2))
        assert id(child1) == id(child2)
        assert not child1
        assert not child2
        child1[key] = d
        assert child1
        assert child2
        assert key in child1
        assert key in child2
        assert child1 == child2
        assert child1[key] == d
        assert child2[key] == d

    def test_copy_value(self):
        sd = self.get_synced_dict()
        key = 'copy_value'
        key2 = 'copy_value2'
        d = self.get_testdata()
        assert key not in sd
        assert key2 not in sd
        sd[key] = d
        assert key in sd
        assert sd[key] == d
        assert key2 not in sd
        sd[key2] = sd[key]
        assert key in sd
        assert sd[key] == d
        assert key2 in sd
        assert sd[key2] == d

    def test_iter(self):
        sd = self.get_synced_dict()
        key1 = 'iter1'
        key2 = 'iter2'
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = {key1: d1, key2: d2}
        sd.update(d)
        assert key1 in sd
        assert key2 in sd
        for i, key in enumerate(sd):
            assert key in d
            assert d[key] == sd[key]
        assert i == 1

    def test_delete(self):
        sd = self.get_synced_dict()
        key = 'delete'
        d = self.get_testdata()
        sd[key] = d
        assert len(sd) == 1
        assert sd[key] == d
        del sd[key]
        assert len(sd) == 0
        with pytest.raises(KeyError):
            sd[key]
        sd[key] = d
        assert len(sd) == 1
        assert sd[key] == d
        del sd['delete']
        assert len(sd) == 0
        with pytest.raises(KeyError):
            sd[key]

    def test_update(self):
        sd = self.get_synced_dict()
        key = 'update'
        d = {key: self.get_testdata()}
        sd.update(d)
        assert len(sd) == 1
        assert sd[key] == d[key]

    def test_clear(self):
        sd = self.get_synced_dict()
        key = 'clear'
        d = self.get_testdata()
        sd[key] = d
        assert len(sd) == 1
        assert sd[key] == d
        sd.clear()
        assert len(sd) == 0

    def test_copy_as_dict(self):
        sd = self.get_synced_dict()
        key = 'copy'
        d = self.get_testdata()
        sd[key] = d
        copy = dict(sd)
        del sd
        assert key in copy
        assert copy[key] == d

    def test_write_invalid_type(self):
        class Foo(object):
            pass

        sd = self.get_synced_dict()
        key = 'write_invalid_type'
        d = self.get_testdata()
        sd[key] = d
        assert len(sd) == 1
        assert sd[key] == d
        assert len(sd) == 1
        assert sd[key] == d

    def test_keys_with_dots(self):
        sd = self.get_synced_dict()
        with pytest.raises(InvalidKeyError):
            sd['a.b'] = None

    def test_keys_valid_type(self):
        sd = self.get_synced_dict()

        class MyStr(str):
            pass
        for key in ('key', MyStr('key'), 0, None, True):
            d = sd[key] = self.get_testdata()
            assert str(key) in sd
            assert sd[str(key)] == d

    def test_keys_invalid_type(self):
        sd = self.get_synced_dict()

        class A:
            pass
        for key in (0.0, A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                sd[key] = self.get_testdata()
        for key in ([], {}, dict()):
            with pytest.raises(TypeError):
                sd[key] = self.get_testdata()


class TestSyncedList(TestSyncedCollectionBase):

    def get_synced_list(self, data=None):
        return SyncedList(data)

    def test_init(self):
        self.get_synced_list()

    def test_set_get(self):
        sl = self.get_synced_list()
        d = self.get_testdata()
        sl.clear()
        assert not bool(sl)
        assert len(sl) == 0
        sl.append(d)
        assert bool(sl)
        assert len(sl) == 1
        assert sl[0] == d

    def test_iter(self):
        sd = self.get_synced_list()
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = [d1, d2]
        sd.extend(d)
        for i in range(len(sd)):
            assert d[i] == sd[i]
        assert i == 1

    def test_delete(self):
        sd = self.get_synced_list()
        d = self.get_testdata()
        sd.append(d)
        assert len(sd) == 1
        assert sd[0] == d
        del sd[0]
        assert len(sd) == 0
        with pytest.raises(IndexError):
            sd[0]

    def test_extend(self):
        sd = self.get_synced_list()
        d = [self.get_testdata()]
        sd.extend(d)
        assert len(sd) == 1
        assert sd[0] == d[0]

    def test_clear(self):
        sd = self.get_synced_list()
        d = self.get_testdata()
        sd.append(d)
        assert len(sd) == 1
        assert sd[0] == d
        sd.clear()
        assert len(sd) == 0

    def test_copy_as_list(self):
        sl = self.get_synced_list()
        d = self.get_testdata()
        sl.append(d)
        assert sl[0] == d
        copy = list(sl)
        del sl
        assert copy[0] == d


class TestNestedDict(TestSyncedDict, TestSyncedList):

    def test_nested_dict(self):
        sd = self.get_synced_dict()
        sd['a'] = dict(a=dict())
        child1 = sd['a']
        child2 = sd['a']['a']
        assert isinstance(child1, type(sd))
        assert isinstance(child1, type(child2))

    def test_nested_list(self):
        sl = self.get_synced_list([1, 2, 3])
        sl.append([2, 4])
        child1 = sl[3]
        child1.append([1])
        child2 = child1[2]
        assert isinstance(child1, type(sl))
        assert isinstance(child2, type(sl))

    def test_nested_dict_with_list(self):
        sd = self.get_synced_dict()
        sd['a'] = [1, 2, 3]
        child1 = sd['a']
        sd['a'].append(dict(a=[1, 2, 3]))
        child2 = sd['a'][3]
        child3 = sd['a'][3]['a']
        assert isinstance(child2, type(sd))
        assert isinstance(child1, type(child3))
        assert isinstance(child1, SyncedList)
        assert isinstance(child3, SyncedList)

    def test_nested_list_with_dict(self):
        sl = self.get_synced_list([{'a': [1, 2, 3, 4]}])
        child1 = sl[0]
        child2 = sl[0]['a']
        assert isinstance(child2, SyncedList)
        assert isinstance(child1, SyncedDict)
