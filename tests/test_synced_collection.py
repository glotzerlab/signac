# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import uuid
import os
from tempfile import TemporaryDirectory
from collections.abc import MutableMapping
from collections.abc import MutableSequence

from signac.core.collection_api import SyncedCollection
from signac.core.jsoncollection import JSONDict
from signac.core.jsoncollection import JSONList
from signac.errors import InvalidKeyError
from signac.errors import KeyTypeError


FN_JSON = 'test.json'


class TestSyncedCollectionBase():

    _type = None

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        request.addfinalizer(self._tmp_dir.cleanup)
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)

    def get_testdata(self):
        return str(uuid.uuid4())

    def get_synced_collection(self, data=None):
        if self._type is not None:
            return self._type(filename=self._fn_, data=data)

    def test_init(self):
        self.get_synced_collection()


class TestJSONDict(TestSyncedCollectionBase):

    _type = JSONDict

    def test_isinstance(self):
        sd = self.get_synced_collection()
        assert isinstance(sd, SyncedCollection)
        assert isinstance(sd, MutableMapping)
        assert isinstance(sd, JSONDict)

    def test_set_get(self):
        sd = self.get_synced_collection()
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
        sd = self.get_synced_collection()
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
        sd = self.get_synced_collection()
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
        sd = self.get_synced_collection()
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
        sd = self.get_synced_collection()
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
        sd = self.get_synced_collection()
        key = 'update'
        d = {key: self.get_testdata()}
        sd.update(d)
        assert len(sd) == 1
        assert sd[key] == d[key]

    def test_clear(self):
        sd = self.get_synced_collection()
        key = 'clear'
        d = self.get_testdata()
        sd[key] = d
        assert len(sd) == 1
        assert sd[key] == d
        sd.clear()
        assert len(sd) == 0

    def test_reopen(self):
        jsd = self.get_synced_collection()
        key = 'reopen'
        d = self.get_testdata()
        jsd[key] = d
        jsd.sync()
        del jsd  # possibly unsafe
        jsd2 = self.get_synced_collection()
        jsd2.load()
        assert len(jsd2) == 1
        assert jsd2[key] == d

    def test_copy_as_dict(self):
        sd = self.get_synced_collection()
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

        jsd = self.get_synced_collection()
        key = 'write_invalid_type'
        d = self.get_testdata()
        jsd[key] = d
        assert len(jsd) == 1
        assert jsd[key] == d
        d2 = Foo()
        with pytest.raises(TypeError):
            jsd[key + '2'] = d2
        assert len(jsd) == 1
        assert jsd[key] == d

    def test_keys_with_dots(self):
        sd = self.get_synced_collection()
        with pytest.raises(InvalidKeyError):
            sd['a.b'] = None

    def test_keys_valid_type(self):
        jsd = self.get_synced_collection()

        class MyStr(str):
            pass
        for key in ('key', MyStr('key'), 0, None, True):
            d = jsd[key] = self.get_testdata()
            assert str(key) in jsd
            assert jsd[str(key)] == d

    def test_keys_invalid_type(self):
        sd = self.get_synced_collection()

        class A:
            pass
        for key in (0.0, A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                sd[key] = self.get_testdata()
        for key in ([], {}, dict()):
            with pytest.raises(TypeError):
                sd[key] = self.get_testdata()


class TestJSONList(TestSyncedCollectionBase):

    _type = JSONList

    def test_isinstance(self):
        sl = self.get_synced_collection()
        assert isinstance(sl, JSONList)
        assert isinstance(sl, MutableSequence)
        assert isinstance(sl, SyncedCollection)

    def test_set_get(self):
        sl = self.get_synced_collection()
        d = self.get_testdata()
        sl.clear()
        assert not bool(sl)
        assert len(sl) == 0
        sl.append(d)
        assert bool(sl)
        assert len(sl) == 1
        assert sl[0] == d

    def test_iter(self):
        sd = self.get_synced_collection()
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = [d1, d2]
        sd.extend(d)
        for i in range(len(sd)):
            assert d[i] == sd[i]
        assert i == 1

    def test_delete(self):
        sd = self.get_synced_collection()
        d = self.get_testdata()
        sd.append(d)
        assert len(sd) == 1
        assert sd[0] == d
        del sd[0]
        assert len(sd) == 0
        with pytest.raises(IndexError):
            sd[0]

    def test_extend(self):
        sd = self.get_synced_collection()
        d = [self.get_testdata()]
        sd.extend(d)
        assert len(sd) == 1
        assert sd[0] == d[0]

    def test_clear(self):
        sd = self.get_synced_collection()
        d = self.get_testdata()
        sd.append(d)
        assert len(sd) == 1
        assert sd[0] == d
        sd.clear()
        assert len(sd) == 0

    def test_reopen(self):
        jsl = self.get_synced_collection()
        d = self.get_testdata()
        jsl.append(d)
        jsl.sync()
        del jsl  # possibly unsafe
        jsl2 = self.get_synced_collection()
        jsl2.load()
        assert len(jsl2) == 1
        assert jsl2[0] == d

    def test_copy_as_list(self):
        sl = self.get_synced_collection()
        d = self.get_testdata()
        sl.append(d)
        assert sl[0] == d
        copy = list(sl)
        del sl
        assert copy[0] == d


class TestNestedDict(TestSyncedCollectionBase):

    def test_nested_dict(self):
        self._type = JSONDict
        sd = self.get_synced_collection()
        sd['a'] = dict(a=dict())
        child1 = sd['a']
        child2 = sd['a']['a']
        assert isinstance(child1, type(sd))
        assert isinstance(child1, type(child2))

    def test_nested_list(self):
        self._type = JSONList
        sl = self.get_synced_collection([1, 2, 3])
        sl.append([2, 4])
        child1 = sl[3]
        child1.append([1])
        child2 = child1[2]
        assert isinstance(child1, type(sl))
        assert isinstance(child2, type(sl))

    def test_nested_dict_with_list(self):
        self._type = JSONDict
        sd = self.get_synced_collection()
        sd['a'] = [1, 2, 3]
        child1 = sd['a']
        sd['a'].append(dict(a=[1, 2, 3]))
        child2 = sd['a'][3]
        child3 = sd['a'][3]['a']
        assert isinstance(child2, type(sd))
        assert isinstance(child1, type(child3))
        assert isinstance(child1, JSONList)
        assert isinstance(child3, JSONList)

    def test_nested_list_with_dict(self):
        self._type = JSONList
        sl = self.get_synced_collection([{'a': [1, 2, 3, 4]}])
        child1 = sl[0]
        child2 = sl[0]['a']
        assert isinstance(child2, JSONList)
        assert isinstance(child1, JSONDict)
