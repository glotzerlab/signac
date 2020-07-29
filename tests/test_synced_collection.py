# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import uuid
import os
import json
from tempfile import TemporaryDirectory
from collections.abc import MutableMapping
from collections.abc import MutableSequence

from signac.core.synced_list import SyncedCollection
from signac.core.jsoncollection import JSONDict
from signac.core.jsoncollection import JSONList
from signac.errors import InvalidKeyError
from signac.errors import KeyTypeError

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False

FN_JSON = 'test.json'


@pytest.fixture
def testdata():
    return str(uuid.uuid4())


class TestSyncedCollectionBase():

    @pytest.fixture(autouse=True)
    def synced_collection(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        yield
        self._tmp_dir.cleanup()

    def test_from_base(self):
        print(JSONDict.backend)
        sd = SyncedCollection.from_base(filename=self._fn_,
                                        data={'a': 0}, backend='signac.core.jsoncollection')
        assert isinstance(sd, JSONDict)
        assert 'a' in sd
        assert sd['a'] == 0

        # invalid input
        with pytest.raises(ValueError):
            SyncedCollection.from_base(data={'a': 0}, filename=self._fn_)


class TestJSONDict():

    _write_concern = False

    @pytest.fixture(autouse=True)
    def synced_dict(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        self._cls = JSONDict
        self._backend_kwargs = {'filename': self._fn_, 'write_concern': self._write_concern}
        yield self._cls(**self._backend_kwargs)
        self._tmp_dir.cleanup()

    def store(self, data):
        with open(self._fn_, 'wb') as file:
            file.write(json.dumps(data).encode())

    def test_init(self, synced_dict):
        assert len(synced_dict) == 0

    def test_invalid_kwargs(self):
        with pytest.raises(ValueError):
            return self._cls()

    def test_isinstance(self, synced_dict):
        assert isinstance(synced_dict, SyncedCollection)
        assert isinstance(synced_dict, MutableMapping)
        assert isinstance(synced_dict, self._cls)

    def test_set_get(self, synced_dict, testdata):
        key = 'setget'
        synced_dict.clear()
        assert not bool(synced_dict)
        assert len(synced_dict) == 0
        assert key not in synced_dict
        synced_dict[key] = testdata
        assert bool(synced_dict)
        assert len(synced_dict) == 1
        assert key in synced_dict
        assert synced_dict[key] == testdata
        assert synced_dict.get(key) == testdata

    def test_set_get_explicit_nested(self, synced_dict, testdata):
        key = 'setgetexplicitnested'
        synced_dict.setdefault('a', dict())
        child1 = synced_dict['a']
        child2 = synced_dict['a']
        assert child1 == child2
        assert isinstance(child1, type(child2))
        assert id(child1) == id(child2)
        assert not child1
        assert not child2
        child1[key] = testdata
        assert child1
        assert child2
        assert key in child1
        assert key in child2
        assert child1 == child2
        assert child1[key] == testdata
        assert child2[key] == testdata

    def test_copy_value(self, synced_dict, testdata):
        key = 'copy_value'
        key2 = 'copy_value2'
        assert key not in synced_dict
        assert key2 not in synced_dict
        synced_dict[key] = testdata
        assert key in synced_dict
        assert synced_dict[key] == testdata
        assert key2 not in synced_dict
        synced_dict[key2] = synced_dict[key]
        assert key in synced_dict
        assert synced_dict[key] == testdata
        assert key2 in synced_dict
        assert synced_dict[key2] == testdata

    def test_iter(self, synced_dict, testdata):
        key1 = 'iter1'
        key2 = 'iter2'
        d = {key1: testdata, key2: testdata}
        synced_dict.update(d)
        assert key1 in synced_dict
        assert key2 in synced_dict
        for i, key in enumerate(synced_dict):
            assert key in d
            assert d[key] == synced_dict[key]
        assert i == 1

    def test_delete(self, synced_dict, testdata):
        key = 'delete'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        del synced_dict[key]
        assert len(synced_dict) == 0
        with pytest.raises(KeyError):
            synced_dict[key]
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        del synced_dict['delete']
        assert len(synced_dict) == 0
        with pytest.raises(KeyError):
            synced_dict[key]

    def test_update(self, synced_dict, testdata):
        key = 'update'
        d = {key: testdata}
        synced_dict.update(d)
        assert len(synced_dict) == 1
        assert synced_dict[key] == d[key]

    def test_pop(self, synced_dict, testdata):
        key = 'pop'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        d1 = synced_dict.pop(key)
        assert len(synced_dict) == 0
        assert testdata == d1
        with pytest.raises(KeyError):
            synced_dict[key]
        d2 = synced_dict.pop(key, 'default')
        assert len(synced_dict) == 0
        assert d2 == 'default'

    def test_popitem(self, synced_dict, testdata):
        key = 'pop'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        key1, d1 = synced_dict.popitem()
        assert len(synced_dict) == 0
        assert key == key1
        assert testdata == d1
        with pytest.raises(KeyError):
            synced_dict[key]

    def test_values(self, synced_dict, testdata):
        data = {'value1': testdata, 'value_nested': {'value2': testdata}}
        synced_dict.reset(data)
        assert 'value1' in synced_dict
        assert 'value_nested' in synced_dict
        for val in synced_dict.values():
            assert not isinstance(val, self._cls)
            assert val in data.values()

    def test_items(self, synced_dict, testdata):
        data = {'item1': testdata, 'item_nested': {'item2': testdata}}
        synced_dict.reset(data)
        assert 'item1' in synced_dict
        assert 'item_nested' in synced_dict
        for key, val in synced_dict.items():
            assert synced_dict[key] == data[key]
            assert not isinstance(val, type(synced_dict))
            assert (key, val) in data.items()

    def test_reset(self, synced_dict, testdata):
        key = 'reset'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        synced_dict.reset()
        assert len(synced_dict) == 0
        synced_dict.reset({'reset': 'abc'})
        assert len(synced_dict) == 1
        assert synced_dict[key] == 'abc'

        # invalid input
        with pytest.raises(ValueError):
            synced_dict.reset([0, 1])

    def test_attr_dict(self, synced_dict, testdata):
        key = 'test'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert key in synced_dict
        assert synced_dict[key] == testdata
        assert synced_dict.get(key) == testdata
        assert synced_dict.test == testdata
        del synced_dict.test
        assert len(synced_dict) == 0
        assert key not in synced_dict
        key = 'test2'
        synced_dict.test2 = testdata
        assert len(synced_dict) == 1
        assert key in synced_dict
        assert synced_dict[key] == testdata
        assert synced_dict.get(key) == testdata
        assert synced_dict.test2 == testdata
        with pytest.raises(AttributeError):
            synced_dict.not_exist

        # deleting a protected attribute
        synced_dict.load()
        del synced_dict._parent
        # deleting _parent will lead to recursion as _parent is treated as key
        # load() will check for _parent and __getattr__ will call __getitem__ which calls load()
        with pytest.raises(RecursionError):
            synced_dict.load()

    def test_clear(self, synced_dict, testdata):
        key = 'clear'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        synced_dict.clear()
        assert len(synced_dict) == 0

    def test_repr(self, synced_dict):
        repr(synced_dict)
        p = eval(repr(synced_dict))
        assert repr(p) == repr(synced_dict)
        assert p == synced_dict

    def test_str(self, synced_dict):
        str(synced_dict) == str(synced_dict.to_base())

    def test_call(self, synced_dict, testdata):
        key = 'call'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        assert isinstance(synced_dict(), dict)
        assert not isinstance(synced_dict(), SyncedCollection)
        assert synced_dict() == synced_dict.to_base()

    def test_reopen(self, synced_dict, testdata):
        key = 'reopen'
        synced_dict[key] = testdata
        synced_dict.sync()
        del synced_dict  # possibly unsafe
        synced_dict2 = self._cls(**self._backend_kwargs)
        synced_dict2.load()
        assert len(synced_dict2) == 1
        assert synced_dict2[key] == testdata

    def test_update_recursive(self, synced_dict, testdata):
        synced_dict.a = {'a': 1}
        synced_dict.b = 'test'
        synced_dict.c = [0, 1, 2]
        assert 'a' in synced_dict
        assert 'b' in synced_dict
        assert 'c' in synced_dict
        data = {'a': 1, 'c': [0, 1, 3], 'd': 1}
        self.store(data)
        assert synced_dict == data

        # invalid data
        data = [1, 2, 3]
        with open(self._fn_, 'wb') as file:
            file.write(json.dumps(data).encode())
        with pytest.raises(ValueError):
            synced_dict.load()

    def test_copy_as_dict(self, synced_dict, testdata):
        key = 'copy'
        synced_dict[key] = testdata
        copy = dict(synced_dict)
        del synced_dict
        assert key in copy
        assert copy[key] == testdata

    def test_nested_dict(self, synced_dict):
        synced_dict['a'] = dict(a=dict())
        child1 = synced_dict['a']
        child2 = synced_dict['a']['a']
        assert isinstance(child1, type(synced_dict))
        assert isinstance(child1, type(child2))

    def test_nested_dict_with_list(self, synced_dict):
        synced_dict['a'] = [1, 2, 3]
        child1 = synced_dict['a']
        synced_dict['a'].append(dict(a=[1, 2, 3]))
        child2 = synced_dict['a'][3]
        child3 = synced_dict['a'][3]['a']
        assert isinstance(child2, type(synced_dict))
        assert isinstance(child1, type(child3))
        assert isinstance(child1, SyncedCollection)
        assert isinstance(child3, SyncedCollection)

    def test_write_invalid_type(self, synced_dict, testdata):
        class Foo(object):
            pass

        key = 'write_invalid_type'
        synced_dict[key] = testdata
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        d2 = Foo()
        with pytest.raises(TypeError):
            synced_dict[key + '2'] = d2
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata

    def test_keys_with_dots(self, synced_dict):
        with pytest.raises(InvalidKeyError):
            synced_dict['a.b'] = None

    def test_keys_valid_type(self, synced_dict, testdata):

        class MyStr(str):
            pass
        for key in ('key', MyStr('key'), 0, None, True):
            synced_dict[key] = testdata
            assert str(key) in synced_dict
            assert synced_dict[str(key)] == testdata

    def test_keys_invalid_type(self, synced_dict, testdata):

        class A:
            pass
        for key in (0.0, A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                synced_dict[key] = testdata
        for key in ([], {}, dict()):
            with pytest.raises(TypeError):
                synced_dict[key] = testdata


class TestJSONList:

    _write_concern = False

    @pytest.fixture(autouse=True)
    def synced_list(self):
        self._tmp_dir = TemporaryDirectory(prefix='jsondict_')
        self._fn_ = os.path.join(self._tmp_dir.name, FN_JSON)
        self._cls = JSONList
        self._backend_kwargs = {'filename': self._fn_, 'write_concern': self._write_concern}
        yield self._cls(**self._backend_kwargs)
        self._tmp_dir.cleanup()

    def store(self, data):
        with open(self._fn_, 'wb') as file:
            file.write(json.dumps(data).encode())

    def test_init(self, synced_list):
        assert len(synced_list) == 0

    def test_invalid_kwargs(self):
        with pytest.raises(ValueError):
            return self._cls()

    def test_isinstance(self, synced_list):
        assert isinstance(synced_list, MutableSequence)
        assert isinstance(synced_list, SyncedCollection)
        assert isinstance(synced_list, self._cls)

    def test_set_get(self, synced_list, testdata):
        synced_list.clear()
        assert not bool(synced_list)
        assert len(synced_list) == 0
        synced_list.append(testdata)
        assert bool(synced_list)
        assert len(synced_list) == 1
        assert synced_list[0] == testdata
        synced_list[0] = 1
        assert bool(synced_list)
        assert len(synced_list) == 1
        assert synced_list[0] == 1

    @pytest.mark.skipif(not NUMPY, reason='test requires the numpy package')
    def test_set_get_numpy_data(self, synced_list):
        data = numpy.random.rand(3, 4)
        data_as_list = data.tolist()
        synced_list.reset(data)
        assert len(synced_list) == len(data_as_list)
        assert synced_list == data_as_list
        data2 = numpy.random.rand(3, 4)
        synced_list.append(data2)
        assert len(synced_list) == len(data_as_list) + 1
        assert synced_list[len(data_as_list)] == data2.tolist()
        data3 = numpy.float_(3.14)
        synced_list.append(data3)
        assert len(synced_list) == len(data_as_list) + 2
        assert synced_list[len(data_as_list) + 1] == data3

    def test_iter(self, synced_list, testdata):
        d = [testdata, 43]
        synced_list.extend(d)
        for i in range(len(synced_list)):
            assert d[i] == synced_list[i]
        assert i == 1

    def test_delete(self, synced_list, testdata):
        synced_list.append(testdata)
        assert len(synced_list) == 1
        assert synced_list[0] == testdata
        del synced_list[0]
        assert len(synced_list) == 0
        with pytest.raises(IndexError):
            synced_list[0]

    def test_extend(self, synced_list, testdata):
        d = [testdata]
        synced_list.extend(d)
        assert len(synced_list) == 1
        assert synced_list[0] == d[0]
        d1 = testdata
        synced_list += [d1]
        assert len(synced_list) == 2
        assert synced_list[0] == d[0]
        assert synced_list[1] == d1

    def test_clear(self, synced_list, testdata):
        synced_list.append(testdata)
        assert len(synced_list) == 1
        assert synced_list[0] == testdata
        synced_list.clear()
        assert len(synced_list) == 0

    def test_reset(self, synced_list):
        synced_list.reset([1, 2, 3])
        assert len(synced_list) == 3
        assert synced_list == [1, 2, 3]
        synced_list.reset()
        assert len(synced_list) == 0
        synced_list.reset([3, 4])
        assert len(synced_list) == 2
        assert synced_list == [3, 4]

        # invalid inputs
        with pytest.raises(ValueError):
            synced_list.reset({'a': 1})

        with pytest.raises(ValueError):
            synced_list.reset(1)

    def test_insert(self, synced_list, testdata):
        synced_list.reset([1, 2])
        assert len(synced_list) == 2
        synced_list.insert(1, testdata)
        assert len(synced_list) == 3
        assert synced_list[1] == testdata

    def test_reversed(self,  synced_list):
        data = [1, 2, 3]
        synced_list.reset([1, 2, 3])
        assert len(synced_list) == 3
        assert synced_list == data
        for i, j in zip(reversed(synced_list), reversed(data)):
            assert i == j

    def test_remove(self, synced_list):
        synced_list.reset([1, 2])
        assert len(synced_list) == 2
        synced_list.remove(1)
        assert len(synced_list) == 1
        assert synced_list[0] == 2
        synced_list.reset([1, 2, 1])
        synced_list.remove(1)
        assert len(synced_list) == 2
        assert synced_list[0] == 2
        assert synced_list[1] == 1

    def test_call(self, synced_list):
        synced_list.reset([1, 2])
        assert len(synced_list) == 2
        assert isinstance(synced_list(), list)
        assert not isinstance(synced_list(), SyncedCollection)
        assert synced_list() == [1, 2]

    def test_update_recursive(self, synced_list):
        synced_list.reset([{'a': 1}, 'b', [1, 2, 3]])
        assert synced_list == [{'a': 1}, 'b', [1, 2, 3]]
        data = ['a', 'b', [1, 2, 4], 'd']
        self.store(data)
        assert synced_list == data
        data1 = ['a', 'b']
        self.store(data1)
        assert synced_list == data1

        # inavlid data in file
        data2 = {'a': 1}
        self.store(data2)
        with pytest.raises(ValueError):
            synced_list.load()

    def test_reopen(self, synced_list, testdata):
        synced_list.append(testdata)
        synced_list.sync()
        del synced_list  # possibly unsafe
        synced_list2 = self._cls(**self._backend_kwargs)
        synced_list2.load()
        assert len(synced_list2) == 1
        assert synced_list2[0] == testdata

    def test_copy_as_list(self, synced_list, testdata):
        synced_list.append(testdata)
        assert synced_list[0] == testdata
        copy = list(synced_list)
        del synced_list
        assert copy[0] == testdata

    def test_repr(self, synced_list):
        repr(synced_list)
        p = eval(repr(synced_list))
        assert repr(p) == repr(synced_list)
        assert p == synced_list

    def test_str(self, synced_list):
        str(synced_list) == str(synced_list.to_base())

    def test_nested_list(self, synced_list):
        synced_list.reset([1, 2, 3])
        synced_list.append([2, 4])
        child1 = synced_list[3]
        child2 = synced_list[3]
        assert child1 == child2
        assert isinstance(child1, type(child2))
        assert isinstance(child1, type(synced_list))
        assert id(child1) == id(child2)
        child1.append(1)
        assert child2[2] == child1[2]
        assert child1 == child2
        assert len(synced_list) == 4
        assert isinstance(child1, type(child2))
        assert isinstance(child1, type(synced_list))
        assert id(child1) == id(child2)
        del child1[0]
        assert child1 == child2
        assert len(synced_list) == 4
        assert isinstance(child1, type(child2))
        assert isinstance(child1, type(synced_list))
        assert id(child1) == id(child2)

    def test_nested_list_with_dict(self, synced_list):
        synced_list.reset([{'a': [1, 2, 3, 4]}])
        child1 = synced_list[0]
        child2 = synced_list[0]['a']
        assert isinstance(child2, SyncedCollection)
        assert isinstance(child1, SyncedCollection)


class TestJSONListWriteConcern(TestJSONList):

    _write_concern = True


class TestJSONDictWriteConcern(TestJSONDict):

    _write_concern = True
