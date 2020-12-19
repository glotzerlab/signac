# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from collections.abc import MutableMapping
from collections.abc import MutableSequence
from copy import deepcopy

from signac.core.synced_collections.synced_collection import SyncedCollection
from signac.core.synced_collections.collection_json import JSONDict
from signac.errors import InvalidKeyError
from signac.errors import KeyTypeError

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


class SyncedCollectionTest:

    def store(self, data):
        raise NotImplementedError(
            "All backend tests must implement the store method.")

    @pytest.fixture(autouse=True)
    def synced_collection(self):
        raise NotImplementedError(
            "All backend tests must implement a synced_collection autouse "
            "fixture that returns an empty instance.")


class SyncedDictTest(SyncedCollectionTest):

    @pytest.fixture(autouse=True)
    def base_collection(self):
        return {'a': 0}

    def test_from_base(self, base_collection):
        sd = SyncedCollection.from_base(
            **self._backend_kwargs, data=base_collection,
            backend=self._backend)
        assert isinstance(sd, self._collection_type)
        assert 'a' in sd
        assert sd['a'] == 0

    def test_from_base_explicit(self, base_collection):
        sd = self._backend_collection.from_base(
            **self._backend_kwargs, data=base_collection)
        assert isinstance(sd, self._collection_type)
        assert 'a' in sd
        assert sd['a'] == 0

    def test_from_base_no_backend(self, base_collection):
        with pytest.raises(ValueError):
            SyncedCollection.from_base(
                **self._backend_kwargs, data=base_collection)

    def test_init(self, synced_collection):
        assert len(synced_collection) == 0

    def test_invalid_kwargs(self, synced_collection):
        # JSONDict raise an error when neither filename nor parent is passed.
        with pytest.raises(ValueError):
            return type(synced_collection)()

    def test_isinstance(self, synced_collection):
        assert isinstance(synced_collection, SyncedCollection)
        assert isinstance(synced_collection, MutableMapping)

    def test_set_get(self, synced_collection, testdata):
        key = 'setget'
        synced_collection.clear()
        assert not bool(synced_collection)
        assert len(synced_collection) == 0
        assert key not in synced_collection
        synced_collection[key] = testdata
        assert bool(synced_collection)
        assert len(synced_collection) == 1
        assert key in synced_collection
        assert synced_collection[key] == testdata
        assert synced_collection.get(key) == testdata

    def test_set_get_explicit_nested(self, synced_collection, testdata):
        key = 'setgetexplicitnested'
        synced_collection.setdefault('a', dict())
        child1 = synced_collection['a']
        child2 = synced_collection['a']
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

    def test_copy_value(self, synced_collection, testdata):
        key = 'copy_value'
        key2 = 'copy_value2'
        assert key not in synced_collection
        assert key2 not in synced_collection
        synced_collection[key] = testdata
        assert key in synced_collection
        assert synced_collection[key] == testdata
        assert key2 not in synced_collection
        synced_collection[key2] = synced_collection[key]
        assert key in synced_collection
        assert synced_collection[key] == testdata
        assert key2 in synced_collection
        assert synced_collection[key2] == testdata

    def test_iter(self, synced_collection, testdata):
        key1 = 'iter1'
        key2 = 'iter2'
        d = {key1: testdata, key2: testdata}
        synced_collection.update(d)
        assert key1 in synced_collection
        assert key2 in synced_collection
        for i, key in enumerate(synced_collection):
            assert key in d
            assert d[key] == synced_collection[key]
        assert i == 1

    def test_delete(self, synced_collection, testdata):
        key = 'delete'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        del synced_collection[key]
        assert len(synced_collection) == 0
        with pytest.raises(KeyError):
            synced_collection[key]
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        del synced_collection['delete']
        assert len(synced_collection) == 0
        with pytest.raises(KeyError):
            synced_collection[key]

    def test_update(self, synced_collection, testdata):
        key = 'update'
        d = {key: testdata}
        synced_collection.update(d)
        assert len(synced_collection) == 1
        assert synced_collection[key] == d[key]
        # upadte with no argument
        synced_collection.update()
        assert len(synced_collection) == 1
        assert synced_collection[key] == d[key]
        # update using key as kwarg
        synced_collection.update(update2=testdata)
        assert len(synced_collection) == 2
        assert synced_collection['update2'] == testdata
        # same key in other dict and as kwarg with different values
        synced_collection.update({key: 1}, update=2)  # here key is 'update'
        assert len(synced_collection) == 2
        assert synced_collection[key] == 2
        # update using list of key and value pair
        synced_collection.update([('update2', 1), ('update3', 2)])
        assert len(synced_collection) == 3
        assert synced_collection['update2'] == 1
        assert synced_collection['update3'] == 2

    def test_pop(self, synced_collection, testdata):
        key = 'pop'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        d1 = synced_collection.pop(key)
        assert len(synced_collection) == 0
        assert testdata == d1
        with pytest.raises(KeyError):
            synced_collection[key]
        d2 = synced_collection.pop(key, 'default')
        assert len(synced_collection) == 0
        assert d2 == 'default'

    def test_popitem(self, synced_collection, testdata):
        key = 'pop'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        key1, d1 = synced_collection.popitem()
        assert len(synced_collection) == 0
        assert key == key1
        assert testdata == d1
        with pytest.raises(KeyError):
            synced_collection[key]

    def test_values(self, synced_collection, testdata):
        data = {'value1': testdata, 'value_nested': {'value2': testdata}}
        synced_collection.reset(data)
        assert 'value1' in synced_collection
        assert 'value_nested' in synced_collection
        for val in synced_collection.values():
            assert not isinstance(val, SyncedCollection)
            assert val in data.values()

    def test_items(self, synced_collection, testdata):
        data = {'item1': testdata, 'item_nested': {'item2': testdata}}
        synced_collection.reset(data)
        assert 'item1' in synced_collection
        assert 'item_nested' in synced_collection
        for key, val in synced_collection.items():
            assert synced_collection[key] == data[key]
            assert not isinstance(val, type(synced_collection))
            assert (key, val) in data.items()

    def test_setdefault(self, synced_collection, testdata):
        key = 'setdefault'
        ret = synced_collection.setdefault(key, testdata)
        assert ret == testdata
        assert key in synced_collection
        assert synced_collection[key] == testdata
        ret = synced_collection.setdefault(key, 1)
        assert ret == testdata
        assert synced_collection[key] == testdata

    def test_reset(self, synced_collection, testdata):
        key = 'reset'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        synced_collection.reset()
        assert len(synced_collection) == 0
        synced_collection.reset({'reset': 'abc'})
        assert len(synced_collection) == 1
        assert synced_collection[key] == 'abc'

        # invalid input
        with pytest.raises(ValueError):
            synced_collection.reset([0, 1])

    def test_attr_dict(self, synced_collection, testdata):
        key = 'test'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert key in synced_collection
        assert synced_collection[key] == testdata
        assert synced_collection.get(key) == testdata
        assert synced_collection.test == testdata
        del synced_collection.test
        assert len(synced_collection) == 0
        assert key not in synced_collection
        key = 'test2'
        synced_collection.test2 = testdata
        assert len(synced_collection) == 1
        assert key in synced_collection
        assert synced_collection[key] == testdata
        assert synced_collection.get(key) == testdata
        assert synced_collection.test2 == testdata
        with pytest.raises(AttributeError):
            synced_collection.not_exist

        # deleting a protected attribute
        synced_collection.load()
        del synced_collection._parent
        # deleting _parent will lead to recursion as _parent is treated as key
        # load() will check for _parent and __getattr__ will call __getitem__ which calls load()
        with pytest.raises(RecursionError):
            synced_collection.load()

    def test_clear(self, synced_collection, testdata):
        key = 'clear'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        synced_collection.clear()
        assert len(synced_collection) == 0

    def test_repr(self, synced_collection):
        repr(synced_collection)
        p = eval(repr(synced_collection))
        assert repr(p) == repr(synced_collection)
        assert p == synced_collection

    def test_str(self, synced_collection):
        str(synced_collection) == str(synced_collection.to_base())

    def test_call(self, synced_collection, testdata):
        key = 'call'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        assert isinstance(synced_collection(), dict)
        assert not isinstance(synced_collection(), SyncedCollection)
        assert synced_collection() == synced_collection.to_base()

    def test_reopen(self, synced_collection, testdata):
        key = 'reopen'
        synced_collection[key] = testdata
        try:
            synced_collection2 = deepcopy(synced_collection)
        except TypeError:
            # Use fallback implementation, deepcopy not supported by backend.
            synced_collection2 = synced_collection._pseudo_deepcopy()
        synced_collection.save()
        del synced_collection  # possibly unsafe
        synced_collection2.load()
        assert len(synced_collection2) == 1
        assert synced_collection2[key] == testdata

    def test_update_recursive(self, synced_collection, testdata):
        synced_collection.a = {'a': 1}
        synced_collection.b = 'test'
        synced_collection.c = [0, 1, 2]
        assert 'a' in synced_collection
        assert 'b' in synced_collection
        assert 'c' in synced_collection
        data = {'a': 1, 'c': [0, 1, 3], 'd': 1}
        self.store(data)
        assert synced_collection == data

        # invalid data
        data = [1, 2, 3]
        self.store(data)
        with pytest.raises(ValueError):
            synced_collection.load()

    def test_copy_as_dict(self, synced_collection, testdata):
        key = 'copy'
        synced_collection[key] = testdata
        copy = dict(synced_collection)
        del synced_collection
        assert key in copy
        assert copy[key] == testdata

    def test_nested_dict(self, synced_collection):
        synced_collection['a'] = dict(a=dict())
        child1 = synced_collection['a']
        child2 = synced_collection['a']['a']
        assert isinstance(child1, type(synced_collection))
        assert isinstance(child1, type(child2))

    def test_nested_dict_with_list(self, synced_collection):
        synced_collection['a'] = [1, 2, 3]
        child1 = synced_collection['a']
        synced_collection['a'].append(dict(a=[1, 2, 3]))
        child2 = synced_collection['a'][3]
        child3 = synced_collection['a'][3]['a']
        assert isinstance(child2, type(synced_collection))
        assert isinstance(child1, type(child3))
        assert isinstance(child1, SyncedCollection)
        assert isinstance(child3, SyncedCollection)

    def test_write_invalid_type(self, synced_collection, testdata):
        class Foo(object):
            pass

        key = 'write_invalid_type'
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        d2 = Foo()
        with pytest.raises(TypeError):
            synced_collection[key + '2'] = d2
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata

    def test_keys_with_dots(self, synced_collection):
        with pytest.raises(InvalidKeyError):
            synced_collection['a.b'] = None

    def test_keys_str_type(self, synced_collection, testdata):

        class MyStr(str):
            pass
        for key in ('key', MyStr('key')):
            synced_collection[key] = testdata
            assert key in synced_collection
            assert synced_collection[key] == testdata

    # The following test tests the support for non-str keys
    # for JSON backend which will be removed in version 2.0.
    # See issue: https://github.com/glotzerlab/signac/issues/316.
    def test_keys_non_str_valid_type(self, synced_collection, testdata):
        if isinstance(synced_collection, JSONDict):
            for key in (0, None, True):
                with pytest.deprecated_call(match="Use of.+as key is deprecated"):
                    synced_collection[key] = testdata
                assert str(key) in synced_collection
                assert synced_collection[str(key)] == testdata

    def test_keys_invalid_type(self, synced_collection, testdata):

        class A:
            pass
        for key in (0.0, A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                synced_collection[key] = testdata
        for key in ([], {}, dict()):
            with pytest.raises(TypeError):
                synced_collection[key] = testdata


class SyncedListTest(SyncedCollectionTest):

    @pytest.fixture(autouse=True)
    def base_collection(self):
        return [0]

    def test_from_base(self, base_collection):
        sd = SyncedCollection.from_base(
            **self._backend_kwargs, data=base_collection,
            backend=self._backend)
        assert isinstance(sd, self._collection_type)
        assert 0 in sd
        assert sd[0] == 0

    def test_from_base_explicit(self, base_collection):
        sd = self._backend_collection.from_base(
            **self._backend_kwargs, data=base_collection)
        assert isinstance(sd, self._collection_type)
        assert 0 in sd
        assert sd[0] == 0

    def test_init(self, synced_collection):
        assert len(synced_collection) == 0

    def test_invalid_kwargs(self, synced_collection):
        # JSONList raise an error when neither filename nor parent is passed.
        with pytest.raises(ValueError):
            return type(synced_collection)()

    def test_isinstance(self, synced_collection):
        assert isinstance(synced_collection, MutableSequence)
        assert isinstance(synced_collection, SyncedCollection)

    def test_set_get(self, synced_collection, testdata):
        synced_collection.clear()
        assert not bool(synced_collection)
        assert len(synced_collection) == 0
        synced_collection.append(testdata)
        assert bool(synced_collection)
        assert len(synced_collection) == 1
        assert synced_collection[0] == testdata
        synced_collection[0] = 1
        assert bool(synced_collection)
        assert len(synced_collection) == 1
        assert synced_collection[0] == 1

    @pytest.mark.skipif(not NUMPY, reason='test requires the numpy package')
    def test_set_get_numpy_data(self, synced_collection):
        data = numpy.random.rand(3, 4)
        data_as_list = data.tolist()
        synced_collection.reset(data)
        assert len(synced_collection) == len(data_as_list)
        assert synced_collection == data_as_list
        data2 = numpy.random.rand(3, 4)
        synced_collection.append(data2)
        assert len(synced_collection) == len(data_as_list) + 1
        assert synced_collection[len(data_as_list)] == data2.tolist()
        data3 = numpy.float_(3.14)
        synced_collection.append(data3)
        assert len(synced_collection) == len(data_as_list) + 2
        assert synced_collection[len(data_as_list) + 1] == data3

    def test_iter(self, synced_collection, testdata):
        d = [testdata, 43]
        synced_collection.extend(d)
        for i in range(len(synced_collection)):
            assert d[i] == synced_collection[i]
        assert i == 1

    def test_delete(self, synced_collection, testdata):
        synced_collection.append(testdata)
        assert len(synced_collection) == 1
        assert synced_collection[0] == testdata
        del synced_collection[0]
        assert len(synced_collection) == 0
        with pytest.raises(IndexError):
            synced_collection[0]

    def test_extend(self, synced_collection, testdata):
        d = [testdata]
        synced_collection.extend(d)
        assert len(synced_collection) == 1
        assert synced_collection[0] == d[0]
        d1 = testdata
        synced_collection += [d1]
        assert len(synced_collection) == 2
        assert synced_collection[0] == d[0]
        assert synced_collection[1] == d1

        # Ensure generators are exhausted only once by extend
        def data_generator():
            yield testdata
        synced_collection.extend(data_generator())
        assert len(synced_collection) == 3
        assert synced_collection[0] == d[0]
        assert synced_collection[1] == d1
        assert synced_collection[2] == testdata

        # Ensure generators are exhausted only once by __iadd__
        def data_generator():
            yield testdata
        synced_collection += data_generator()
        assert len(synced_collection) == 4
        assert synced_collection[0] == d[0]
        assert synced_collection[1] == d1
        assert synced_collection[2] == testdata
        assert synced_collection[3] == testdata

    def test_clear(self, synced_collection, testdata):
        synced_collection.append(testdata)
        assert len(synced_collection) == 1
        assert synced_collection[0] == testdata
        synced_collection.clear()
        assert len(synced_collection) == 0

    def test_reset(self, synced_collection):
        synced_collection.reset([1, 2, 3])
        assert len(synced_collection) == 3
        assert synced_collection == [1, 2, 3]
        synced_collection.reset()
        assert len(synced_collection) == 0
        synced_collection.reset([3, 4])
        assert len(synced_collection) == 2
        assert synced_collection == [3, 4]

        # invalid inputs
        with pytest.raises(ValueError):
            synced_collection.reset({'a': 1})

        with pytest.raises(ValueError):
            synced_collection.reset(1)

    def test_insert(self, synced_collection, testdata):
        synced_collection.reset([1, 2])
        assert len(synced_collection) == 2
        synced_collection.insert(1, testdata)
        assert len(synced_collection) == 3
        assert synced_collection[1] == testdata

    def test_reversed(self, synced_collection):
        data = [1, 2, 3]
        synced_collection.reset([1, 2, 3])
        assert len(synced_collection) == 3
        assert synced_collection == data
        for i, j in zip(reversed(synced_collection), reversed(data)):
            assert i == j

    def test_remove(self, synced_collection):
        synced_collection.reset([1, 2])
        assert len(synced_collection) == 2
        synced_collection.remove(1)
        assert len(synced_collection) == 1
        assert synced_collection[0] == 2
        synced_collection.reset([1, 2, 1])
        synced_collection.remove(1)
        assert len(synced_collection) == 2
        assert synced_collection[0] == 2
        assert synced_collection[1] == 1

    def test_call(self, synced_collection):
        synced_collection.reset([1, 2])
        assert len(synced_collection) == 2
        assert isinstance(synced_collection(), list)
        assert not isinstance(synced_collection(), SyncedCollection)
        assert synced_collection() == [1, 2]

    def test_update_recursive(self, synced_collection):
        synced_collection.reset([{'a': 1}, 'b', [1, 2, 3]])
        assert synced_collection == [{'a': 1}, 'b', [1, 2, 3]]
        data = ['a', 'b', [1, 2, 4], 'd']
        self.store(data)
        assert synced_collection == data
        data1 = ['a', 'b']
        self.store(data1)
        assert synced_collection == data1

        # invalid data in file
        data2 = {'a': 1}
        self.store(data2)
        with pytest.raises(ValueError):
            synced_collection.load()

    def test_reopen(self, synced_collection, testdata):
        try:
            synced_collection2 = deepcopy(synced_collection)
        except TypeError:
            # Use fallback implementation, deepcopy not supported by backend.
            synced_collection2 = synced_collection._pseudo_deepcopy()
        synced_collection.append(testdata)
        synced_collection.save()
        del synced_collection  # possibly unsafe
        synced_collection2.load()
        assert len(synced_collection2) == 1
        assert synced_collection2[0] == testdata

    def test_copy_as_list(self, synced_collection, testdata):
        synced_collection.append(testdata)
        assert synced_collection[0] == testdata
        copy = list(synced_collection)
        del synced_collection
        assert copy[0] == testdata

    def test_repr(self, synced_collection):
        repr(synced_collection)
        p = eval(repr(synced_collection))
        assert repr(p) == repr(synced_collection)
        assert p == synced_collection

    def test_str(self, synced_collection):
        str(synced_collection) == str(synced_collection.to_base())

    def test_nested_list(self, synced_collection):
        synced_collection.reset([1, 2, 3])
        synced_collection.append([2, 4])
        child1 = synced_collection[3]
        child2 = synced_collection[3]
        assert child1 == child2
        assert isinstance(child1, type(child2))
        assert isinstance(child1, type(synced_collection))
        assert id(child1) == id(child2)
        child1.append(1)
        assert child2[2] == child1[2]
        assert child1 == child2
        assert len(synced_collection) == 4
        assert isinstance(child1, type(child2))
        assert isinstance(child1, type(synced_collection))
        assert id(child1) == id(child2)
        del child1[0]
        assert child1 == child2
        assert len(synced_collection) == 4
        assert isinstance(child1, type(child2))
        assert isinstance(child1, type(synced_collection))
        assert id(child1) == id(child2)

    def test_nested_list_with_dict(self, synced_collection):
        synced_collection.reset([{'a': [1, 2, 3, 4]}])
        child1 = synced_collection[0]
        child2 = synced_collection[0]['a']
        assert isinstance(child2, SyncedCollection)
        assert isinstance(child1, SyncedCollection)
