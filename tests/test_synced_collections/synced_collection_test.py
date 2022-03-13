# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import platform
from collections.abc import MutableMapping, MutableSequence
from copy import deepcopy
from typing import Any, Tuple, Type

import pytest

from signac.errors import KeyTypeError
from signac.synced_collections import SyncedCollection
from signac.synced_collections.numpy_utils import NumpyConversionWarning

PYPY = "PyPy" in platform.python_implementation()


try:
    import numpy

    NUMPY = True

    NUMPY_INT_TYPES: Tuple[Type, ...] = (
        numpy.bool_,
        numpy.byte,
        numpy.ubyte,
        numpy.short,
        numpy.ushort,
        numpy.intc,
        numpy.uintc,
        numpy.int_,
        numpy.uint,
        numpy.longlong,
        numpy.ulonglong,
        numpy.int8,
        numpy.int16,
        numpy.int32,
        numpy.int64,
        numpy.uint8,
        numpy.uint16,
        numpy.uint32,
        numpy.uint64,
        numpy.intp,
        numpy.uintp,
    )

    NUMPY_FLOAT_TYPES: Tuple[Type, ...] = (
        numpy.half,
        numpy.float16,
        numpy.single,
        numpy.longdouble,
        numpy.float32,
        numpy.float64,
        numpy.float128,
        numpy.float_,
    )

    NUMPY_COMPLEX_TYPES: Tuple[Type, ...] = (
        numpy.csingle,
        numpy.cdouble,
        numpy.clongdouble,
        numpy.complex64,
        numpy.complex128,
        numpy.complex_,
    )
    NUMPY_SHAPES: Tuple[Any, ...] = (None, (1,), (2,), (2, 2))

    # Older numpy versions don't have the new rngs.
    try:
        rng = numpy.random.default_rng()
        random_sample = rng.random
        randint = rng.integers
    except AttributeError:
        random_sample = numpy.random.random_sample
        randint = numpy.random.randint

except ImportError:
    NUMPY = False
    NUMPY_INT_TYPES = ()
    NUMPY_FLOAT_TYPES = ()
    NUMPY_COMPLEX_TYPES = ()
    NUMPY_SHAPES = ()


class SyncedCollectionTest:
    """The parent for all synced collection tests.

    This class defines the standard APIs that are expected of all test subclasses.
    Following these protocols allows different backends and data types to share
    most test by just defining the expected additional variables.
    """

    def store(self, synced_collection, data):
        """Directly store data to the backend using its own API.

        This method should bypass the synced_collection, just using it to get
        information on the underlying resource (e.g. a file) and then saving to
        it directly. This is used, for instance, to test data integrity validation.
        """
        raise NotImplementedError("All backend tests must implement the store method.")

    @pytest.fixture
    def synced_collection(self):
        """Generate a synced collection of the appropriate type.

        The type of the synced collection should be specified by the _collection_type
        class variable.
        """
        raise NotImplementedError(
            "All backend tests must implement a synced_collection fixture "
            "that returns an empty instance."
        )

    @pytest.fixture
    def base_collection(self):
        """Generate a collection of the base data type.

        This fixture should generate a base collection (e.g. a list or a dict)
        that can be used to set the data of a synced collection of the corresponding
        type for comparison in tests.
        """
        raise NotImplementedError(
            "All data type tests must implement a base_collection fixture that "
            "returns an instance populated with test data."
        )


class SyncedDictTest(SyncedCollectionTest):
    @pytest.fixture
    def base_collection(self):
        return {"a": 0}

    def test_init(self, synced_collection):
        assert len(synced_collection) == 0

    def test_init_positional(self, synced_collection_positional):
        assert len(synced_collection_positional) == 0

    def test_isinstance(self, synced_collection):
        assert isinstance(synced_collection, SyncedCollection)
        assert isinstance(synced_collection, MutableMapping)

    def test_set_get(self, synced_collection, testdata):
        key = "setget"
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
        key = "setgetexplicitnested"
        synced_collection.setdefault("a", dict())
        child1 = synced_collection["a"]
        child2 = synced_collection["a"]
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
        key = "copy_value"
        key2 = "copy_value2"
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
        key1 = "iter1"
        key2 = "iter2"
        d = {key1: testdata, key2: testdata}
        synced_collection.update(d)
        assert key1 in synced_collection
        assert key2 in synced_collection
        for i, key in enumerate(synced_collection):
            assert key in d
            assert d[key] == synced_collection[key]
        assert i == 1

    def test_delete(self, synced_collection, testdata):
        key = "delete"
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
        del synced_collection["delete"]
        assert len(synced_collection) == 0
        with pytest.raises(KeyError):
            synced_collection[key]

    def test_update(self, synced_collection, testdata):
        key = "update"
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
        assert synced_collection["update2"] == testdata
        # same key in other dict and as kwarg with different values
        synced_collection.update({key: 1}, update=2)  # here key is 'update'
        assert len(synced_collection) == 2
        assert synced_collection[key] == 2
        # update using list of key and value pair
        synced_collection.update([("update2", 1), ("update3", 2)])
        assert len(synced_collection) == 3
        assert synced_collection["update2"] == 1
        assert synced_collection["update3"] == 2

    def test_pop(self, synced_collection, testdata):
        key = "pop"
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        d1 = synced_collection.pop(key)
        assert len(synced_collection) == 0
        assert testdata == d1
        with pytest.raises(KeyError):
            synced_collection[key]
        d2 = synced_collection.pop(key, "default")
        assert len(synced_collection) == 0
        assert d2 == "default"

    def test_popitem(self, synced_collection, testdata):
        key = "pop"
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
        data = {"value1": testdata, "value_nested": {"value2": testdata}}
        synced_collection.reset(data)
        assert "value1" in synced_collection
        assert "value_nested" in synced_collection
        for val in synced_collection.values():
            assert not isinstance(val, SyncedCollection)
            assert val in data.values()

    def test_items(self, synced_collection, testdata):
        data = {"item1": testdata, "item_nested": {"item2": testdata}}
        synced_collection.reset(data)
        assert "item1" in synced_collection
        assert "item_nested" in synced_collection
        for key, val in synced_collection.items():
            assert synced_collection[key] == data[key]
            assert not isinstance(val, type(synced_collection))
            assert (key, val) in data.items()

    def test_setdefault(self, synced_collection, testdata):
        key = "setdefault"
        ret = synced_collection.setdefault(key, testdata)
        assert ret == testdata
        assert key in synced_collection
        assert synced_collection[key] == testdata
        ret = synced_collection.setdefault(key, 1)
        assert ret == testdata
        assert synced_collection[key] == testdata

    def test_reset(self, synced_collection, testdata):
        key = "reset"
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        synced_collection.reset({"reset": "abc"})
        assert len(synced_collection) == 1
        assert synced_collection[key] == "abc"

        # invalid input
        with pytest.raises(ValueError):
            synced_collection.reset([0, 1])

    def test_clear(self, synced_collection, testdata):
        key = "clear"
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
        str(synced_collection) == str(synced_collection())

    def test_call(self, synced_collection, testdata):
        key = "call"
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        assert isinstance(synced_collection(), dict)
        assert not isinstance(synced_collection(), SyncedCollection)

        def recursive_convert(d):
            return {
                k: (recursive_convert(v) if isinstance(v, SyncedCollection) else v)
                for k, v in d.items()
            }

        assert synced_collection() == recursive_convert(synced_collection)
        assert synced_collection() == {"call": testdata}

    def test_reopen(self, synced_collection, testdata):
        key = "reopen"
        synced_collection[key] = testdata
        try:
            synced_collection2 = deepcopy(synced_collection)
        except TypeError:
            # Ignore backends that don't support deepcopy.
            return
        synced_collection._save()
        del synced_collection  # possibly unsafe
        synced_collection2._load()
        assert len(synced_collection2) == 1
        assert synced_collection2[key] == testdata

    def test_update_recursive(self, synced_collection, testdata):
        synced_collection["a"] = {"a": 1}
        synced_collection["b"] = "test"
        synced_collection["c"] = [0, 1, 2]
        assert "a" in synced_collection
        assert "b" in synced_collection
        assert "c" in synced_collection
        data = {"a": 1, "c": [0, 1, 3], "d": 1}
        self.store(synced_collection, data)
        assert synced_collection == data

        # Test multiple changes. kwargs should supersede the mapping
        synced_collection.update({"a": 1, "b": 3}, a={"foo": "bar"}, d=[2, 3])
        assert synced_collection == {
            "a": {"foo": "bar"},
            "b": 3,
            "c": [0, 1, 3],
            "d": [2, 3],
        }

        # Test multiple changes using a sequence of key-value pairs.
        synced_collection.update((("d", [{"bar": "baz"}, 1]), ("c", 1)), e=("a", "b"))
        assert synced_collection == {
            "a": {"foo": "bar"},
            "b": 3,
            "c": 1,
            "d": [{"bar": "baz"}, 1],
            "e": ["a", "b"],
        }

        # invalid data
        data = [1, 2, 3]
        self.store(synced_collection, data)
        with pytest.raises(ValueError):
            synced_collection._load()

    def test_copy_as_dict(self, synced_collection, testdata):
        key = "copy"
        synced_collection[key] = testdata
        copy = dict(synced_collection)
        del synced_collection
        assert key in copy
        assert copy[key] == testdata

    def test_nested_dict(self, synced_collection):
        synced_collection["a"] = dict(a=dict())
        child1 = synced_collection["a"]
        child2 = synced_collection["a"]["a"]
        assert isinstance(child1, type(synced_collection))
        assert isinstance(child1, type(child2))

    def test_nested_dict_with_list(self, synced_collection):
        synced_collection["a"] = [1, 2, 3]
        child1 = synced_collection["a"]
        synced_collection["a"].append(dict(a=[1, 2, 3]))
        child2 = synced_collection["a"][3]
        child3 = synced_collection["a"][3]["a"]
        assert isinstance(child2, type(synced_collection))
        assert isinstance(child1, type(child3))
        assert isinstance(child1, SyncedCollection)
        assert isinstance(child3, SyncedCollection)

    def test_write_invalid_type(self, synced_collection, testdata):
        class Foo:
            pass

        key = "write_invalid_type"
        synced_collection[key] = testdata
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata
        d2 = Foo()
        with pytest.raises(TypeError):
            synced_collection[key + "2"] = d2
        assert len(synced_collection) == 1
        assert synced_collection[key] == testdata

    def test_keys_str_type(self, synced_collection, testdata):
        class MyStr(str):
            pass

        for key in ("key", MyStr("key")):
            synced_collection[key] = testdata
            assert key in synced_collection
            assert synced_collection[key] == testdata

    # TODO: This test should only be applied for backends where JSON-formatting
    # is required.
    def test_keys_invalid_type(self, synced_collection, testdata):
        class A:
            pass

        for key in (A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                synced_collection[key] = testdata
        for key in ([], {}):
            with pytest.raises(TypeError):
                synced_collection[key] = testdata

    def test_multithreaded(self, synced_collection):
        """Test multithreaded runs of synced dicts."""
        if not type(synced_collection)._supports_threading:
            return

        from concurrent.futures import ThreadPoolExecutor
        from json.decoder import JSONDecodeError
        from threading import current_thread

        def set_value(sd):
            sd[current_thread().name] = current_thread().name

        num_threads = 50
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(executor.map(set_value, [synced_collection] * num_threads * 10))

        assert len(synced_collection) == num_threads

        # Now clear the data and try again with multithreading disabled. Unless
        # we're very unlucky, some of these threads should overwrite each
        # other.
        type(synced_collection).disable_multithreading()
        synced_collection.clear()

        try:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                list(executor.map(set_value, [synced_collection] * num_threads * 10))
        except (RuntimeError, JSONDecodeError, KeyError):
            # This line may raise an exception, or it may successfully complete
            # but not modify all the expected data. If it raises an exception,
            # then the underlying data is likely to be invalid, so we must
            # clear it.
            synced_collection.clear()
        else:
            # PyPy is fast enough that threads will frequently complete without
            # being preempted, so this check is frequently invalidated.
            if not PYPY:
                assert len(synced_collection) != num_threads

        # For good measure, try reenabling multithreading and test to be safe.
        type(synced_collection).enable_multithreading()
        synced_collection.clear()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(executor.map(set_value, [synced_collection] * num_threads * 10))

        assert len(synced_collection) == num_threads

    @pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
    @pytest.mark.parametrize("dtype", NUMPY_INT_TYPES)
    @pytest.mark.parametrize("shape", NUMPY_SHAPES)
    def test_set_get_numpy_int_data(self, synced_collection, dtype, shape):
        """Test setting scalar int types, which should always work."""
        try:
            max_value = numpy.iinfo(dtype).max
        except ValueError:
            max_value = 1
        value = randint(max_value, dtype=dtype, size=shape)

        with pytest.warns(NumpyConversionWarning):
            synced_collection["numpy_dtype_val"] = value
        raw_value = value.item() if shape is None else value.tolist()
        assert synced_collection["numpy_dtype_val"] == raw_value

    @pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
    @pytest.mark.parametrize("dtype", NUMPY_FLOAT_TYPES)
    @pytest.mark.parametrize("shape", NUMPY_SHAPES)
    def test_set_get_numpy_float_data(self, synced_collection, dtype, shape):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        value = dtype(random_sample(shape))

        # If casting via item does not give a base Python type, the number
        # should fail to set correctly.
        raw_value = value.item() if shape is None else value.tolist()
        test_value = value.item(0) if isinstance(raw_value, list) else raw_value
        has_corresponding_python_type = isinstance(
            test_value, (numpy.number, numpy.bool_)
        )

        if has_corresponding_python_type:
            with pytest.raises((ValueError, TypeError)), pytest.warns(
                NumpyConversionWarning
            ):
                synced_collection["numpy_dtype_val"] = value
        else:
            with pytest.warns(NumpyConversionWarning):
                synced_collection["numpy_dtype_val"] = value
            assert synced_collection["numpy_dtype_val"] == raw_value

    @pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
    @pytest.mark.parametrize("dtype", NUMPY_COMPLEX_TYPES)
    @pytest.mark.parametrize("shape", NUMPY_SHAPES)
    def test_set_get_numpy_complex_data(self, synced_collection, dtype, shape):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        value = dtype(random_sample(shape))

        with pytest.raises((ValueError, TypeError)), pytest.warns(
            NumpyConversionWarning
        ):
            synced_collection["numpy_dtype_val"] = value


class SyncedListTest(SyncedCollectionTest):
    @pytest.fixture
    def base_collection(self):
        return [0]

    def test_init(self, synced_collection):
        assert len(synced_collection) == 0

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
        synced_collection.reset([3, 4])
        assert len(synced_collection) == 2
        assert synced_collection == [3, 4]

        # invalid inputs
        with pytest.raises(ValueError):
            synced_collection.reset({"a": 1})

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
        synced_collection.reset([{"a": 1}, "b", [1, 2, 3]])
        assert synced_collection == [{"a": 1}, "b", [1, 2, 3]]
        data = ["a", "b", [1, 2, 4], "d"]
        self.store(synced_collection, data)
        assert synced_collection == data
        data1 = ["a", "b"]
        self.store(synced_collection, data1)
        assert synced_collection == data1

        # invalid data in file
        data2 = {"a": 1}
        self.store(synced_collection, data2)
        with pytest.raises(ValueError):
            synced_collection._load()

    def test_reopen(self, synced_collection, testdata):
        try:
            synced_collection2 = deepcopy(synced_collection)
        except TypeError:
            # Ignore backends that don't support deepcopy.
            return
        synced_collection.append(testdata)
        synced_collection._save()
        del synced_collection  # possibly unsafe
        synced_collection2._load()
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
        str(synced_collection) == str(synced_collection())

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
        synced_collection.reset([{"a": [1, 2, 3, 4]}])
        child1 = synced_collection[0]
        child2 = synced_collection[0]["a"]
        assert isinstance(child2, SyncedCollection)
        assert isinstance(child1, SyncedCollection)

    def test_multithreaded(self, synced_collection):
        """Test multithreaded runs of synced lists."""
        if not type(synced_collection)._supports_threading:
            return

        from concurrent.futures import ThreadPoolExecutor

        def append_value(sl):
            sl.append(0)

        num_threads = 50
        num_elements = num_threads * 10
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(executor.map(append_value, [synced_collection] * num_elements))

        assert len(synced_collection) == num_elements

    @pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
    @pytest.mark.parametrize("dtype", NUMPY_INT_TYPES)
    @pytest.mark.parametrize("shape", NUMPY_SHAPES)
    def test_set_get_numpy_int_data(self, synced_collection, dtype, shape):
        """Test setting scalar int types, which should always work."""
        try:
            max_value = numpy.iinfo(dtype).max
        except ValueError:
            max_value = 1
        value = randint(max_value, dtype=dtype, size=shape)

        with pytest.warns(NumpyConversionWarning):
            synced_collection.append(value)
        raw_value = value.item() if shape is None else value.tolist()
        assert synced_collection[-1] == raw_value

        # Test assignment after append.
        with pytest.warns(NumpyConversionWarning):
            synced_collection[-1] = value

    @pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
    @pytest.mark.parametrize("dtype", NUMPY_FLOAT_TYPES)
    @pytest.mark.parametrize("shape", NUMPY_SHAPES)
    def test_set_get_numpy_float_data(self, synced_collection, dtype, shape):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        value = dtype(random_sample(shape))

        # If casting via item does not give a base Python type, the number
        # should fail to set correctly.
        raw_value = value.item() if shape is None else value.tolist()
        test_value = value.item(0) if isinstance(raw_value, list) else raw_value
        should_fail = isinstance(test_value, (numpy.number, numpy.bool_))

        if should_fail:
            with pytest.raises((ValueError, TypeError)), pytest.warns(
                NumpyConversionWarning
            ):
                synced_collection.append(value)
        else:
            with pytest.warns(NumpyConversionWarning):
                synced_collection.append(value)
            assert synced_collection[-1] == raw_value

            # Test assignment after append.
            with pytest.warns(NumpyConversionWarning):
                synced_collection[-1] = value

    @pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
    @pytest.mark.parametrize("dtype", NUMPY_COMPLEX_TYPES)
    @pytest.mark.parametrize("shape", NUMPY_SHAPES)
    def test_set_get_numpy_complex_data(self, synced_collection, dtype, shape):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        value = dtype(random_sample(shape))

        with pytest.raises((ValueError, TypeError)), pytest.warns(
            NumpyConversionWarning
        ):
            synced_collection.append(value)

        with pytest.raises((ValueError, TypeError)), pytest.warns(
            NumpyConversionWarning
        ):
            synced_collection[-1] = value

    @pytest.mark.parametrize("dtype", NUMPY_INT_TYPES)
    @pytest.mark.parametrize("shape", NUMPY_SHAPES)
    def test_reset_numpy_int_data(self, synced_collection, dtype, shape):
        """Test setting scalar int types, which should always work."""
        try:
            max_value = numpy.iinfo(dtype).max
        except ValueError:
            max_value = 1
        value = randint(max_value, dtype=dtype, size=shape)

        if shape is None:
            with pytest.raises((ValueError, TypeError)), pytest.warns(
                NumpyConversionWarning
            ):
                synced_collection.reset(value)
        else:
            with pytest.warns(NumpyConversionWarning):
                synced_collection.reset(value)
            assert synced_collection == value.tolist()
