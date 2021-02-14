# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

try:
    import numpy as np

    NUMPY = True

    NUMPY_INT_TYPES = [
        np.bool_,
        np.byte,
        np.ubyte,
        np.short,
        np.ushort,
        np.intc,
        np.uintc,
        np.int_,
        np.uint,
        np.longlong,
        np.ulonglong,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.intp,
        np.uintp,
    ]

    NUMPY_FLOAT_TYPES = [
        np.half,
        np.float16,
        np.single,
        np.longdouble,
        np.float32,
        np.float64,
        np.float128,
        np.float_,
    ]

    NUMPY_COMPLEX_TYPES = [
        np.csingle,
        np.cdouble,
        np.clongdouble,
        np.complex64,
        np.complex128,
        np.complex_,
    ]

except ImportError:
    NUMPY = False
    NUMPY_INT_TYPES = []
    NUMPY_FLOAT_TYPES = []
    NUMPY_COMPLEX_TYPES = []


@pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
class SyncedListNumpyTest:
    @pytest.mark.parametrize("dtype", NUMPY_INT_TYPES)
    @pytest.mark.parametrize("shape", (None, (1,), (2,)))
    def test_set_get_numpy_int_data(self, synced_collection, dtype, shape):
        """Test setting scalar int types, which should always work."""
        try:
            max_value = np.iinfo(dtype).max
        except ValueError:
            max_value = 1
        value = np.random.randint(max_value, dtype=dtype, size=shape)

        # TODO: Use pytest.warns once the warning is added.
        synced_collection.append(value)
        raw_value = value.item() if shape is None else value.tolist()
        assert synced_collection[-1] == raw_value

        # Test assignment after append.
        synced_collection[-1] = value

    @pytest.mark.parametrize("dtype", NUMPY_FLOAT_TYPES)
    @pytest.mark.parametrize("shape", (None, (1,), (2,)))
    def test_set_get_numpy_float_data(self, synced_collection, dtype, shape):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        # Explicitly get an array with a shape so we can
        value = dtype(np.random.random_sample(shape))

        # If casting via item does not give a base Python type, the number
        # should fail to set correctly.
        raw_value = value.item() if shape is None else value.tolist()
        test_value = value[0].item() if isinstance(raw_value, list) else raw_value
        should_fail = isinstance(test_value, (np.number, np.bool_))

        if should_fail:
            with pytest.raises((ValueError, TypeError)):
                synced_collection.append(value)
        else:
            # TODO: Use pytest.warns once the warning is added.
            synced_collection.append(value)
            assert synced_collection[-1] == raw_value

            # Test assignment after append.
            synced_collection[-1] = value

    @pytest.mark.parametrize("dtype", NUMPY_COMPLEX_TYPES)
    @pytest.mark.parametrize("shape", (None, (1,), (2,)))
    def test_set_get_numpy_complex_data(self, synced_collection, dtype, shape):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        # Explicitly get an array with a shape so we can
        value = dtype(np.random.random_sample(shape))

        # TODO: Use pytest.warns once the warning is added.
        with pytest.raises((ValueError, TypeError)):
            synced_collection.append(value)

        with pytest.raises((ValueError, TypeError)):
            synced_collection[-1] = value

    @pytest.mark.parametrize("dtype", NUMPY_INT_TYPES)
    @pytest.mark.parametrize("shape", (None, (1,), (2,)))
    def test_reset_numpy_int_data(self, synced_collection, dtype, shape):
        """Test setting scalar int types, which should always work."""
        try:
            max_value = np.iinfo(dtype).max
        except ValueError:
            max_value = 1
        value = np.random.randint(max_value, dtype=dtype, size=shape)

        # TODO: Use pytest.warns once the warning is added.
        if shape is None:
            with pytest.raises(ValueError):
                synced_collection.reset(value)
        else:
            synced_collection.reset(value)
            assert synced_collection == value.tolist()

    def test_set_get_numpy_data(self, synced_collection):
        data = np.random.rand(3, 4)
        data_as_list = data.tolist()
        synced_collection.reset(data)
        assert len(synced_collection) == len(data_as_list)
        assert synced_collection == data_as_list
        data2 = np.random.rand(3, 4)
        synced_collection.append(data2)
        assert len(synced_collection) == len(data_as_list) + 1
        assert synced_collection[len(data_as_list)] == data2.tolist()
        data3 = np.float_(3.14)
        synced_collection.append(data3)
        assert len(synced_collection) == len(data_as_list) + 2
        assert synced_collection[len(data_as_list) + 1] == data3


# """
# Cases to test on the np side of things:
#
# Different cases from the synced collection perspective:
#    Resetting a syncedlist to a np array
#    Creating a syncedlist from a np array
#
# Need the cartesian product of the above two things.
# """


@pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
class SyncedDictNumpyTest:
    @pytest.mark.parametrize("dtype", NUMPY_INT_TYPES)
    @pytest.mark.parametrize("shape", (None, (1,), (2,)))
    def test_set_get_numpy_int_data(self, synced_collection, dtype, shape):
        """Test setting scalar int types, which should always work."""
        try:
            max_value = np.iinfo(dtype).max
        except ValueError:
            max_value = 1
        value = np.random.randint(max_value, dtype=dtype, size=shape)

        # TODO: Use pytest.warns once the warning is added.
        synced_collection["numpy_dtype_val"] = value
        raw_value = value.item() if shape is None else value.tolist()
        assert synced_collection["numpy_dtype_val"] == raw_value

    @pytest.mark.parametrize("dtype", NUMPY_FLOAT_TYPES)
    @pytest.mark.parametrize("shape", (None, (1,), (2,)))
    def test_set_get_numpy_float_data(self, synced_collection, dtype, shape):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        # Explicitly get an array with a shape so we can
        value = dtype(np.random.random_sample(shape))

        # If casting via item does not give a base Python type, the number
        # should fail to set correctly.
        raw_value = value.item() if shape is None else value.tolist()
        test_value = value[0].item() if isinstance(raw_value, list) else raw_value
        should_fail = isinstance(test_value, (np.number, np.bool_))

        if should_fail:
            with pytest.raises((ValueError, TypeError)):
                synced_collection["numpy_dtype_val"] = value
        else:
            # TODO: Use pytest.warns once the warning is added.
            synced_collection["numpy_dtype_val"] = value
            assert synced_collection["numpy_dtype_val"] == raw_value

    @pytest.mark.parametrize("dtype", NUMPY_COMPLEX_TYPES)
    @pytest.mark.parametrize("shape", (None, (1,), (2,)))
    def test_set_get_numpy_complex_data(self, synced_collection, dtype, shape):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        # Explicitly get an array with a shape so we can
        value = dtype(np.random.random_sample(shape))

        # TODO: Use pytest.warns once the warning is added.
        with pytest.raises((ValueError, TypeError)):
            synced_collection["numpy_dtype_val"] = value
