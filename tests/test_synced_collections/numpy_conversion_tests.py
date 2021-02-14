# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


np = numpy


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


@pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
class SyncedListNumpyTest:
    NUMPY_INT_TYPES = NUMPY_INT_TYPES
    NUMPY_FLOAT_TYPES = NUMPY_FLOAT_TYPES
    NUMPY_COMPLEX_TYPES = NUMPY_COMPLEX_TYPES

    def test_set_get_numpy_scalar_int_data(self, synced_collection):
        """Test setting scalar int types, which should always work."""
        try:
            for dtype in self.NUMPY_INT_TYPES:
                for size in (None, ()):
                    try:
                        max_value = np.iinfo(dtype).max
                    except ValueError:
                        max_value = 1
                    value = np.random.randint(max_value, dtype=dtype, size=size)
                    # TODO: Use pytest.warns once the warning is added.
                    synced_collection.append(value)
                    assert synced_collection[-1] == value.item()

                    # Test assignment after append.
                    synced_collection[-1] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}, size={size}") from e

    def test_set_get_numpy_scalar_float_data(self, synced_collection):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        try:
            for dtype in self.NUMPY_FLOAT_TYPES:
                for squeeze in (True, False):
                    # Explicitly get an array with a shape so we can
                    value = dtype(np.random.rand())
                    if squeeze:
                        value = value.squeeze()
                    raw_value = value.item()

                    # TODO: Use pytest.warns once the warning is added.

                    # If casting via item does not give a base Python type, the number
                    # should fail to set correctly.
                    if isinstance(raw_value, (numpy.number, numpy.bool_)):
                        with pytest.raises((ValueError, TypeError)):
                            synced_collection.append(value)
                    else:
                        synced_collection.append(value)
                        assert synced_collection[-1] == raw_value

                        # Test assignment after append.
                        synced_collection[-1] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}, squeeze={squeeze}") from e

    def test_set_get_numpy_scalar_complex_data(self, synced_collection):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        try:
            for dtype in self.NUMPY_COMPLEX_TYPES:
                for squeeze in (True, False):
                    # Explicitly get an array with a shape so we can
                    value = dtype(np.random.rand())
                    if squeeze:
                        value = value.squeeze()

                    # TODO: Use pytest.warns once the warning is added.
                    with pytest.raises((ValueError, TypeError)):
                        synced_collection.append(value)

                    with pytest.raises((ValueError, TypeError)):
                        synced_collection[-1] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}, squeeze={squeeze}") from e

    def test_set_get_numpy_vector_int_data(self, synced_collection):
        """Test setting scalar int types, which should always work."""
        try:
            for dtype in self.NUMPY_INT_TYPES:
                try:
                    max_value = np.iinfo(dtype).max
                except ValueError:
                    max_value = 1
                value = np.random.randint(max_value, dtype=dtype, size=(2,))
                # TODO: Use pytest.warns once the warning is added.
                synced_collection.append(value)
                assert synced_collection[-1] == value.tolist()

                # Test assignment after append.
                synced_collection[-1] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}") from e

    def test_set_get_numpy_vector_float_data(self, synced_collection):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        try:
            for dtype in self.NUMPY_FLOAT_TYPES:
                # Explicitly get an array with a shape so we can
                value = dtype(np.random.rand(2))

                # TODO: Use pytest.warns once the warning is added.

                # If casting via item does not give a base Python type, the number
                # should fail to set correctly.
                if isinstance(value[0].item(), (numpy.number, numpy.bool_)):
                    with pytest.raises((ValueError, TypeError)):
                        synced_collection.append(value)
                else:
                    synced_collection.append(value)
                    assert synced_collection[-1] == value.tolist()

                    # Test assignment after append.
                    synced_collection[-1] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}") from e

    def test_set_get_numpy_vector_complex_data(self, synced_collection):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        try:
            for dtype in self.NUMPY_COMPLEX_TYPES:
                # Explicitly get an array with a shape so we can
                value = dtype(np.random.rand(2))

                # TODO: Use pytest.warns once the warning is added.
                with pytest.raises((ValueError, TypeError)):
                    synced_collection.append(value)

                with pytest.raises((ValueError, TypeError)):
                    synced_collection[-1] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}") from e

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


# """
# Cases to test on the numpy side of things:
#
# Different cases from the synced collection perspective:
#    Resetting a syncedlist to a numpy array
#    Creating a syncedlist from a numpy array
#
# Need the cartesian product of the above two things.
# """


@pytest.mark.skipif(not NUMPY, reason="This test requires the numpy package.")
class SyncedDictNumpyTest:
    NUMPY_INT_TYPES = NUMPY_INT_TYPES
    NUMPY_FLOAT_TYPES = NUMPY_FLOAT_TYPES
    NUMPY_COMPLEX_TYPES = NUMPY_COMPLEX_TYPES

    def test_set_get_numpy_scalar_int_data(self, synced_collection):
        """Test setting scalar int types, which should always work."""
        try:
            for dtype in self.NUMPY_INT_TYPES:
                for size in (None, ()):
                    try:
                        max_value = np.iinfo(dtype).max
                    except ValueError:
                        max_value = 1
                    value = np.random.randint(max_value, dtype=dtype, size=size)
                    # TODO: Use pytest.warns once the warning is added.
                    synced_collection["numpy_dtype_val"] = value
                    assert synced_collection["numpy_dtype_val"] == value.item()
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}, size={size}") from e

    def test_set_get_numpy_scalar_float_data(self, synced_collection):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        try:
            for dtype in self.NUMPY_FLOAT_TYPES:
                for squeeze in (True, False):
                    # Explicitly get an array with a shape so we can
                    value = dtype(np.random.rand())
                    if squeeze:
                        value = value.squeeze()
                    raw_value = value.item()

                    # TODO: Use pytest.warns once the warning is added.

                    # If casting via item does not give a base Python type, the number
                    # should fail to set correctly.
                    if isinstance(raw_value, (numpy.number, numpy.bool_)):
                        with pytest.raises((ValueError, TypeError)):
                            synced_collection["numpy_dtype_val"] = value
                    else:
                        synced_collection["numpy_dtype_val"] = value
                        assert synced_collection["numpy_dtype_val"] == raw_value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}, squeeze={squeeze}") from e

    def test_set_get_numpy_scalar_complex_data(self, synced_collection):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        try:
            for dtype in self.NUMPY_COMPLEX_TYPES:
                for squeeze in (True, False):
                    # Explicitly get an array with a shape so we can
                    value = dtype(np.random.rand())
                    if squeeze:
                        value = value.squeeze()

                    # TODO: Use pytest.warns once the warning is added.
                    with pytest.raises((ValueError, TypeError)):
                        synced_collection["numpy_dtype_val"] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}, squeeze={squeeze}") from e

    def test_set_get_numpy_vector_int_data(self, synced_collection):
        """Test setting scalar int types, which should always work."""
        try:
            for dtype in self.NUMPY_INT_TYPES:
                try:
                    max_value = np.iinfo(dtype).max
                except ValueError:
                    max_value = 1
                value = np.random.randint(max_value, dtype=dtype, size=(2,))
                # TODO: Use pytest.warns once the warning is added.
                synced_collection["numpy_dtype_val"] = value
                assert synced_collection["numpy_dtype_val"] == value.tolist()
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}") from e

    def test_set_get_numpy_vector_float_data(self, synced_collection):
        """Test setting scalar float types, which work if a raw Python analog exists."""
        try:
            for dtype in self.NUMPY_FLOAT_TYPES:
                # Explicitly get an array with a shape so we can
                value = dtype(np.random.rand(2))

                # TODO: Use pytest.warns once the warning is added.

                # If casting via item does not give a base Python type, the number
                # should fail to set correctly.
                if isinstance(value[0].item(), (numpy.number, numpy.bool_)):
                    with pytest.raises((ValueError, TypeError)):
                        synced_collection["numpy_dtype_val"] = value
                else:
                    synced_collection["numpy_dtype_val"] = value
                    assert synced_collection["numpy_dtype_val"] == value.tolist()
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}") from e

    def test_set_get_numpy_vector_complex_data(self, synced_collection):
        """Test setting scalar complex types, which should always fail."""
        # Note that the current behavior of this test is based on the fact that
        # all backends rely on JSON-serialization (at least implicitly), even
        # non-JSON backends. This test may have to be generalized if we add any
        # backends that support other data, or if we want to test cases like
        # ZarrCollection with a non-JSON codec (alternatives are supported, but
        # not a priority to test here).
        try:
            for dtype in self.NUMPY_COMPLEX_TYPES:
                # Explicitly get an array with a shape so we can
                value = dtype(np.random.rand(2))

                # TODO: Use pytest.warns once the warning is added.
                with pytest.raises((ValueError, TypeError)):
                    synced_collection["numpy_dtype_val"] = value
        except Exception as e:
            # Re-raise so that we can also indicate which dtype failed.
            raise AssertionError(f"Failed for {dtype}") from e
