# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from signac.synced_collections.errors import InvalidKeyError, KeyTypeError
from signac.synced_collections.validators import (
    json_format_validator,
    no_dot_in_key,
    require_string_key,
)

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


class TestRequireStringKey:
    def test_valid_data(self, testdata):
        test_dict = {}

        key = "valid_str"
        test_dict[key] = testdata
        require_string_key(test_dict)
        assert key in test_dict
        assert test_dict[key] == testdata

    def test_invalid_data(self, testdata):
        # invalid key types
        for key in (0.0, 1.0 + 2.0j, (1, 2, 3), 1, False, None):
            with pytest.raises(KeyTypeError):
                require_string_key({key: testdata})


class TestNoDotInKey:
    def test_valid_data(self, testdata):
        test_dict = {}
        # valid data
        for key in ("valid_str", "another_valid_str"):
            test_dict[key] = testdata
            no_dot_in_key(test_dict)
            assert key in test_dict
            assert test_dict[key] == testdata

    def test_invalid_data(self, testdata):
        # dict key containing dot
        with pytest.raises(InvalidKeyError):
            no_dot_in_key({"a.b": testdata})
        # nested dict key containing dot
        with pytest.raises(InvalidKeyError):
            no_dot_in_key({"nested": {"a.b": 1}})
        # list containing dict
        with pytest.raises(InvalidKeyError):
            no_dot_in_key([{"a.b": 1}])
        # invalid key types
        for key in (0.0, 1.0 + 2.0j, (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                no_dot_in_key({key: testdata})


class TestJSONFormatValidator:
    def test_valid_data(self):
        for data in ("foo", 1, 1.0, True, None, {}, []):
            json_format_validator(data)
            json_format_validator({"test_key": data})
        json_format_validator(("foo", 1, 1.0, True, None, {}, []))

    def test_dict_data(self, testdata):
        for data in ("foo", 1, 1.0, True, None):
            json_format_validator({"test_key": data})
        for key in (0.0, (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                json_format_validator({key: testdata})

    @pytest.mark.skipif(not NUMPY, reason="test requires the numpy package")
    def test_numpy_data(self):
        data = numpy.random.rand(3, 4)
        json_format_validator(data)
        json_format_validator(numpy.float_(3.14))
        # numpy data as dict value
        json_format_validator({"test": data})
        json_format_validator({"test": numpy.float_(1.0)})
        # numpy data in list
        json_format_validator([data, numpy.float_(1.0), 1, "test"])

    def test_invalid_data(self):
        class A:
            pass

        invalid_data = (1.0 + 2.0j, A())
        for data in invalid_data:
            with pytest.raises(TypeError):
                json_format_validator(data)
        # invalid data as dict value
        for data in invalid_data:
            with pytest.raises(TypeError):
                json_format_validator({"test": data})
        # invalid data in tuple
        with pytest.raises(TypeError):
            json_format_validator(invalid_data)

    @pytest.mark.skipif(not NUMPY, reason="test requires the numpy package")
    def test_numpy_invalid_data(self):
        # complex data
        data = numpy.complex128(1 + 2j)
        with pytest.raises(TypeError):
            json_format_validator(data)
        # complex data in ndarray
        data = numpy.asarray([1, 2, 1j, 1 + 2j])
        with pytest.raises(TypeError):
            json_format_validator(data)
