# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from signac.core.validators import NoDotInKey
from signac.core.validators import JSONFormatValidator
from signac.errors import KeyTypeError
from signac.errors import InvalidKeyError

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


class TestNoDotInKey:

    def test_valid_data(self, testdata):
        test_dict = {}
        # valid data
        for key in ('valid_str', 1, False, None):
            test_dict[key] = testdata
            NoDotInKey(test_dict)  # should not raise any error.
            assert key in test_dict
            assert test_dict[key] == testdata

    def test_invalid_data(self, testdata):
        # dict containig dot
        with pytest.raises(InvalidKeyError):
            NoDotInKey({'a.b': testdata})
        # invalid key types
        for key in (0.0, 1.0 + 2.0j, (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                NoDotInKey({key: testdata})


class TestJSONFormatValidator:

    def test_valid_data(self):
        for data in ('foo', 1, 1.0, True, None, {}, []):
            JSONFormatValidator(data)  # should not raise any error
        for data in ('foo', 1, 1.0, True, None, {}, []):
            JSONFormatValidator({'test_key': data})  # should not raise any error
        JSONFormatValidator(('foo', 1, 1.0, True, None, {}, []))  # should not raise any error

    def test_dict_data(self, testdata):
        for data in ('foo', 1, 1.0, True, None):
            JSONFormatValidator({'test_key': data})  # should not raise any error
        for key in (1, True, None):
            with pytest.deprecated_call(match="Use of.+as key is deprecated"):
                JSONFormatValidator({key: 'test_data'})
        for key in (0.0, (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                JSONFormatValidator({key: testdata})

    @pytest.mark.skipif(not NUMPY, reason='test requires the numpy package')
    def test_numpy_data(self):
        data = numpy.random.rand(3, 4)
        JSONFormatValidator(data)  # should not raise any error
        JSONFormatValidator(numpy.float_(3.14))  # should not raise any error
        # numpy data as dict value
        JSONFormatValidator({'test': data})
        JSONFormatValidator({'test': numpy.float_(1.0)})
        # numpy data in list
        JSONFormatValidator([data, numpy.float_(1.0), 1, 'test'])

    def test_invalid_data(self):

        class A:
            pass
        invalid_data = (1.0 + 2.0j, A())
        for data in invalid_data:
            with pytest.raises(TypeError):
                JSONFormatValidator(data)
        # invalid data as dict value
        for data in invalid_data:
            with pytest.raises(TypeError):
                JSONFormatValidator({'test': data})
        # invalid data in tuple
        with pytest.raises(TypeError):
            JSONFormatValidator(invalid_data)
