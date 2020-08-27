# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Validators for SyncedCollection API."""
import warnings
from collections.abc import Mapping
from collections.abc import Sequence

from ..errors import InvalidKeyError
from ..errors import KeyTypeError
from .synced_collection import NUMPY

if NUMPY:
    import numpy


def NoDotInKey(data):
    """Raise an exception if there is dot in key.

    Parameters
    ----------
    data
        Data to validate.

    Returns
    -------
    Validated data.

    Raises
    ------
    KeyTypeError
        If keys have unsupported data type.
    InvalidKeyError
        If key is not supported.
    """
    VALID_KEY_TYPES = (str, int, bool, type(None))

    if isinstance(data, Mapping):
        for key in data.keys():
            if isinstance(key, str):
                if '.' in key:
                    raise InvalidKeyError(
                        "Mapping keys may not contain dots ('.'): {}".format(key))
            elif not isinstance(key, VALID_KEY_TYPES):
                raise KeyTypeError(
                    "Mapping keys must be str, int, bool or None, not {}"
                    .format(type(key).__name__))
    return data


def JSONFormatValidator(data):
    """Implement the validation for JSON serializable data

    Parameters
    ----------
    data
        Data to validate.

    Returns
    -------
    Validated data.

    Raises
    ------
    KeyTypeError
        If keys of mapping have unsupported data type.
    TypeError
        If data type is not supported.
    """

    if isinstance(data, (str, int, float, bool, type(None))):
        return data
    elif isinstance(data, Mapping):
        ret = {}
        for key, value in data.items():
            if isinstance(key, (int, bool, type(None))):
                warnings.warn(
                    "Use of {} as key is deprecated and will be removed in version 2.0"
                    .format(type(key)), DeprecationWarning)
                new_key = str(key)
            elif isinstance(key, str):
                new_key = key
            else:
                raise KeyTypeError(
                    "Keys must be str, int, bool or None, not {}".format(type(key).__name__))
            new_value = JSONFormatValidator(value)
            ret[new_key] = new_value
        return ret
    elif isinstance(data, Sequence):
        for i in range(len(data)):
            data[i] = JSONFormatValidator(data[i])
        return data
    elif NUMPY:
        if isinstance(data, (numpy.ndarray, numpy.number)):
            return data
    raise TypeError("Object of {} is not JSON-serializable".format(type(data)))
