# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Validators for SyncedCollection API."""
import warnings
from collections.abc import Mapping
from collections.abc import Sequence

from ..errors import InvalidKeyError
from ..errors import KeyTypeError

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


def NoDotInKey(data):
    """Raise an exception if there is a dot (``.``) in a mapping's key.

    Parameters
    ----------
    data
        Data to validate.

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
                        f"Mapping keys may not contain dots ('.'): {key}")
            elif not isinstance(key, VALID_KEY_TYPES):
                raise KeyTypeError(
                    f"Mapping keys must be str, int, bool or None, not {type(key).__name__}")


def JSONFormatValidator(data):
    """Implement the validation for JSON serializable data.

    Parameters
    ----------
    data
        Data to validate.

    Raises
    ------
    KeyTypeError
        If keys of mapping have unsupported data type.
    TypeError
        If data type is not supported.
    """

    if isinstance(data, (str, int, float, bool, type(None))):
        return
    if isinstance(data, Mapping):
        for key, value in data.items():
            if isinstance(key, (int, bool, type(None))):
                warnings.warn(
                    f"Use of {type(key).__name__} as key is deprecated "
                    "and will be removed in version 2.0",
                    DeprecationWarning)
            elif not isinstance(key, str):
                raise KeyTypeError(
                    f"Keys must be str, int, bool or None, not {type(key).__name__}")
            JSONFormatValidator(value)
        return
    if isinstance(data, Sequence):
        for i in range(len(data)):
            JSONFormatValidator(data[i])
        return
    if NUMPY and isinstance(data, (numpy.ndarray, numpy.number)):
        return
    raise TypeError(f"Object of {type(data).__name__} type is not JSON-serializable")
