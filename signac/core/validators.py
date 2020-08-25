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


class NoDotInKey:
    """Raises error for if there is dot in key"""

    VALID_KEY_TYPES = (str, int, bool, type(None))

    def __call__(self, data):
        """Raise an exception if data is invalid.

        Parameters
        ----------
        data:
            Data to validate.

        Returns
        -------
        data

        Raises
        ------
        KeyTypeError:
            If keys have unsupported data type.
        InvalidKeyError
            If key is not supported.
        """
        if isinstance(data, Mapping):
            for key in data.keys():
                if isinstance(key, str):
                    if '.' in key:
                        raise InvalidKeyError(
                            "Mapping keys may not contain dots ('.'): {}".format(key))
                elif not isinstance(key, self.VALID_KEY_TYPES):
                    raise KeyTypeError(
                        "Mapping keys must be str, int, bool or None, not {}"
                        .format(type(key).__name__))
        return data


class JSONFormatValidator:
    """Implement the validation for JSON serializable data."""

    def __call__(self, data):
        """Emit a warning or raise an exception if data is invalid.

        Parameters
        ----------
        data:
            Data to validate.

        Returns
        -------
        data

        Raises
        ------
        KeyTypeError:
            If keys of mapping have unsupported data type.
        TypeError
            If data type is not supported.
        """

        if isinstance(data, (str, int, float, bool, bytes, type(None))):
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
                new_value = self(value)
                ret[new_key] = new_value
            return ret
        elif isinstance(data, Sequence):
            for i in range(len(data)):
                data[i] = self(data[i])
            return data
        elif NUMPY:
            if isinstance(data, numpy.ndarray):
                data = data.tolist()
                return self(data)
            if isinstance(data, numpy.number):
                return data.item()
        raise TypeError("Object of {} is not JSON-serializable".format(type(data)))
