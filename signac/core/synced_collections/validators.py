# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Validators for SyncedCollection API.

A validator is any callable that raises Exceptions when called with invalid data.
Validators should act recursively for nested data structures and should not
return any values, only raise errors.
"""

from collections.abc import Mapping, Sequence

from ...errors import InvalidKeyError, KeyTypeError

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


def no_dot_in_key(data):
    """Raise an exception if there is a dot (``.``) in a mapping's key.

    Parameters
    ----------
    data
        Data to validate.

    Raises
    ------
    KeyTypeError
        If key data type is not supported.
    InvalidKeyError
        If the key contains invalid characters or is otherwise malformed.
    """
    VALID_KEY_TYPES = (str, int, bool, type(None))

    if isinstance(data, Mapping):
        for key, value in data.items():
            if isinstance(key, str):
                if "." in key:
                    raise InvalidKeyError(
                        f"Mapping keys may not contain dots ('.'): {key}"
                    )
            elif not isinstance(key, VALID_KEY_TYPES):
                raise KeyTypeError(
                    f"Mapping keys must be str, int, bool or None, not {type(key).__name__}"
                )
            no_dot_in_key(value)
    elif isinstance(data, Sequence) and not isinstance(data, str):
        for value in data:
            no_dot_in_key(value)


def json_format_validator(data):
    """Validate input data can be serialized to JSON.

    Parameters
    ----------
    data
        Data to validate.

    Raises
    ------
    KeyTypeError
        If key data type is not supported.
    TypeError
        If the data type of ``data`` is not supported.
    """

    if isinstance(data, (str, int, float, bool, type(None))):
        return
    elif isinstance(data, Mapping):
        for key, value in data.items():
            # Support for non-str keys will be removed in version 2.0.
            # See issue: https://github.com/glotzerlab/signac/issues/316.
            if not isinstance(key, (str, int, bool, type(None))):
                raise KeyTypeError(
                    f"Keys must be str, int, bool or None, not {type(key).__name__}"
                )
            json_format_validator(value)
    elif isinstance(data, Sequence):
        for value in data:
            json_format_validator(value)
    elif NUMPY and isinstance(data, (numpy.ndarray, numpy.number)):
        if numpy.iscomplex(data).any():
            raise TypeError(
                "NumPy object with complex value(s) is not JSON serializable"
            )
    else:
        raise TypeError(
            f"Object of type {type(data).__name__} is not JSON serializable"
        )
