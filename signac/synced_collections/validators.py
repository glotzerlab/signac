# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Validators for SyncedCollection API.

A validator is any callable that raises Exceptions when called with invalid data.
Validators should act recursively for nested data structures and should not
return any values, only raise errors. This module implements built-in validators,
but client code is free to implement and add additioal validators to collection
types as needed.
"""

from collections.abc import Mapping, Sequence

from .errors import InvalidKeyError, KeyTypeError
from .utils import AbstractTypeResolver

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


_no_dot_in_key_type_resolver = AbstractTypeResolver(
    {
        "MAPPING": lambda obj: isinstance(obj, Mapping),
        "NON_STR_SEQUENCE": lambda obj: isinstance(obj, Sequence)
        and not isinstance(obj, str),
    }
)


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

    switch_type = _no_dot_in_key_type_resolver.get_type(data)

    if switch_type == "MAPPING":
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
    elif switch_type == "NON_STR_SEQUENCE":
        for value in data:
            no_dot_in_key(value)


_json_format_validator_type_resolver = AbstractTypeResolver(
    {
        "BASE": lambda obj: isinstance(obj, (str, int, float, bool, type(None))),
        "MAPPING": lambda obj: isinstance(obj, Mapping),
        "SEQUENCE": lambda obj: isinstance(obj, Sequence),
        "NUMPY": lambda obj: NUMPY and isinstance(obj, (numpy.ndarray, numpy.number)),
    }
)


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
    switch_type = _json_format_validator_type_resolver.get_type(data)

    if switch_type == "BASE":
        return
    elif switch_type == "MAPPING":
        for key, value in data.items():
            # Support for non-str keys will be removed in version 2.0.
            # See issue: https://github.com/glotzerlab/signac/issues/316.
            if not isinstance(key, (str, int, bool, type(None))):
                raise KeyTypeError(
                    f"Keys must be str, int, bool or None, not {type(key).__name__}"
                )
            json_format_validator(value)
    elif switch_type == "SEQUENCE":
        for value in data:
            json_format_validator(value)
    elif switch_type == "NUMPY":
        if numpy.iscomplex(data).any():
            raise TypeError(
                "NumPy object with complex value(s) is not JSON serializable"
            )
    else:
        raise TypeError(
            f"Object of type {type(data).__name__} is not JSON serializable"
        )
