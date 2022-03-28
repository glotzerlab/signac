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
from .numpy_utils import (
    _is_atleast_1d_numpy_array,
    _is_complex,
    _is_numpy_scalar,
    _numpy_cache_blocklist,
)
from .utils import AbstractTypeResolver

_no_dot_in_key_type_resolver = AbstractTypeResolver(
    {
        "MAPPING": lambda obj: isinstance(obj, Mapping),
        "SEQUENCE": lambda obj: isinstance(obj, Sequence) and not isinstance(obj, str),
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
            # (issue #726) TODO: Make it an error to have a non-str key here in signac 2.0.
            elif not isinstance(key, VALID_KEY_TYPES):
                raise KeyTypeError(
                    f"Mapping keys must be str, int, bool or None, not {type(key).__name__}"
                )
            no_dot_in_key(value)
    elif switch_type == "SEQUENCE":
        for value in data:
            no_dot_in_key(value)


def require_string_key(data):
    """Raise an exception if key in a mapping is not a string.

    Almost all supported backends require string keys.

    Parameters
    ----------
    data
        Data to validate.

    Raises
    ------
    KeyTypeError
        If key type is not a string.

    """
    # Reuse the type resolver here since it has the same groupings.
    switch_type = _no_dot_in_key_type_resolver.get_type(data)

    if switch_type == "MAPPING":
        for key, value in data.items():
            if not isinstance(key, str):
                raise KeyTypeError(
                    f"Mapping keys must be str, not {type(key).__name__}"
                )
            require_string_key(value)
    elif switch_type == "NON_STR_SEQUENCE":
        for value in data:
            require_string_key(value)


_json_format_validator_type_resolver = AbstractTypeResolver(
    {
        # We identify >0d numpy arrays as sequences for validation purposes.
        "SEQUENCE": lambda obj: (isinstance(obj, Sequence) and not isinstance(obj, str))
        or _is_atleast_1d_numpy_array(obj),
        "NUMPY": lambda obj: _is_numpy_scalar(obj),
        "BASE": lambda obj: isinstance(obj, (str, int, float, bool, type(None))),
        "MAPPING": lambda obj: isinstance(obj, Mapping),
    },
    cache_blocklist=_numpy_cache_blocklist,
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
            if not isinstance(key, str):
                raise KeyTypeError(f"Keys must be str, not {type(key).__name__}")
            json_format_validator(value)
    elif switch_type == "SEQUENCE":
        for value in data:
            json_format_validator(value)
    elif switch_type == "NUMPY":
        if _is_numpy_scalar(data.item()):
            raise TypeError("NumPy extended precision types are not JSON serializable.")
        elif _is_complex(data):
            raise TypeError("Complex numbers are not JSON serializable.")
    else:
        raise TypeError(
            f"Object of type {type(data).__name__} is not JSON serializable"
        )
