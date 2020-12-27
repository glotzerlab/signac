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
from enum import Enum, auto

from ...errors import InvalidKeyError, KeyTypeError

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


"""Instance checks for abcs are expensive. Moreover, even for base classes isinstance
is slower than a dict lookup. Therefore, we can be much faster by only calling isinstance
when we have not seen a type before, then caching it; it's very unlikely that a user
will be using many _different_ data types in a given run."""


class AbstractTypes(Enum):
    """Simple representation of ABCs to avoid instance checks."""

    GENERIC = auto()
    MAPPING = auto()
    NON_STR_SEQUENCE = auto()


_TYPE_CACHE = {
    str: AbstractTypes.GENERIC,
    dict: AbstractTypes.MAPPING,
    list: AbstractTypes.NON_STR_SEQUENCE,
    tuple: AbstractTypes.NON_STR_SEQUENCE,
}


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

    dtype = type(data)
    try:
        switch_type = _TYPE_CACHE[dtype]
    except KeyError:
        if isinstance(data, Mapping):
            switch_type = _TYPE_CACHE[dtype] = AbstractTypes.MAPPING
        elif isinstance(data, Sequence) and not isinstance(data, str):
            switch_type = _TYPE_CACHE[dtype] = AbstractTypes.NON_STR_SEQUENCE
        else:
            switch_type = _TYPE_CACHE[dtype] = AbstractTypes.GENERIC

    if switch_type == AbstractTypes.MAPPING:
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
    elif switch_type == AbstractTypes.NON_STR_SEQUENCE:
        for value in data:
            no_dot_in_key(value)


class AbstractTypesJSON(Enum):
    """Simple representation of ABCs to avoid instance checks."""

    BASE = auto()
    MAPPING = auto()
    SEQUENCE = auto()
    NUMPY = auto()
    INVALID = auto()


_TYPE_CACHE_JSON = {
    str: AbstractTypesJSON.BASE,
    int: AbstractTypesJSON.BASE,
    float: AbstractTypesJSON.BASE,
    bool: AbstractTypesJSON.BASE,
    type(None): AbstractTypesJSON.BASE,
    dict: AbstractTypesJSON.MAPPING,
    list: AbstractTypesJSON.SEQUENCE,
    tuple: AbstractTypesJSON.SEQUENCE,
}


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
    dtype = type(data)

    try:
        switch_type = _TYPE_CACHE_JSON[dtype]
    except KeyError:
        if isinstance(data, (str, int, float, bool, type(None))):
            switch_type = _TYPE_CACHE_JSON[dtype] = AbstractTypesJSON.BASE
        if isinstance(data, Mapping):
            switch_type = _TYPE_CACHE_JSON[dtype] = AbstractTypesJSON.MAPPING
        elif isinstance(data, Sequence):
            switch_type = _TYPE_CACHE_JSON[dtype] = AbstractTypesJSON.SEQUENCE
        elif NUMPY and isinstance(data, (numpy.ndarray, numpy.number)):
            switch_type = _TYPE_CACHE_JSON[dtype] = AbstractTypesJSON.NUMPY
        else:
            switch_type = _TYPE_CACHE_JSON[dtype] = AbstractTypesJSON.INVALID

    if switch_type == AbstractTypesJSON.BASE:
        return
    elif switch_type == AbstractTypesJSON.MAPPING:
        for key, value in data.items():
            # Support for non-str keys will be removed in version 2.0.
            # See issue: https://github.com/glotzerlab/signac/issues/316.
            if not isinstance(key, (str, int, bool, type(None))):
                raise KeyTypeError(
                    f"Keys must be str, int, bool or None, not {type(key).__name__}"
                )
            json_format_validator(value)
    elif switch_type == AbstractTypesJSON.SEQUENCE:
        for value in data:
            json_format_validator(value)
    elif switch_type == AbstractTypesJSON.NUMPY:
        if numpy.iscomplex(data).any():
            raise TypeError(
                "NumPy object with complex value(s) is not JSON serializable"
            )
    else:
        raise TypeError(
            f"Object of type {type(data).__name__} is not JSON serializable"
        )
