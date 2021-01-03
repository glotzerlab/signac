# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define common utilities."""

from json import JSONEncoder
from typing import Any, Dict

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


class AbstractTypeResolver:
    """Mapping between recognized types and their abstract parents.

    Synced collections are heavily reliant on checking the types of objects to
    determine the appropriate type of behavior in various scenarios. For maximum
    generality, most of these checks use the ABCs defined in :py:mod:`collections.abc`.
    The price of this flexibility is that `isinstance` checks with these classes
    are very slow because the ``__instancecheck__`` hooks are implemented in pure
    Python and require checking many different cases.

    Rather than attempting to directly optimize this behavior, this class provides
    a workaround by which we can amortize the cost of type checks. Given a set
    of types that must be resolved and a way to identify each of these (which
    may be expensive), it maintains a local cache of all instances of a given
    type that have previously been observed. This reduces the cost of type checking
    to a simple dict lookup, except for the first time a new type is observed.

    Parameters
    ----------
    abstract_type_identifiers : collections.abc.Mapping
        A mapping from a string identifier for a group of types (e.g. ``MAPPING``)
        to a callable that can be used to identify that type. Due to insertion order
        guarantees of dictionaries in Python>=3.6 (officially 3.7), it is beneficial
        to order this dictionary with the most frequently occuring types first.

    Attributes
    ----------
    abstract_type_identifiers : Dict[str, Callable[Any, bool]]
        A mapping from string identifiers for an abstract type to callables that
        accepts an object and returns True if the object is of the key type and
        False if not.
    type_map : Dict[Type, str]
        A mapping from concrete types to the corresponding named abstract type
        from :attr:`type_enum`.

    """

    def __init__(self, abstract_type_identifiers):
        self.abstract_type_identifiers = abstract_type_identifiers
        self.type_map = {}

    def get_type(self, obj):
        """Get the type string corresponding to this data type.

        Parameters
        ----------
        obj : Any
            Any object whose type to check

        Returns
        -------
        str
            The name of the type, where valid types are the keys of the dict
            argument to the constructor. If the object's type cannot be identified,
            will return ``None``.

        """
        obj_type = type(obj)
        enum_type = None
        try:
            enum_type = self.type_map[obj_type]
        except KeyError:
            for data_type, id_func in self.abstract_type_identifiers.items():
                if id_func(obj):
                    enum_type = self.type_map[obj_type] = data_type
                    break
            self.type_map[obj_type] = enum_type

        return enum_type


def default(o: Any) -> Dict[str, Any]:  # noqa: D102
    """Get a JSON-serializable version of compatible types.

    This function is suitable for use with JSON-serialization tools as a way
    to serialize :class:`SyncedCollection` objects and NumPy arrays.

    Warnings
    --------
    JSON encoding of numpy arrays is not invertible; once encoded, reloading
    the data will result in converting arrays to lists and numpy numbers into
    ints or floats.

    """
    if NUMPY:
        if isinstance(o, numpy.number):
            return o.item()
        elif isinstance(o, numpy.ndarray):
            return o.tolist()
    try:
        return o._data
    except AttributeError as e:
        raise TypeError from e


class SCJSONEncoder(JSONEncoder):
    """A JSONEncoder capable of encoding SyncedCollections and other supported types.

    This encoder will attempt to obtain a JSON-serializable representation of
    an object that is otherwise not serializable by attempting to access its
    _data attribute. In addition, it supports direct writing of numpy arrays.

    Warnings
    --------
    JSON encoding of numpy arrays is not invertible; once encoded, reloading
    the data will result in converting arrays to lists and numpy numbers into
    ints or floats.

    """

    # TODO: If a user tries to access this encoder to manually dump and calls a
    # dump before any operation, the data won't have been initialized. This
    # isn't in itself important, since we'll make this private, but consider
    # whether there are any issues with that. I assume not, since we're
    # considering making these objects lazy altogether.
    def default(self, o: Any) -> Dict[str, Any]:  # noqa: D102
        try:
            return default(o)
        except TypeError:
            # Call the super method, which raises a TypeError if it cannot
            # encode the object.
            return super().default(o)
