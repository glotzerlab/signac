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
    - JSON encoding of numpy arrays is not invertible; once encoded, reloading
      the data will result in converting arrays to lists and numpy numbers into
      ints or floats.
    - This function assumes that the in-memory data for a SyncedCollection is
      up-to-date. If the data has been changed on disk without updating the
      collection, or if this function is used to serialize the data before any
      method is invoked that would load the data from disk, the resulting
      serialized data may be incorrect.

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
    - JSON encoding of numpy arrays is not invertible; once encoded, reloading
      the data will result in converting arrays to lists and numpy numbers into
      ints or floats.
    - This class assumes that the in-memory data for a SyncedCollection is
      up-to-date. If the data has been changed on disk without updating the
      collection, or if this class is used to serialize the data before any
      method of the collection is invoked that would load the data from disk,
      the resulting serialized data may be incorrect.

    """

    def default(self, o: Any) -> Dict[str, Any]:  # noqa: D102
        try:
            return default(o)
        except TypeError:
            # Call the super method, which raises a TypeError if it cannot
            # encode the object.
            return super().default(o)


class _NullContext:
    """A nullary context manager.

    There are various cases where we sometimes want to perform a task within a
    particular context, but at other times we wish to ignore that context. The
    most obvious example is a lock for threading: since
    :class:`SyncedCollection`s allow multithreading support to be enabled or
    disabled, it is important to be able to write code that is agnostic to
    whether or not a mutex must be acquired prior to executing a task. Locks
    support the context manager protocol and are used in that manner throughout
    the code base, so the most transparent way to disable buffering is to
    create a nullary context manager that can be placed as a drop-in
    replacement for the lock so that all other code can handle this in a
    transparent manner.
    """

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class _CounterContext:
    """A context manager that maintains a total entry count.

    This class simply contains an internal counter that is incremented on every
    entrance and decremented on every exit.
    """

    def __init__(self):
        self._count = 0

    def __enter__(self):
        self._count += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._count -= 1

    def __bool__(self):
        return self._count > 0
