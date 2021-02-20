# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements the :class:`SyncedList`.

This implements a list-like data structure that also conforms to the
:class:`~.SyncedCollection` API and can be combined with any backend type to
give a list-like API to a synchronized data structure.
"""

from collections.abc import MutableSequence, Sequence

from ..numpy_utils import (
    _convert_numpy,
    _is_atleast_1d_numpy_array,
    _numpy_cache_blocklist,
)
from ..utils import AbstractTypeResolver
from .synced_collection import SyncedCollection, _sc_resolver

# Identifies sequences, which are the base type for this class.
_sequence_resolver = AbstractTypeResolver(
    {
        "SEQUENCE": (
            lambda obj: (isinstance(obj, Sequence) and not isinstance(obj, str))
            or _is_atleast_1d_numpy_array(obj)
        ),
    },
    cache_blocklist=_numpy_cache_blocklist,
)


class SyncedList(SyncedCollection, MutableSequence):
    r"""Implementation of list data structure.

    The SyncedList inherits from :class:`~synced_collection.SyncedCollection`
    and :class:`~collections.abc.MutableSequence`. Therefore, it behaves similar
    to a :class:`list`.

    Parameters
    ----------
    data : Sequence, optional
        The initial data to populate the list. If ``None``, defaults to
        ``[]`` (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`SyncedList` object behaves like a :class:`list`, there
    are important distinctions to remember. In particular, because operations
    are reflected as changes to an underlying backend, copying (even deep
    copying) a :class:`SyncedList` instance may exhibit unexpected behavior. If
    a true copy is required, you should use the `_to_base()` method to get a
    :class:`list` representation, and if necessary construct a new
    :class:`SyncedList`.

    """

    # The _validate parameter is an optimization for internal use only. This
    # argument will be passed by _from_base whenever an unsynced collection is
    # being recursively converted, ensuring that validation only happens once.
    def __init__(self, data=None, _validate=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if data is None:
            self._data = []
        else:
            if _validate:
                self._validate(data)
            data = _convert_numpy(data)
            with self._suspend_sync:
                self._data = [
                    self._from_base(data=value, parent=self) for value in data
                ]

    @classmethod
    def is_base_type(cls, data):
        """Check whether the data is an non-string Sequence.

        Parameters
        ----------
        data : Any
            Data to be checked

        Returns
        -------
        bool

        """
        return _sequence_resolver.get_type(data) == "SEQUENCE"

    def _to_base(self):
        """Convert the SyncedList object to a :class:`list`.

        Returns
        -------
        list
        An equivalent raw :class:`list`.

        """
        converted = []
        for value in self._data:
            switch_type = _sc_resolver.get_type(value)
            if switch_type == "SYNCEDCOLLECTION":
                converted.append(value._to_base())
            else:
                converted.append(value)
        return converted

    def _update(self, data=None, _validate=False):
        """Update the in-memory representation to match the provided data.

        The purpose of this method is to update the SyncedCollection to match
        the data in the underlying resource.  The result of calling this method
        should be that ``self == data``. The reason that this method is
        necessary is that SyncedCollections can be nested, and nested
        collections must also be instances of SyncedCollection so that
        synchronization occurs even when nested structures are modified.
        Recreating the full nested structure every time data is reloaded from
        file is highly inefficient, so this method performs an in-place update
        that only changes entries that need to be changed.

        Parameters
        ----------
        data : collections.abc.Sequence
            The data to be assigned to this list. If ``None``, the data is left
            unchanged (Default value = None).
        _validate : bool
            If True, the data will not be validated (Default value = False).

        """
        if data is None:
            # If no data is passed, take no action.
            pass
        elif _sequence_resolver.get_type(data) == "SEQUENCE":
            with self._suspend_sync:
                # This loop is optimized based on common usage patterns:
                # insertion and removal at the end of a list. Inserting or
                # removing in the middle will result in extra conversion
                # operations for all subsequent items. In the worst case,
                # inserting at the beginning will require reconverting all
                # elements of the data.
                for i in range(min(len(self), len(data))):
                    if data[i] == self._data[i]:
                        continue
                    if _sc_resolver.get_type(self._data[i]) == "SYNCEDCOLLECTION":
                        try:
                            self._data[i]._update(data[i])
                            continue
                        except ValueError:
                            pass
                    if not _validate:
                        self._validate(data[i])
                    self._data[i] = self._from_base(data[i], parent=self)

                if len(self._data) > len(data):
                    self._data = self._data[: len(data)]
                else:
                    new_data = data[len(self) :]
                    if not _validate:
                        self._validate(new_data)
                    self.extend(new_data)
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None.".format(
                    type(data)
                )
            )

    def reset(self, data):
        """Update the instance with new data.

        Parameters
        ----------
        data : non-string Sequence
            Data to update the instance.

        Raises
        ------
        ValueError
            If the data is not a non-string sequence.

        """
        data = _convert_numpy(data)
        if _sequence_resolver.get_type(data) == "SEQUENCE":
            self._update(data)
            with self._thread_lock:
                self._save()
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None.".format(
                    type(data)
                )
            )

    def __setitem__(self, key, value):
        self._validate(value)
        with self._load_and_save, self._suspend_sync:
            self._data[key] = self._from_base(data=value, parent=self)

    def __reversed__(self):
        self._load()
        return reversed(self._data)

    def __iadd__(self, iterable):
        # Convert input to a list so that iterators work as well as iterables.
        iterable_data = list(iterable)
        self._validate(iterable_data)
        with self._load_and_save, self._suspend_sync:
            self._data += [
                self._from_base(data=value, parent=self) for value in iterable_data
            ]
        return self

    def insert(self, index, item):  # noqa: D102
        self._validate(item)
        with self._load_and_save, self._suspend_sync:
            self._data.insert(index, self._from_base(data=item, parent=self))

    def append(self, item):  # noqa: D102
        self._validate(item)
        with self._load_and_save, self._suspend_sync:
            self._data.append(self._from_base(data=item, parent=self))

    def extend(self, iterable):  # noqa: D102
        # Convert iterable to a list to ensure generators are exhausted only once
        iterable_data = list(iterable)
        self._validate(iterable_data)
        with self._load_and_save, self._suspend_sync:
            self._data.extend(
                [self._from_base(data=value, parent=self) for value in iterable_data]
            )

    def remove(self, value):  # noqa: D102
        with self._load_and_save, self._suspend_sync:
            self._data.remove(self._from_base(data=value, parent=self))

    def clear(self):  # noqa: D102
        self._data = []
        with self._thread_lock:
            self._save()

    def __lt__(self, other):
        if isinstance(other, type(self)):
            return self() < other()
        else:
            return self() > other

    def __le__(self, other):
        if isinstance(other, type(self)):
            return self() <= other()
        else:
            return self() <= other

    def __gt__(self, other):
        if isinstance(other, type(self)):
            return self() > other()
        else:
            return self() > other

    def __ge__(self, other):
        if isinstance(other, type(self)):
            return self() >= other()
        else:
            return self() >= other
