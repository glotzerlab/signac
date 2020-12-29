# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements the SyncedList class.

This implements the list data structure for SyncedCollection API by
implementing the convert method `_to_base` for lists.
"""

from collections.abc import MutableSequence, Sequence

from .synced_collection import NUMPY, SyncedCollection, _sc_resolver
from .utils import AbstractTypeResolver

if NUMPY:
    import numpy


# Identifies sequences, which are the base type for this class.
_sequence_resolver = AbstractTypeResolver(
    {
        "SEQUENCE": (
            lambda obj: isinstance(obj, Sequence)
            and not isinstance(obj, str)
            or (NUMPY and isinstance(obj, numpy.ndarray))
        ),
    }
)


class SyncedList(SyncedCollection, MutableSequence):
    """Implementation of list data structure.

    The SyncedList inherits from :class:`~core.synced_collection.SyncedCollection`
    and :class:`~collections.abc.MutableSequence`. Therefore, it behaves similar
    to a :class:`list`.

    Warnings
    --------
    While the SyncedList object behaves like a :class:`list`, there are
    important distinctions to remember. In particular, because operations
    are reflected as changes to an underlying backend, copying (even deep
    copying) a SyncedList instance may exhibit unexpected behavior. If a
    true copy is required, you should use the `_to_base()` method to get a
    :class:`list` representation, and if necessary construct a new
    SyncedList.

    """

    def __init__(self, data=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if data is None:
            self._data = []
        else:
            self._validate(data)
            if NUMPY and isinstance(data, numpy.ndarray):
                data = data.tolist()
            with self._suspend_sync():
                self._data = [
                    self._from_base(data=value, parent=self) for value in data
                ]
            self._save()

    @classmethod
    def is_base_type(cls, data):
        """Check whether the data is an non-string Sequence.

        Parameters
        ----------
        data: any
            Data to be checked

        Returns
        -------
        bool

        """
        return _sequence_resolver.get_type(data) == "SEQUENCE"

    def _to_base(self):
        """Convert the SyncedList object to list.

        Returns
        -------
        converted: list
            List containing the conveted SyncedList object.

        """
        converted = []
        for value in self._data:
            switch_type = _sc_resolver.get_type(value)
            if switch_type == "SYNCEDCOLLECTION":
                converted.append(value._to_base())
            else:
                converted.append(value)
        return converted

    def _update(self, data=None):
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
            The data to be assigned to this list.

        """
        if data is None:
            self._data.clear()
        elif _sequence_resolver.get_type(data) == "SEQUENCE":
            with self._suspend_sync():
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
                    self._validate(data[i])
                    self._data[i] = self._from_base(data[i], parent=self)

                if len(self._data) > len(data):
                    self._data = self._data[: len(data)]
                else:
                    new_data = data[len(self) :]
                    self._validate(new_data)
                    self.extend(new_data)
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None.".format(
                    type(data)
                )
            )

    def reset(self, data=None):
        """Update the instance with new data.

        Parameters
        ----------
        data: non-string Sequence, optional
            Data to update the instance (Default value = None).

        Raises
        ------
        ValueError
            If the data is not instance of non-string seqeuence

        """
        if data is None:
            data = []
        elif NUMPY and isinstance(data, numpy.ndarray):
            data = data.tolist()
        self._validate(data)
        if _sequence_resolver.get_type(data) == "SEQUENCE":
            with self._suspend_sync():
                self._data = [
                    self._from_base(data=value, parent=self) for value in data
                ]
            self._save()
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None.".format(
                    type(data)
                )
            )

    def __setitem__(self, key, value):
        self._validate(value)
        self._load()
        with self._suspend_sync():
            self._data[key] = self._from_base(data=value, parent=self)
        self._save()

    def __reversed__(self):
        self._load()
        return reversed(self._data)

    def __iadd__(self, iterable):
        # Convert input to a list so that iterators work as well as iterables.
        iterable_data = list(iterable)
        self._validate(iterable_data)
        self._load()
        with self._suspend_sync():
            self._data += [
                self._from_base(data=value, parent=self) for value in iterable_data
            ]
        self._save()
        return self

    def insert(self, index, item):  # noqa: D102
        self._validate(item)
        self._load()
        with self._suspend_sync():
            self._data.insert(index, self._from_base(data=item, parent=self))
        self._save()

    def append(self, item):  # noqa: D102
        self._validate(item)
        self._load()
        with self._suspend_sync():
            self._data.append(self._from_base(data=item, parent=self))
        self._save()

    def extend(self, iterable):  # noqa: D102
        # Convert iterable to a list to ensure generators are exhausted only once
        iterable_data = list(iterable)
        self._validate(iterable_data)
        self._load()
        with self._suspend_sync():
            self._data.extend(
                [self._from_base(data=value, parent=self) for value in iterable_data]
            )
        self._save()

    def remove(self, value):  # noqa: D102
        self._load()
        with self._suspend_sync():
            self._data.remove(self._from_base(data=value, parent=self))
        self._save()

    def clear(self):  # noqa: D102
        self._data = []
        self._save()
