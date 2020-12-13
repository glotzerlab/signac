# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements the SyncedList class.

This implements the list data structure for SyncedCollection API by
implementing the convert method `to_base` for lists.
"""

from collections.abc import Sequence
from collections.abc import MutableSequence

from .synced_collection import SyncedCollection
from .synced_collection import NUMPY

if NUMPY:
    import numpy


class SyncedList(SyncedCollection, MutableSequence):
    """Implementation of list data structure.

    The SyncedList inherits from :class:`~core.synced_collection.SyncedCollection`
    and :class:`~collections.abc.MutableSequence`. Therefore, it behaves similar
    to a :class:`list`.

    .. warning::

        While the SyncedList object behaves like a :class:`list`, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying backend, copying (even deep
        copying) a SyncedList instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        :class:`list` representation, and if necessary construct a new
        SyncedList.
    """

    def __init__(self, data=None, **kwargs):
        super().__init__(**kwargs)
        if data is None:
            self._data = []
        else:
            self._validate(data)
            if NUMPY and isinstance(data, numpy.ndarray):
                data = data.tolist()
            with self._suspend_sync():
                self._data = [self.from_base(data=value, parent=self) for value in data]
            self.sync()

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
        if isinstance(data, Sequence) and not isinstance(data, str):
            return True
        elif NUMPY:
            if isinstance(data, numpy.ndarray):
                return True
        return False

    def to_base(self):
        """Convert the SyncedList object to list.

        Returns
        -------
        converted: list
            List containing the conveted SyncedList object.
        """
        converted = list()
        for value in self._data:
            if isinstance(value, SyncedCollection):
                converted.append(value.to_base())
            else:
                converted.append(value)
        return converted

    def _update(self, data=None):
        """Update the instance of SyncedList with data using depth-first traversal."""
        if data is None:
            data = []
        self._validate(data)
        if isinstance(data, Sequence) and not isinstance(data, str):
            with self._suspend_sync():
                # This loop avoids rebuilding existing synced collections for performance.
                # TODO: Potential improvements to this code: Remove order constraints.
                for i in range(min(len(self), len(data))):
                    if data[i] == self._data[i]:
                        continue
                    if isinstance(self._data[i], SyncedCollection):
                        try:
                            self._data[i]._update(data[i])
                            continue
                        except ValueError:
                            pass
                    self._data[i] = self.from_base(data=data[i], parent=self)
                if len(self._data) > len(data):
                    self._data = self._data[:len(data)]
                else:
                    self.extend(data[len(self):])
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None."
                .format(type(data)))

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
        if isinstance(data, Sequence) and not isinstance(data, str):
            # TODO: Loading here conceptually shouldn't be necessary, but I
            # explicitly have a sanity check in the buffered syncs assuming
            # that a load always occurs before a sync so that items are in the
            # cache when sync occurs. Not loading here breaks that assumption,
            # and I'm not sure I want to get rid of that.
            self.load()
            with self._suspend_sync():
                self._data = [self.from_base(data=value, parent=self) for value in data]
            self.sync()
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None."
                .format(type(data)))

    def __setitem__(self, key, value):
        self._validate(value)
        self.load()
        with self._suspend_sync():
            self._data[key] = self.from_base(data=value, parent=self)
        self.sync()

    def __reversed__(self):
        self.load()
        return reversed(self._data)

    def __iadd__(self, iterable):
        # Convert input to a list so that iterators work as well as iterables.
        iterable_data = list(iterable)
        self._validate(iterable_data)
        self.load()
        with self._suspend_sync():
            self._data += [self.from_base(data=value, parent=self) for value in iterable_data]
        self.sync()
        return self

    def insert(self, index, item):
        self._validate(item)
        self.load()
        with self._suspend_sync():
            self._data.insert(index, self.from_base(data=item, parent=self))
        self.sync()

    def append(self, item):
        self._validate(item)
        self.load()
        with self._suspend_sync():
            self._data.append(self.from_base(data=item, parent=self))
        self.sync()

    def extend(self, iterable):
        # Convert iterable to a list to ensure generators are exhausted only once
        iterable_data = list(iterable)
        self._validate(iterable_data)
        self.load()
        with self._suspend_sync():
            self._data.extend([self.from_base(data=value, parent=self) for value in iterable_data])
        self.sync()

    def remove(self, item):
        self.load()
        with self._suspend_sync():
            self._data.remove(self.from_base(data=item, parent=self))
        self.sync()

    def clear(self):
        self._data = []
        self.sync()
