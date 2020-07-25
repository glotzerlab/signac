# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the SyncedCollection class.

SyncedCollection encapsulates the synchronization of different data-structures.
These features are implemented in different subclasses which enable us to use a
backend with different data-structures or vice-versa. It declares as abstract
methods the methods that must be implemented by any subclass to match the API.
"""

from contextlib import contextmanager
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Collection

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


class SyncedCollection(Collection):
    """The base synced collection represents a collection that is synced with a backend.

    The class is intended for use as an ABC. The SyncedCollection is a
    :class:`~collections.abc.Collection` where all data is stored persistently
    in the underlying backend. The backend name wil be same as the module name.
    """

    backend = None

    def __init__(self, parent=None):
        self._data = None
        self._parent = parent
        self._suspend_sync_ = 0

    @classmethod
    def register(cls, *args):
        """Register the synced data structures.

        Registry is used when recursively converting synced data structures to determine
        what to convert their children into.

        Parameters
        ----------
        *args
            Classes to register
        """
        if not hasattr(cls, 'registry'):
            cls.registry = defaultdict(list)
        for _cls in args:
            cls.registry[_cls.backend].append(_cls)

    @classmethod
    def from_base(cls, data, backend=None, **kwargs):
        """Dynamically resolve the type of object to the corresponding synced collection.

        Parameters
        ----------
        data : any
            Data to be converted from base class.
        backend: str
            Name of backend for synchronization. Default to backend of class.
        **kwargs:
            Kwargs passed to instance of synced collection.

        Returns
        -------
        data : object
            Synced object of corresponding base type.
        """
        backend = cls.backend if backend is None else backend
        if backend is None:
            raise ValueError("No backend found.")
        for _cls in cls.registry[backend]:
            if _cls.is_base_type(data):
                return _cls(data=data, **kwargs)
        if NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
        return data

    @abstractmethod
    def to_base(self):
        """Dynamically resolve the synced collection to the corresponding base type."""
        pass

    @contextmanager
    def _suspend_sync(self):
        """Prepare context where load and sync are suspended."""
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    @classmethod
    @abstractmethod
    def is_base_type(cls, data):
        """Check whether data is of the same base type (such as list or dict) as this class."""
        pass

    @abstractmethod
    def _load(self):
        """Load data from underlying backend."""
        pass

    @abstractmethod
    def _sync(self):
        """Write data to underlying backend."""
        pass

    def sync(self):
        """Synchronize the data with the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._sync()
            else:
                self._parent.sync()

    def load(self):
        """Load the data from the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load()
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent.load()

    # methods having same implementaion for all data-structures
    def __getitem__(self, key):
        self.load()
        return self._data[key]

    def __delitem__(self, item):
        del self._data[item]
        self.sync()

    def __iter__(self):
        self.load()
        return iter(self._data)

    def __len__(self):
        self.load()
        return len(self._data)

    def __call__(self):
        self.load()
        return self.to_base()

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self() == other()
        else:
            return self() == other

    def __repr__(self):
        self.load()
        return repr(self._data)

    def __str__(self):
        self.load()
        return str(self._data)
