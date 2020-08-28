# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the SyncedCollection class.

SyncedCollection encapsulates the synchronization of different data-structures.
These features are implemented in different subclasses which enable us to use a
backend with different data-structures or vice-versa. It declares as abstract
methods the methods that must be implemented by any subclass to match the API.
"""
import inspect
from contextlib import contextmanager
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Collection
from typing import List, DefaultDict, Any

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
    registry: DefaultDict[str, List[Any]] = defaultdict(list)
    backend_registry: List[Any] = []

    def __init__(self, name=None, parent=None):
        self._data = None
        self._parent = parent
        self._name = name
        self._suspend_sync_ = 0
        if (name is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or name must be None, but not both.")

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
        for _cls in args:
            if not inspect.isabstract(_cls):
                cls.registry[_cls.backend].append(_cls)
            elif _cls.backend and _cls.backend not in cls.backend_registry:
                cls.backend_registry.append(_cls)

    @classmethod
    def from_base(cls, data, backend=None, **kwargs):
        """Dynamically resolve the type of object to the corresponding synced collection.

        Parameters
        ----------
        data : any
            Data to be converted from base class.
        backend: str
            Name of backend for synchronization. Default to backend of class.
        **kwargs
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

    @classmethod
    def from_backend(cls, backend_name):
        """Return backend class corresponding to backend name.

        Parameters
        ----------
        backend_name: str
            Name of the backend.

        Returns
        -------
        _cls
            Class corresponding to name.

        Raises:
        -------
        ValueError
        """
        for _cls in cls.backend_registry:
            if _cls.backend == backend_name:
                return _cls
        raise ValueError(f"{backend_name} backend not found.")

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
    def _sync(self, data):
        """Write data to underlying backend."""
        pass

    def _sync_to_backend(self):
        self._sync()

    def _load_from_backend(self):
        return self._load()

    def sync(self):
        """Synchronize the data with the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._sync_to_backend()
            else:
                self._parent.sync()

    def load(self):
        """Load the data from the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load_from_backend()
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent.load()

    # The following methods share a common implementation for
    # all data structures and regardless of backend.

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
