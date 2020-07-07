# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from contextlib import contextmanager
from abc import abstractmethod
from abc import ABCMeta
from collections import defaultdict
from collections.abc import Collection

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


class SyncedCollectionABCMeta(ABCMeta):
    """ Metaclass for the definition of SyncedCollection.

    This metaclass automatically registers synced data structures' definition,
    this is used when recursively converting synced data structures to determine
    what to convert their children into.
    """
    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = defaultdict(list)
        else:
            if not cls.__abstractmethods__:
                cls.registry[cls.backend].append(cls)
        return super().__init__(name, bases, dct)


class SyncedCollection(Collection, metaclass=SyncedCollectionABCMeta):
    """The base synced collection represents a collection that is synced with a backend.

    The class is intended for use as an ABC. In addition, it declares abstract
    methods that must be implemented by any subclass.The SyncedCollection is a
    :class:`~collections.abc.Collection` where all data is stored persistently in
    the underlying backend.
    """
    backend = None

    def __init__(self, parent=None):
        self._data = None
        self._parent = parent
        self._suspend_sync_ = 0

    @classmethod
    def from_base(cls, data, backend=None, **kwargs):
        """This method dynamically resolves the type of object to the
        corresponding synced collection.

        Parameters
        ----------
        data : any
            Data to be converted from base class.
        backend: str
            Name of backend for synchronization. Default to backend of class.
        kwargs:
            Kwargs passed to instance of Synced Class.

        Returns
        -------
        data : object
            Synced object of corresponding base type.
        """
        backend = cls.backend if backend is None else backend
        if backend is None:
            raise ValueError("No backend found!!")
        for _cls in cls.registry[backend]:
            if _cls.is_base_type(data):
                return _cls(data=data, **kwargs)
        if NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
        return data

    @abstractmethod
    def to_base(self):
        "Dynamically resolve the synced collection to the corresponding base type."
        pass

    @contextmanager
    def _suspend_sync(self):
        """Prepares context where load and sync are suspended"""
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    @classmethod
    @abstractmethod
    def is_base_type(cls):
        """Check whether data is of same base type as Synced Class"""
        pass

    @abstractmethod
    def _load(self):
        """Loads data from file."""
        pass

    @abstractmethod
    def _sync(self):
        """Writes data to file."""
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

    # defining common methods
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
        return repr(self._data)

    def __str__(self):
        return str(self._data)
