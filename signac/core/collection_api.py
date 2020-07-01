# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from contextlib import contextmanager
from collections import defaultdict
from abc import abstractmethod
from abc import ABCMeta

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False

try:
    from collections.abc import Collection
except ImportError:
    # Collection does not exist in Python 3.5, only Python 3.6 or newer.

    from collections.abc import Sized, Iterable, Container

    def _check_methods(C, *methods):
        mro = C.__mro__
        for method in methods:
            for B in mro:
                if method in B.__dict__:
                    if B.__dict__[method] is None:
                        return NotImplemented
                    break
            else:
                return NotImplemented
        return True

    class Collection(Sized, Iterable, Container):  # type: ignore
        @classmethod
        def __subclasshook__(cls, C):
            if cls is Collection:
                return _check_methods(C,  "__len__", "__iter__", "__contains__")
            return NotImplemented


class CustomABCMeta(ABCMeta):
    """ Metaclass for the definition of SyncedCollection.

    This metaclass automatically registers Synced Classes definitions,
    which enables the automatic determination of Synced Class for a
    base type and backend.
    """
    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = defaultdict(list)
        else:
            if cls.base_type and cls.backend:
                cls.registry[cls.backend].append(cls)
        return super().__init__(name, bases, dct)


class SyncedCollection(Collection, metaclass=CustomABCMeta):
    """The base synced collection represents a collection that is synced with a
    file.
    The class is intended for use as an ABC.In addition, it declares abstract
    methods that must be implemented by any subclass.
    """
    base_type = None
    backend = None

    def __init__(self):
        self._data = None
        self._suspend_sync_ = 0

    @classmethod
    def from_base(self, data, **kwargs):
        """This method dynamically resolve the type of object to the
        corresponding synced collection.

        Parameters
        ----------
        data : any
            Data to be converted from base class.
        filename: str
            Name of file to store the data(Default value None).
        kwargs:
            Kwargs passed to instance of Synced Class.

        Returns
        -------
        data : object
            Synced object of corresponding base type.
        """
        for _cls in self.registry[self.backend]:
            if _cls.is_base_type(data):
                return _cls(data=data, **kwargs)
        if NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
        return data

    @abstractmethod
    def to_base(self):
        "Dynamically resolve the object synced collection to the corresponding base type."
        pass

    @contextmanager
    def _suspend_sync(self):
        """Prepares context where load and sync are ignored"""
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    @abstractmethod
    def is_base_type(self):
        """Check wether data is of same base type as Synced Class"""
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
        """If the parent is None, writes the data from the file,
        otherwise, calls the load of the parent."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._sync()
            else:
                self._parent.sync()

    def load(self):
        """If the parent is None, loads the data from the file and
        updates the instance, otherwise, calls the load of the parent."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load()
                with self._suspend_sync():
                    self._dfs_update(data)
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
