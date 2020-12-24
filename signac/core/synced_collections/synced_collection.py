# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the SyncedCollection class.

SyncedCollection encapsulates the synchronization of different data-structures.
These features are implemented in different subclasses which enable us to use a
backend with different data-structures or vice-versa. It declares as abstract
methods the methods that must be implemented by any subclass to match the API.
"""
from inspect import isabstract
from typing import List, Callable, DefaultDict, Any
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

    _backend = None
    registry: DefaultDict[str, List[Any]] = defaultdict(list)
    _validators: List[Callable] = []

    def __init__(self, parent=None, *args, **kwargs):
        self._data = None
        self._parent = parent
        self._suspend_sync_ = 0

    @classmethod
    def __init_subclass__(cls):
        """Add  ``_validator`` attribute to every subclass.

        Subclasses contain a list of validators that are applied to collection input data.
        Every subclass must have a separate list so that distinct sets of validators can
        be registered to each of them.
        """
        # The Python data model promises that __init_subclass__ will be called
        # after the class namespace is fully defined, so at this point we know
        # whether we have a concrete subclass or not.
        if not isabstract(cls):
            # Add to the parent level registry
            SyncedCollection.registry[cls._backend].append(cls)
        cls._validators = []

    @property
    def validators(self):
        """Return the list of validators applied to the instance."""
        validators = []
        # Classes inherit the validators of their parent classes.
        for base_cls in type(self).__mro__:
            if hasattr(base_cls, "_validators"):
                validators.extend(
                    [v for v in base_cls._validators if v not in validators]
                )
        return validators

    @classmethod
    def add_validator(cls, *args):
        r"""Register validator.

        Parameters
        ----------
        \*args
            Validator(s) to register.
        """
        cls._validators.extend([v for v in args if v not in cls._validators])

    @classmethod
    def from_base(cls, data, backend=None, **kwargs):
        r"""Dynamically resolve the type of object to the corresponding synced collection.

        Parameters
        ----------
        data : any
            Data to be converted from base class.
        backend: str
            Name of backend for synchronization. Default to backend of class.
        \*\*kwargs:
            Kwargs passed to instance of synced collection.

        Returns
        -------
        data : object
            Synced object of corresponding base type.
        """
        backend = cls._backend if backend is None else backend
        if backend is None:
            raise ValueError("No backend found.")
        for base_cls in SyncedCollection.registry[backend]:
            if base_cls.is_base_type(data):
                return base_cls(data=data, **kwargs)
        if NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
        # TODO: This return value could be the original object if no match is
        # found, there should be an error or at least a warning.
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
    def _load_from_resource(self):
        """Load data from underlying backend."""
        pass

    @abstractmethod
    def _save_to_resource(self):
        """Write data to underlying backend."""
        pass

    def _save(self):
        """Synchronize the data with the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._save_to_resource()
            else:
                self._parent._save()

    @abstractmethod
    def _update(self, data):
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
        """
        pass

    def _load(self):
        """Load the data from the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load_from_resource()
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent._load()

    def _validate(self, data):
        """Validate the input data."""
        for validator in self.validators:
            validator(data)

    def _validate_constructor_args(self, resource_args, data, parent):
        """Validate the provided constructor arguments.

        In order to support nesting of SyncedCollections, every collection
        should either be associated with an underlying resource from which it
        acquires data or be nested within another SyncedCollection, in which
        case it contains its own data and points to a parent.
        """
        all_parent = all([arg is not None for arg in resource_args.values()])
        any_parent = any([arg is not None for arg in resource_args.values()])

        all_nested = (data is not None) and (parent is not None)
        any_nested = (data is not None) or (parent is not None)

        if not ((all_parent and not any_nested)
                or (all_nested and not any_parent)):
            raise ValueError(
                f"A {type(self)} must either be synchronized, in which case "
                f"the arguments ({', '.join(resource_args.keys())}) must be "
                "provided, or it must be nested within another collection, "
                "in which case the data and parent arguments must both be "
                "provided. The received arguments were " +
                ', '.join(f"{key}: {val}" for key, val in resource_args.items()) +
                f", data={data}, parent={parent}"
            )

    # The following methods share a common implementation for
    # all data structures and regardless of backend.

    def __getitem__(self, key):
        self._load()
        return self._data[key]

    def __delitem__(self, item):
        self._load()
        del self._data[item]
        self._save()

    def __iter__(self):
        self._load()
        return iter(self._data)

    def __len__(self):
        self._load()
        return len(self._data)

    def __call__(self):
        self._load()
        return self.to_base()

    def __eq__(self, other):
        self._load()
        if isinstance(other, type(self)):
            return self() == other()
        else:
            return self() == other

    def __repr__(self):
        self._load()
        return repr(self._data)

    def __str__(self):
        self._load()
        return str(self._data)
