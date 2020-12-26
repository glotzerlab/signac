# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the SyncedCollection class."""
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Collection
from contextlib import contextmanager
from inspect import isabstract
from typing import Any, Callable, DefaultDict, List

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


class SyncedCollection(Collection):
    """An abstract :class:`Collection` type that is synced with a backend.

    This class extends :py:class:`collections.abc.Collection` and adds a number of abstract
    internal methods that must be implemented by its subclasses. These methods can be
    split into two groups of functions that are designed to be implemented by
    separate subtrees in the inheritance hierarchy that can then be composed:

        **Concrete Collection Types**

        These subclasses should implement the APIs for specific types of
        collections. For instance, a list-like :class:`SyncedCollection`
        should implement the standard methods for sequences. In addition, they
        must implement the following abstract methods defined by the
        :class:`SyncedCollection`:

        - :meth:`~.is_base_type`: Determines whether an object satisfies the
          semantics of the collection object a given :class:`SyncedCollection`
          is designed to mimic.
        - :meth:`~._to_base`: Converts a :class:`SyncedCollection` to its
          natural base type (e.g. a `list`).
        - :meth:`~._update`: Updates the :class:`SyncedCollection` to match the
          contents of the provided :py:class:`collections.abc.Collection`.
          After calling ``sc._update(c)``, we must have that ``sc == c``; however,
          since such updates are frequent when loading and saving data to a
          resource, :meth:`_update` should be implemented to minimize new object
          creation wherever possible.

        **Backend**

        These subclasses encode the process by which in-memory data is
        converted into a representation suitable for a particular backend. For
        instance, a JSON backend should know how to save a Python object into a
        JSON-encoded file and then read that object back.

        - :meth:`~._load_from_resource`: Loads data from the underlying
          resource and returns it in an object satisfying :meth:`~.is_base_type`.
        - :meth:`~._save_to_resource`: Stores data to the underlying resource.
        - :attr:`~._backend`: A unique string identifier for the resource backend.

    Since these functionalities are effectively completely orthogonal, members of
    a given group should be interchangeable. For instance, a dict-like SyncedCollection
    can be combined equally easily with JSON, MongoDB, or SQL backends.

    To fully support the restrictions and requirements of particular backends,
    arbitrary validators may be added to different subclasses. Validators are
    callables that accept different data types as input and raise Exceptions if the
    data does not conform to the requirements of a particular backend. For
    example, a JSON validator would raise Exceptions if it detected non-string
    keys in a dict.

    Parameters
    ----------
    parent : SyncedCollection or None
        If provided, the collection within which this collection is nested
        (Default value = None).
    """

    registry: DefaultDict[str, List[Any]] = defaultdict(list)
    _validators: List[Callable] = []

    def __init__(self, parent=None, *args, **kwargs):
        self._parent = parent
        self._suspend_sync_ = 0

    @classmethod
    def __init_subclass__(cls):
        """Registers and enables validation in subclasses.

        All subclasses are given a ``_validators`` list so that separate sets of
        validators can be registered to different types of synced collections. Concrete
        subclasses (those that have all methods implemented, i.e. that are associated
        with both a specific backend and a concrete data type) are also recorded in
        an internal registry that is used to convert data from some collection-like
        object into a :class:`SyncedCollection`.
        """
        # The Python data model promises that __init_subclass__ will be called
        # after the class namespace is fully defined, so at this point we know
        # whether we have a concrete subclass or not.
        if not isabstract(cls):
            SyncedCollection.registry[cls._backend].append(cls)
        cls._validators = []

    @property
    def validators(self):
        """List[Callable]: The validators that will be applied.

        Validators are inherited from all parents of a class.
        """
        # TODO: Determine whether it makes sense to construct this list here,
        # or whether we can just do it at initialization and cache it. The only
        # reason not to do that would be to support adding validators to a
        # class after instantiating objects and still having those validators
        # applied, which I don't think is necessary.
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
        r"""Register a validator to this class.

        Parameters
        ----------
        \*args : List[Callable]
            Validator(s) to register.
        """
        cls._validators.extend([v for v in args if v not in cls._validators])

    @property
    @abstractmethod
    def _backend(self):
        """str: The backend associated with a given collection.

        This property is abstract to enforce that subclasses implement it.
        Since it's only internal, subclasses can safely override it with just a
        raw attribute; this property just serves as a way to enforce the
        abstract API for subclasses.
        """
        pass

    @classmethod
    def _from_base(cls, data, **kwargs):
        r"""Dynamically resolve the type of object to the corresponding synced collection.

        Parameters
        ----------
        data : Collection
            Data to be converted from base type.
        \*\*kwargs:
            Any keyword arguments to pass to the collection constructor.

        Returns
        -------
        Collection
            Synced object of corresponding base type.

        Notes
        -----
        This method relies on the internal registry of subclasses populated by
        :meth:`~.__init_subclass__` and the :meth:`is_base_type` method to
        determine the subclass with the appropriate backend and data type. Once
        an appropriate type is determined, that class's constructor is called.
        Since this method relies on the constructor and other methods, it can
        be concretely implemented here rather than requiring subclass
        implementations.
        """
        for base_cls in SyncedCollection.registry[cls._backend]:
            if base_cls.is_base_type(data):
                return base_cls(data=data, **kwargs)
        if NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
        # TODO: This return value could be the original object if no match is
        # found, there should be an error or at least a warning.
        return data

    @abstractmethod
    def _to_base(self):
        """Dynamically resolve the synced collection to the corresponding base type.

        This method should not load the data from the underlying resource, it
        should simply converts the current in-memory representation of a synced
        collection to its naturally corresponding unsynced collection type.

        Returns
        -------
        Collection
            An equivalent unsynced collection satisfying :meth:`is_base_type`.
        """
        pass

    @contextmanager
    def _suspend_sync(self):
        """Prepare context where synchronization is suspended."""
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    @classmethod
    @abstractmethod
    def is_base_type(cls, data):
        """Check whether data is of the same base type (such as list or dict) as this class.

        Parameters
        ----------
        data : Any
            The input data to test.

        Returns
        -------
        bool
            Whether or not the object can be converted into this synced collection type.
        """
        pass

    @abstractmethod
    def _load_from_resource(self):
        """Load data from underlying backend.

        This method must be implemented for each backend.

        Returns
        -------
        Collection
            An equivalent unsynced collection satisfying :meth:`is_base_type` that
            contains the data in the underlying resource (e.g. a file).
        """
        pass

    @abstractmethod
    def _save_to_resource(self):
        """Save data to the backend.

        This method must be implemented for each backend.
        """
        pass

    def _save(self):
        """Save the data to the backend.

        This method encodes the recursive logic required to handle the saving of
        nested collections. For a collection contained within another collection,
        only the parent is ever responsible for storing the data. This method
        handles the appropriate recursive calls, then farms out the actual writing
        to the abstract method :meth:`~._save_to_resource`.
        """
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

        Parameters
        ----------
        data : Collection
            An collection satisfying :meth:`is_base_type`.
        """
        pass

    def _load(self):
        """Load the data from the backend.

        This method encodes the recursive logic required to handle the loadingof
        nested collections. For a collection contained within another collection,
        only the parent is ever responsible for loading the data. This method
        handles the appropriate recursive calls, then farms out the actual reading
        to the abstract method :meth:`~._load_from_resource`.
        """
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load_from_resource()
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent._load()

    def _validate(self, data):
        """Validate the input data.

        Parameters
        ----------
        data : Collection
            An collection satisfying :meth:`is_base_type`.
        """
        for validator in self.validators:
            validator(data)

    def _validate_constructor_args(self, resource_args, data, parent):
        """Validate the provided constructor arguments.

        In order to support nesting of SyncedCollections, every collection
        should either be associated with an underlying resource from which it
        acquires data or be nested within another SyncedCollection, in which
        case it contains its own data and points to a parent. Based on these
        considerations, only certain combinations of constructor arguments are
        valid. This method serves to validate those inputs.

        Parameters
        ----------
        resource_args : dict
            A dictionary of the keyword arguments that will be passed to the
            backend constructor.
        data : Collection or None
            If provided, the data to be associated with this collection
            (Default value = None).
        parent : SyncedCollection or None
            If provided, the collection within which this collection is nested
            (Default value = None).
        """
        all_parent = all([arg is not None for arg in resource_args.values()])
        any_parent = any([arg is not None for arg in resource_args.values()])

        all_nested = (data is not None) and (parent is not None)
        any_nested = (data is not None) or (parent is not None)

        if not ((all_parent and not any_nested) or (all_nested and not any_parent)):
            raise ValueError(
                f"A {type(self)} must either be synchronized, in which case "
                f"the arguments ({', '.join(resource_args.keys())}) must be "
                "provided, or it must be nested within another collection, "
                "in which case the data and parent arguments must both be "
                "provided. The received arguments were "
                + ", ".join(f"{key}: {val}" for key, val in resource_args.items())
                + f", data={data}, parent={parent}"
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
        return self._to_base()

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
