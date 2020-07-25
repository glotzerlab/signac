# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements the SyncedAttrDict class.

This implements the dict data-structure for SyncedCollection API by
implementing the convert methods (`to_base`, `from_base`) for dictionaries.
This class also allows access to values through key indexing or attributes
named by keys, including nested keys.
"""

from collections.abc import Mapping
from collections.abc import MutableMapping

from .synced_collection import SyncedCollection
from ..errors import InvalidKeyError
from ..errors import KeyTypeError


class SyncedAttrDict(SyncedCollection, MutableMapping):
    """Implement the dict data structure along with values access through attributes named as keys.

    The SyncedAttrDict inherits from :class:`~core.collection_api.SyncedCollection`
    and :class:`~collections.abc.MutableMapping`. Therefore, it behaves similar to
    a :class:`dict`. This class also allows access to values through key indexing or
    attributes named by keys, including nested keys.

    .. warning::

        While the SyncedAttrDict object behaves like a dictionary, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying backend, copying (even deep
        copying) a SyncedAttrDict instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new SyncedAttrDict.
    """

    _PROTECTED_KEYS = ('_data', '_suspend_sync_', '_load', '_sync', '_parent')

    VALID_KEY_TYPES = (str, int, bool, type(None))

    def __init__(self, data=None, **kwargs):
        super().__init__(**kwargs)
        if data is None:
            self._data = {}
        else:
            with self._suspend_sync():
                self._data = {
                    self._validate_key(key): self.from_base(data=value, parent=self)
                    for key, value in data.items()
                }
            self.sync()

    def to_base(self):
        """Convert the SyncedDict object to Dictionary.

        Returns:
        --------
        converted: dict
            Dictionary containing the converted synced dict object.
        """
        converted = {}
        for key, value in self._data.items():
            if isinstance(value, SyncedCollection):
                converted[key] = value.to_base()
            else:
                converted[key] = value
        return converted

    @classmethod
    def is_base_type(cls, data):
        """Check whether the data is an instance of mapping.

        Parameters
        ----------
        data: any
            Data to be checked.

        Returns
        -------
        bool
        """
        if isinstance(data, Mapping):
            return True
        return False

    def _update(self, data=None):
        """Update the SyncedDict instance with data using depth-first traversal."""
        if data is None:
            data = {}
        if isinstance(data, Mapping):
            with self._suspend_sync():
                # This loop avoids rebuilding existing synced collections for performance.
                for key in data:
                    if key in self._data:
                        if data[key] == self._data[key]:
                            continue
                        if isinstance(self._data[key], SyncedCollection):
                            try:
                                self._data[key]._update(data[key])
                                continue
                            except ValueError:
                                pass
                    self._data[self._validate_key(key)] = self.from_base(data[key])
                remove = set()
                for key in self._data:
                    if key not in data:
                        remove.add(key)
                for key in remove:
                    del self._data[key]
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a mapping or None.".format(type(data)))

    @staticmethod
    def _validate_key(key):
        """Raise an exception if key is invalid. Returns key."""
        if isinstance(key, SyncedAttrDict.VALID_KEY_TYPES):
            key = str(key)
            if '.' in key:
                raise InvalidKeyError(
                    "SyncedDict keys may not contain dots ('.'): {}".format(key))
            else:
                return key
        else:
            raise KeyTypeError(
                "SyncedDict keys must be str, int, bool or None, not {}".format(type(key).__name__))

    def __setitem__(self, key, value):
        self.load()
        with self._suspend_sync():
            self._data[self._validate_key(key)] = self.from_base(data=value, parent=self)
        self.sync()

    def reset(self, data=None):
        """Update the instance with new data.

        Parameters
        ----------
        data: mapping
            Data to update the instance (Default value = None).

        Raises
        ------
        ValueError
            If the data is not instance of mapping
        """
        if data is None:
            data = {}

        if isinstance(data, Mapping):
            self.load()
            with self._suspend_sync():
                self._data = {
                    self._validate_key(key): self.from_base(data=value, parent=self)
                    for key, value in data.items()
                }
            self.sync()
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a mapping or None.".format(type(data)))

    def keys(self):
        self.load()
        return self._data.keys()

    def values(self):
        self.load()
        return self.to_base().values()

    def items(self):
        self.load()
        return self.to_base().items()

    def get(self, key, default=None):
        self.load()
        return self._data.get(key, default)

    def pop(self, key, default=None):
        self.load()
        ret = self._data.pop(key, default)
        self.sync()
        return ret

    def popitem(self):
        self.load()
        ret = self._data.popitem()
        self.sync()
        return ret

    def clear(self):
        self.load()
        self._data = {}
        self.sync()

    def update(self, mapping):
        self.load()
        with self._suspend_sync():
            for key, value in mapping.items():
                self._data[self._validate_key(key)] = self.from_base(data=value, parent=self)
        self.sync()

    def setdefault(self, key, default=None):
        self.load()
        with self._suspend_sync():
            ret = self._data.setdefault(self._validate_key(key),
                                        self.from_base(data=default, parent=self))
        self.sync()
        return ret

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError("'SyncedAttrDict' object has no attribute '{}'".format(name))
        try:
            return self.__getitem__(name)
        except KeyError as e:
            raise AttributeError(e)

    def __setattr__(self, key, value):
        try:
            self.__getattribute__('_data')
        except AttributeError:
            super().__setattr__(key, value)
        else:
            if key.startswith('__') or key in self._PROTECTED_KEYS:
                super().__setattr__(key, value)
            else:
                self.__setitem__(key, value)

    def __delattr__(self, key):
        if key.startswith('__') or key in self._PROTECTED_KEYS:
            super().__delattr__(key)
        else:
            self.__delitem__(key)
