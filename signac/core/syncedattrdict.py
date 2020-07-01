# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from collections.abc import Mapping
from collections.abc import MutableMapping

from .collection_api import SyncedCollection


class _SyncedDict(SyncedCollection, MutableMapping):
    """Implements the dict data structures"""

    base_type = 'mapping'

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
        """Converts the SyncedDict object to Dictionary.

        Returns:
        --------
        converted: dict
            Dictionary containing the converted synced dict object."""
        converted = {}
        for key, value in self._data.items():
            if isinstance(value, SyncedCollection):
                converted[key] = value.to_base()
            else:
                converted[key] = value
        return converted

    @classmethod
    def is_base_type(self, data):
        """Checks whether the data is of base type of SyncedDict
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

    def _dfs_update(self, data=None):
        """Updates the SyncedDict instance with data by using dfs."""
        if data is None:
            data = {}
        if isinstance(data, Mapping):
            with self._suspend_sync():
                for key in data:
                    if key in self._data:
                        if data[key] == self._data[key]:
                            continue
                        if isinstance(self._data[key], SyncedCollection):
                            try:
                                self._data[key]._dfs_update(data[key])
                                continue
                            except ValueError:
                                pass
                    self[key] = data[key]
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
        "Emit a warning or raise an exception if key is invalid. Returns key."
        if isinstance(key, _SyncedDict.VALID_KEY_TYPES):
            key = str(key)
            if '.' in key:
                from ..errors import InvalidKeyError
                raise InvalidKeyError(
                    "SyncedDict keys may not contain dots ('.'): {}".format(key))
            else:
                return key
        else:
            from ..errors import KeyTypeError
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
            Data to update the instance(Default value None).

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

    def popitem(self, key, default=None):
        self.load()
        ret = self._data.pop(key, default)
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


class SyncedAttrDict(_SyncedDict):
    """A synced dictionary where (nested) values can be accessed as attributes."""
    def __getattr__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if name.startswith('__'):
                raise
            try:
                return self.__getitem__(name)
            except KeyError as e:
                raise AttributeError(e)

    def __setattr__(self, key, value):
        try:
            super().__getattribute__('_data')
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
