# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements the SyncedAttrDict class.

This implements the dict data-structure for SyncedCollection API by
implementing the convert method `_to_base` for dictionaries.
This class also allows access to values through key indexing or attributes
named by keys, including nested keys.
"""

from collections.abc import Mapping, MutableMapping
from typing import Tuple

from .synced_collection import SyncedCollection
from .validators import no_dot_in_key


class SyncedAttrDict(SyncedCollection, MutableMapping):
    """Implement the dict data structure along with values access through attributes named as keys.

    The SyncedAttrDict inherits from :class:`~core.synced_collection.SyncedCollection`
    and :class:`~collections.abc.MutableMapping`. Therefore, it behaves similar to
    a :class:`dict`. This class also allows access to values through key indexing or
    attributes named by keys, including nested keys.

    .. warning::

        While the SyncedAttrDict object behaves like a :class:`dict`, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying backend, copying (even deep
        copying) a SyncedAttrDict instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `_to_base()` method to get a
        :class:`dict` representation, and if necessary construct a new SyncedAttrDict.
    """

    # Must specify this as a variable length tuple to allow subclasses to
    # extend the list of protected keys.
    _PROTECTED_KEYS: Tuple[str, ...] = (
        "_data",
        "_name",
        "_suspend_sync_",
        "_load",
        "_sync",
        "_parent",
        "_validators",
    )

    def __init__(self, data=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if data is None:
            self._data = {}
        else:
            self._validate(data)
            with self._suspend_sync():
                self._data = {
                    key: self._from_base(data=value, parent=self)
                    for key, value in data.items()
                }
            self._save()

    def _to_base(self):
        """Convert the SyncedDict object to Dictionary.

        Returns
        -------
        converted: dict
            Dictionary containing the converted synced dict object.

        """
        converted = {}
        for key, value in self._data.items():
            if isinstance(value, SyncedCollection):
                converted[key] = value._to_base()
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
            self._validate(data)
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
                    self._data[key] = self._from_base(data[key], parent=self)
                remove = set()
                for key in self._data:
                    if key not in data:
                        remove.add(key)
                for key in remove:
                    del self._data[key]
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a mapping or None.".format(
                    type(data)
                )
            )

    def __setitem__(self, key, value):
        self._validate({key: value})
        self._load()
        with self._suspend_sync():
            self._data[key] = self._from_base(value, parent=self)
        self._save()

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
            self._validate(data)
            with self._suspend_sync():
                self._data = {
                    key: self._from_base(data=value, parent=self)
                    for key, value in data.items()
                }
            self._save()
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a mapping or None.".format(
                    type(data)
                )
            )

    def keys(self):  # noqa: D102
        self._load()
        return self._data.keys()

    def values(self):  # noqa: D102
        self._load()
        return self._to_base().values()

    def items(self):  # noqa: D102
        self._load()
        return self._to_base().items()

    def get(self, key, default=None):  # noqa: D102
        self._load()
        return self._data.get(key, default)

    def pop(self, key, default=None):  # noqa: D102
        self._load()
        ret = self._data.pop(key, default)
        self._save()
        return ret

    def popitem(self):  # noqa: D102
        self._load()
        ret = self._data.popitem()
        self._save()
        return ret

    def clear(self):  # noqa: D102
        self._data = {}
        self._save()

    def update(self, other=None, **kwargs):  # noqa: D102
        if other is not None:
            # Convert sequence of key, value pairs to dict before validation
            if not isinstance(other, Mapping):
                other = dict(other)
            self._validate(other)
        if kwargs:
            self._validate(kwargs)
        self._load()
        with self._suspend_sync():
            if other:
                for key, value in other.items():
                    self._data[key] = self._from_base(data=value, parent=self)
            for key, value in kwargs.items():
                self._data[key] = self._from_base(data=value, parent=self)
        self._save()

    def setdefault(self, key, default=None):  # noqa: D102
        self._load()
        if key in self._data:
            ret = self._data[key]
        else:
            self._validate({key: default})
            ret = self._from_base(default, parent=self)
            with self._suspend_sync():
                self._data[key] = ret
            self._save()
        return ret

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(f"'SyncedAttrDict' object has no attribute '{name}'")
        try:
            return self.__getitem__(name)
        except KeyError as e:
            raise AttributeError(e)

    def __setattr__(self, key, value):
        try:
            self.__getattribute__("_data")
        except AttributeError:
            super().__setattr__(key, value)
        else:
            if key.startswith("__") or key in self._PROTECTED_KEYS:
                super().__setattr__(key, value)
            else:
                self.__setitem__(key, value)

    def __delattr__(self, key):
        if key.startswith("__") or key in self._PROTECTED_KEYS:
            super().__delattr__(key)
        else:
            self.__delitem__(key)


SyncedAttrDict.add_validator(no_dot_in_key)
