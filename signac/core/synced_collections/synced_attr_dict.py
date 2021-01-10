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

from .synced_collection import SyncedCollection, _sc_resolver
from .utils import AbstractTypeResolver
from .validators import no_dot_in_key

# Identifies mappings, which are the base type for this class.
_mapping_resolver = AbstractTypeResolver(
    {
        "MAPPING": lambda obj: isinstance(obj, Mapping),
    }
)


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
            with self._suspend_sync:
                self._data = {
                    key: self._from_base(data=value, parent=self)
                    for key, value in data.items()
                }
            with self._thread_lock():
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
            switch_type = _sc_resolver.get_type(value)
            if switch_type == "SYNCEDCOLLECTION":
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
        return _mapping_resolver.get_type(data) == "MAPPING"

    def _update(self, data=None):
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
        data : collections.abc.Mapping
            The data to be assigned to this dict.

        """
        if data is None:
            self._data.clear()
        elif _mapping_resolver.get_type(data) == "MAPPING":
            with self._suspend_sync:
                for key, new_value in data.items():
                    try:
                        # The most common usage of SyncedCollections is with a
                        # single object referencing an underlying resource at a
                        # time, so we should almost always find that elements
                        # of data are already contained in self._data, so EAFP
                        # is the best choice for performance.
                        existing = self._data[key]
                    except KeyError:
                        # If the item wasn't present at all, we can simply
                        # assign it.
                        self._validate({key: new_value})
                        self._data[key] = self._from_base(new_value, parent=self)
                    else:
                        if new_value == existing:
                            continue
                        if _sc_resolver.get_type(existing) == "SYNCEDCOLLECTION":
                            try:
                                existing._update(new_value)
                                continue
                            except ValueError:
                                pass

                        # Fall through if the new value is not identical to the
                        # existing value and
                        #     1) The existing value is not a SyncedCollection
                        #        (in which case we would have tried to update it), OR
                        #     2) The existing value is a SyncedCollection, but
                        #       the new value is not a compatible type for _update.
                        self._validate({key: new_value})
                        self._data[key] = self._from_base(new_value, parent=self)

                to_remove = [key for key in self._data if key not in data]
                for key in to_remove:
                    del self._data[key]
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a mapping or None.".format(
                    type(data)
                )
            )

    def __setitem__(self, key, value):
        # TODO: Remove in signac 2.0, currently we're constructing a dict to
        # allow in-place modification by _convert_key_to_str, but validators
        # should not have side effects once that backwards compatibility layer
        # is removed, so we can validate a temporary dict {key: value} and
        # directly set using those rather than looping over data.
        data = {key: value}
        self._validate(data)
        with self._load_and_save():
            with self._suspend_sync:
                for key, value in data.items():
                    self._data[key] = self._from_base(value, parent=self)

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
        if _mapping_resolver.get_type(data) == "MAPPING":
            self._validate(data)
            with self._suspend_sync:
                self._data = {
                    key: self._from_base(data=value, parent=self)
                    for key, value in data.items()
                }
            with self._thread_lock():
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
        with self._load_and_save():
            ret = self._data.pop(key, default)
        return ret

    def popitem(self):  # noqa: D102
        with self._load_and_save():
            ret = self._data.popitem()
        return ret

    def clear(self):  # noqa: D102
        self._data = {}
        with self._thread_lock():
            self._save()

    def update(self, other=None, **kwargs):  # noqa: D102
        if other is not None:
            # Convert sequence of key, value pairs to dict before validation
            if _mapping_resolver.get_type(other) != "MAPPING":
                other = dict(other)
        else:
            other = {}

        with self._load_and_save():
            # The order here is important to ensure that the promised sequence of
            # overrides is obeyed: kwargs > other > existing data.
            self._update({**self._data, **other, **kwargs})

    def setdefault(self, key, default=None):  # noqa: D102
        with self._load_and_save():
            if key in self._data:
                ret = self._data[key]
            else:
                ret = self._from_base(default, parent=self)
                # TODO: Remove in signac 2.0, currently we're constructing a dict
                # to allow in-place modification by _convert_key_to_str, but
                # validators should not have side effects once that backwards
                # compatibility layer is removed, so we can validate a temporary
                # dict {key: value} and directly set using those rather than
                # looping over data.
                data = {key: ret}
                self._validate(data)
                with self._suspend_sync:
                    for key, value in data.items():
                        self._data[key] = value
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
