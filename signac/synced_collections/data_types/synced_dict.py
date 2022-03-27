# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements the :class:`SyncedDict`.

This implements a dict-like data structure that also conforms to the
:class:`~.SyncedCollection` API and can be combined with any backend type to
give a dict-like API to a synchronized data structure.
"""

from collections.abc import Mapping, MutableMapping

from ..utils import AbstractTypeResolver
from .synced_collection import SyncedCollection, _sc_resolver

# Identifies mappings, which are the base type for this class.
_mapping_resolver = AbstractTypeResolver(
    {
        "MAPPING": lambda obj: isinstance(obj, Mapping),
    }
)


class SyncedDict(SyncedCollection, MutableMapping):
    r"""Implement the dict data structure along with values access through attributes named as keys.

    The SyncedDict inherits from :class:`~.SyncedCollection`
    and :class:`~collections.abc.MutableMapping`. Therefore, it behaves like a
    :class:`dict`.

    Parameters
    ----------
    data : Mapping, optional
        The initial data to populate the dict. If ``None``, defaults to
        ``{}`` (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warning
    -------
    While the :class:`SyncedDict` object behaves like a :class:`dict`,
    there are important distinctions to remember. In particular, because
    operations are reflected as changes to an underlying backend, copying (even
    deep copying) a :class:`SyncedDict` instance may exhibit unexpected
    behavior. If a true copy is required, you should use the `_to_base()`
    method to get a :class:`dict` representation, and if necessary construct a
    new :class:`SyncedDict`.
    """

    # The _validate parameter is an optimization for internal use only. This
    # argument will be passed by _from_base whenever an unsynced collection is
    # being recursively converted, ensuring that validation only happens once.
    def __init__(self, data=None, _validate=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if data is None:
            self._data = {}
        else:
            if _validate:
                self._validate(data)
            with self._suspend_sync:
                self._data = {
                    key: self._from_base(data=value, parent=self)
                    for key, value in data.items()
                }

    def _to_base(self):
        """Convert the SyncedDict object to a :class:`dict`.

        Returns
        -------
        dict
            An equivalent raw :class:`dict`.

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
        data : any
            Data to be checked.

        Returns
        -------
        bool

        """
        return _mapping_resolver.get_type(data) == "MAPPING"

    def _update(self, data=None, _validate=False):
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
            The data to be assigned to this dict. If ``None``, the data is left
            unchanged (Default value = None).
        _validate : bool
            If True, the data will not be validated (Default value = False).

        """
        if data is None:
            # If no data is passed, take no action.
            pass
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
                        if not _validate:
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
                        if not _validate:
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
        # (issue # 728) TODO: Remove in signac 2.0, currently we're constructing a dict to
        # allow in-place modification by _convert_key_to_str, but validators
        # should not have side effects once that backwards compatibility layer
        # is removed, so we can validate a temporary dict {key: value} and
        # directly set using those rather than looping over data.

        data = {key: value}
        self._validate(data)
        with self._load_and_save, self._suspend_sync:
            for key, value in data.items():
                self._data[key] = self._from_base(value, parent=self)

    def reset(self, data):
        """Update the instance with new data.

        Parameters
        ----------
        data : mapping
            Data to update the instance.

        Raises
        ------
        ValueError
            If the data is not a mapping.

        """
        if _mapping_resolver.get_type(data) == "MAPPING":
            self._update(data)
            with self._thread_lock:
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
        with self._load_and_save:
            ret = self._data.pop(key, default)
        return ret

    def popitem(self):  # noqa: D102
        with self._load_and_save:
            ret = self._data.popitem()
        return ret

    def clear(self):  # noqa: D102
        self._data = {}
        with self._thread_lock:
            self._save()

    def update(self, other=None, **kwargs):  # noqa: D102
        if other is not None:
            # Convert sequence of key, value pairs to dict before validation
            if _mapping_resolver.get_type(other) != "MAPPING":
                other = dict(other)
        else:
            other = {}

        with self._load_and_save:
            # The order here is important to ensure that the promised sequence of
            # overrides is obeyed: kwargs > other > existing data.
            self._update({**self._data, **other, **kwargs})

    def setdefault(self, key, default=None):  # noqa: D102
        with self._load_and_save:
            if key in self._data:
                ret = self._data[key]
            else:
                ret = self._from_base(default, parent=self)
                # (issue #728) TODO: Remove in signac 2.0, currently we're constructing a dict
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
