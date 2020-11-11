# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Defines attribute dictionaries, where values can be accessed as attributes."""

from .synceddict import _SyncedDict


class SyncedAttrDict(_SyncedDict):
    """A synced dictionary where (nested) values can be accessed as attributes.

    For example:

    .. code-block:: python

        nested_dict = dict(a=dict(b=0))
        ad = SyncedAttrDict(nested_dict)
        assert ad.a.b == 0
    """

    _PROTECTED_KEYS = ("_data", "_suspend_sync_", "_load", "_save")

    def __getattr__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if name.startswith("__"):
                raise
            try:
                return self.__getitem__(name)
            except KeyError as e:
                raise AttributeError(e)

    def __setattr__(self, key, value):
        try:
            super().__getattribute__("_data")
        except AttributeError:
            super().__setattr__(key, value)
        else:
            if key.startswith("__") or key in self.__getattribute__("_PROTECTED_KEYS"):
                super().__setattr__(key, value)
            else:
                self.__setitem__(key, value)

    def __delattr__(self, key):
        if key.startswith("__") or key in self._PROTECTED_KEYS:
            super().__delattr__(key)
        else:
            self.__delitem__(key)
