# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from .synceddict import _SyncedDict


class _AttrDict(object):
    """A mapping where (nested) values can be accessed as attributes.

    For example:

    .. code-block:: python

        nested_dict = dict(a=dict(b=0))
        ad = _AttrDict(nested_dict)
        assert ad.a.b == 0
    """
    def __init__(self, initialdata=None):
        super(_AttrDict, self).__init__()
        if initialdata is None:
            initialdata = dict()
        else:
            initialdata = dict(initialdata)
        self._data = initialdata

    def __getattr__(self, key):
        try:
            return super(_AttrDict, self).__getattribute__(key)
        except AttributeError:
            return self._data[key]

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super(_AttrDict, self).__setattr__(key, value)
        else:
            try:
                super(_AttrDict, self).__getattribute__('_data')
            except AttributeError:
                super(_AttrDict, self).__setattr__(key, value)
            else:
                self.__setitem__(key, value)
        return value


class SyncedAttrDict(_SyncedDict, _AttrDict):
    pass
