# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from contextlib import contextmanager
from ..common import six
from .synceddict import _SyncedDict
if six.PY2:
    from UserDict import UserDict as UD
    from collections import Mapping
else:
    from collections import UserDict
    from collections.abc import Mapping

if six.PY2:
    class UserDict(UD, object):  # noqa
        pass

def convert_to_dict(m):
    "Convert (nested) values of AttrDict to dict."
    ret = dict()
    if isinstance(m, AttrDict):
        for k in m:
            ret[k] = convert_to_dict(getattr(m, k))
    elif isinstance(m, Mapping):
        for k, v in m.items():
            ret[k] = convert_to_dict(v)
    elif isinstance(m, list):
        return [convert_to_dict(x) for x in m]
    else:
        return m
    return ret


class AttrDict(object):
    """A mapping where (nested) values can be accessed as attributes.

    For example:

    .. code-block:: python

        nested_dict = dict(a=dict(b=0))
        ad = AttrDict(nested_dict)
        assert ad.a.b == 0
    """
    def __init__(self, initialdata=None):
        super(AttrDict, self).__init__()
        if initialdata is None:
            initialdata = dict()
        else:
            initialdata = dict(initialdata)
        self._data = initialdata

    def __getattr__(self, key):
        if key.startswith('_') or key in ('load', 'save', 'get', 'clear',
                                          'update', 'pop', 'keys', 'items'):
            return super(AttrDict, self).__getattribute__(key)
        else:
            return self._data[key]

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super(AttrDict, self).__setattr__(key, value)
        else:
            try:
                super(AttrDict, self).__getattribute__('_data')
            except AttributeError:
                super(AttrDict, self).__setattr__(key, value)
            else:
                self.__setitem__(key, value)
        return value

    def __setitem__(self, key, value):
        raise NotImplementedError()
        if isinstance(value, Mapping):
            value = type(self)(value)
        super(AttrDict, self).__setitem__(key, value)
        return value


class SyncedAttrDict(_SyncedDict, AttrDict):
    pass
