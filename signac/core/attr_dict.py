# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from contextlib import contextmanager
from ..common import six
if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping


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
    def __init__(self, mapping=None, cb=None):
        self._cb = cb
        self._data_ = dict()
        if mapping is not None:
            with self._no_callback():
                self._update(mapping)

    def __repr__(self):
        return repr(self._data)

    def _modified(self, value=None):
        if self._cb is not None:
            self._cb(convert_to_dict(self._data))

    def _invalidate(self):
        super(AttrDict, self).__setattr__('_data_', None)

    def _is_valid(self):
        if self._data_ is None:
            raise RuntimeError("Stale!")

    @property
    def _data(self):
        self._is_valid()
        return self._data_

    def __getattr__(self, key):
        if key.startswith('__'):
            super(AttrDict, self).__getattr__(key)
        return self._data[key]

    def __setattr__(self, key, value):
        try:
            super(AttrDict, self).__getattribute__('_data_')
        except AttributeError:
            super(AttrDict, self).__setattr__(key, value)
        else:
            self.__setitem__(key, value)
        return value

    def __getitem__(self, key):
        self._is_valid()
        return self._data.__getitem__(key)

    def __setitem__(self, key, value):
        if isinstance(value, Mapping):
            if not isinstance(value, type(self)):
                value = type(self)(value, cb=self._modified)
        self._data.__setitem__(key, value)
        self._modified()
        return value

    def __delitem__(self, key):
        del self._data[key]
        self._modified()

    def _update(self, other):
        for key, value in other.items():
            self[key] = value

    def __iter__(self):
        return iter(self._data)

    def __contains__(self, key):
        return key in self._data

    def __len__(self):
        return len(self._data)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def pop(self, *args, **kwargs):
        ret = self._data.pop(*args, **kwargs)
        self._modified()
        return ret

    @contextmanager
    def _no_callback(self):
        "Manipulate data without triggering a callback."
        cb = self._cb
        super(AttrDict, self).__setattr__('_cb', None)
        yield
        super(AttrDict, self).__setattr__('_cb', cb)
