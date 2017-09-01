# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Synchronized dictionary."
import logging
from contextlib import contextmanager

from ..common import six

if six.PY2:
    from UserDict import UserDict as UD
    from collections import Mapping
else:
    from collections import UserDict
    from collections.abc import Mapping


logger = logging.getLogger(__name__)

if six.PY2:
    class UserDict(UD, object):  # noqa
        pass


def _convert_to_dict(m):
    "Convert (nested) values of AttrDict to dict."
    ret = dict()
    if isinstance(m, _SyncedDict):
        for k in m:
            ret[k] = _convert_to_dict(m[k])
    elif isinstance(m, Mapping):
        for k, v in m.items():
            ret[k] = _convert_to_dict(v)
    elif isinstance(m, list):
        return [_convert_to_dict(x) for x in m]
    else:
        return m
    return ret


def _convert_nested(m, dict_type, **kwargs):
    "Convert (nested) values of AttrDict to dict."
    if m is None:
        return dict()
    ret = dict_type(None, **kwargs)
    if isinstance(m, Mapping):
        for k in m:
            ret[k] = _convert_nested(m[k], dict_type, load=ret.load, save=ret.save)
    elif isinstance(m, list):
        return [_convert_nested(i, dict_type, load=ret.load, save=ret.save) for i in m]
    else:
        return m
    return ret


class _SyncedDict(object):

    def __init__(self, initialdata=None, load=None, save=None):
        self._load, self._save = None, None
        super(_SyncedDict, self).__init__()
        self._data = _convert_nested(initialdata, type(self), load=self.load, save=self.save)
        self._load, self._save = load, save

    @contextmanager
    def _suspend_sync(self):
        load_save = self._load, self._save
        self._load, self._save = None, None
        yield
        self._load, self._save = load_save

    def load(self):
        if self._load is not None:
            self._load()

    def save(self):
        if self._save is not None:
            self._save()

    def __setitem__(self, key, value):
        self.load()
        with self._suspend_sync():
            if isinstance(value, Mapping):
                value = _convert_nested(value, type(self), load=self.load, save=self.save)
            self._data[key] = value
        self.save()
        return value

    def __getitem__(self, key):
        self.load()
        return self._data[key]

    def get(self, key, default=None):
        self.load()
        return self._data.get(key, default)

    def pop(self, key, default=None):
        self.load()
        ret = self._data.pop(key, default)
        self.save()
        return ret

    def __delitem__(self, key):
        self.load()
        del self._data[key]
        self.save()

    def clear(self):
        with self._suspend_sync():
            self._data.clear()
        self.save()

    def update(self, mapping):
        self.load()
        for key in mapping:
            self._data[key] = mapping[key]
        self.save()

    def __len__(self):
        self.load()
        return len(self._data)

    def __contains__(self, key):
        self.load()
        return key in self._data

    def __iter__(self):
        self.load()
        for d in self._data:
            yield d

    def keys(self):
        self.load()
        return self._data.keys()

    def items(self):
        self.load()
        return self._data.items()

    def __str__(self):
        self.load()
        return super(_SyncedDict, self).__str__()

    def __repr__(self):
        self.load()
        return super(_SyncedDict, self).__repr__()

    @classmethod
    def _convert_to_dict(cls, m):
        "Convert (nested) values to dict."
        ret = dict()
        if isinstance(m, cls):
            for k in m:
                ret[k] = _convert_to_dict(m[k])
        elif isinstance(m, Mapping):
            for k, v in m.items():
                ret[k] = _convert_to_dict(v)
        elif isinstance(m, list):
            return [_convert_to_dict(x) for x in m]
        else:
            return m
        return ret

    def __call__(self):
        return self._convert_to_dict(self)

    def __eq__(self, other):
        return self._data == other._data
