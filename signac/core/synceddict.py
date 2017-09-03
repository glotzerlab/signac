# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Synchronized dictionary."
import logging
from contextlib import contextmanager

from ..common import six

if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping


logger = logging.getLogger(__name__)


class _SyncedDict(object):

    def __init__(self, initialdata=None, load=None, save=None):
        self._suspend_sync_ = 1
        self._load, self._save = load, save
        super(_SyncedDict, self).__init__()
        if initialdata is None:
            self._data = dict()
        else:
            self._data = {
                k: self._dfs_convert(v, load=self.load, save=self.save)
                for k, v in initialdata.items()
            }
        self._suspend_sync_ = 0

    @classmethod
    def _dfs_convert(cls, root, **kwargs):
        if type(root) == cls:
            for k in root:
                root[k] = cls._dfs_convert(root[k], **kwargs)
        elif isinstance(root, Mapping):
            ret = cls(None, **kwargs)
            for k in root:
                ret[k] = cls._dfs_convert(root[k], **kwargs)
            return ret
        return root

    @classmethod
    def _convert_to_dict(cls, root):
        "Convert (nested) values to dict."
        if type(root) == cls:
            ret = dict()
            root.load()
            with root._suspend_sync():
                for k in root:
                    ret[k] = cls._convert_to_dict(root[k])
            return ret
        elif type(root) == dict:
            for k in root:
                root[k] = cls._convert_to_dict(root[k])
        return root

    @contextmanager
    def _suspend_sync(self):
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    def load(self):
        if self._suspend_sync_ <= 0 and self._load is not None:
            data = self._load()
            if data is not None:
                with self._suspend_sync():
                    self._data = {
                        k: self._dfs_convert(v, load=self.load, save=self.save)
                        for k, v in data.items()
                    }

    def save(self):
        if self._suspend_sync_ <= 0 and self._save is not None:
            self._save()

    def __setitem__(self, key, value):
        with self._suspend_sync():
            self._data[key] = self._dfs_convert(value, load=self.load, save=self.save)
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
        with self._suspend_sync():
            for key in mapping.keys():
                self[key] = mapping[key]
        self.save()

    def __len__(self):
        self.load()
        return len(self._data)

    def __contains__(self, key):
        self.load()
        return key in self._data

    def __iter__(self):
        self.load()
        for k in self._data:
            yield k

    def keys(self):
        self.load()
        return self._data.keys()

    def values(self):
        self.load()
        return self._convert_to_dict(self._data).values()

    def items(self):
        self.load()
        return self._convert_to_dict(self._data).items()

    def __str__(self):
        return str(self())

    def __repr__(self):
        self.load()
        with self._suspend_sync():
            return '{}({})'.format(type(self).__name__, repr(self()))

    def _as_dict(self):
        with self._suspend_sync():
            return self._convert_to_dict(self._data.copy())

    def __call__(self):
        self.load()
        return self._as_dict()

    def __eq__(self, other):
        self.load()
        if type(other) == type(self):
            return self._data == other._data
        else:
            return self._data == other
