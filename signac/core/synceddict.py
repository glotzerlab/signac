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
        self._load, self._save = None, None
        super(_SyncedDict, self).__init__()
        if initialdata is None:
            self._data = dict()
        else:
            self._data = {
                k: self._dfs_convert(v, load=self.load, save=self.save)
                for k, v in initialdata.items()
            }
        self._load, self._save = load, save

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
            for k in root:
                ret[k] = cls._convert_to_dict(root[k])
            return ret
        elif type(root) == dict:
            for k in root:
                root[k] = cls._convert_to_dict(root[k])
        return root

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

    def __call__(self):
        return self._convert_to_dict(self)

    def __eq__(self, other):
        return self._data == other._data
