# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Synchronized dictionary."
import logging
from contextlib import contextmanager

from ..common import six

if six.PY2:
    from collections import Mapping
    from collections import MutableMapping
else:
    from collections.abc import Mapping
    from collections.abc import MutableMapping


logger = logging.getLogger(__name__)


class _SyncedDict(MutableMapping):

    def __init__(self, initialdata=None, parent=None):
        self._suspend_sync_ = 1
        self._parent = parent
        super(_SyncedDict, self).__init__()
        if initialdata is None:
            self._data = dict()
        else:
            self._data = {
                k: self._dfs_convert(v)
                for k, v in initialdata.items()
            }
        self._suspend_sync_ = 0

    def _dfs_convert(self, root):
        if type(root) == type(self):
            for k in root:
                root[k] = self._dfs_convert(root[k])
        elif isinstance(root, Mapping):
            ret = type(self)(parent=self)
            with ret._suspend_sync():
                for k in root:
                    ret[k] = root[k]
            return ret
        return root

    @classmethod
    def _convert_to_dict(cls, root):
        "Convert (nested) values to dict."
        if type(root) == cls:
            ret = dict()
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

    def _load(self):
        return None

    def _save(self):
        pass

    def _dfs_update(self, old, new):
        for key in new:
            if key in old:
                if old[key] == new[key]:
                    continue
                elif isinstance(new[key], Mapping) and isinstance(old[key], Mapping):
                    self._dfs_update(old[key], new[key])
                    continue
            old[key] = self._dfs_convert(new[key])
        remove = set()
        for key in old:
            if key not in new:
                remove.add(key)
        for key in remove:
            del old[key]

    def load(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load()
                if data is not None:
                    with self._suspend_sync():
                        self._dfs_update(self._data, data)
                    for value in self._data:
                        if isinstance(value, Mapping):
                            assert type(value) == type(self)
            else:
                self._parent.load()

    def save(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._save()
            else:
                self._parent.save()

    def __setitem__(self, key, value):
        self.load()
        with self._suspend_sync():
            self._data[key] = self._dfs_convert(value)
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

    def popitem(self):
        self.load()
        key, value = self._data.popitem()
        self.save()
        return key, value._as_dict()

    def setdefault(self, key, default=None):
        self.load()
        ret = self._data.setdefault(key, self._dfs_convert(default))
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

    def _update(self, mapping):
        with self._suspend_sync():
            for key in mapping.keys():
                self[key] = mapping[key]

    def update(self, mapping):
        self.load()
        self._update(mapping)
        self.save()

    def __len__(self):
        self.load()
        return len(self._data)

    def __contains__(self, key):
        self.load()
        return key in self._data

    def __iter__(self):
        self.load()
        with self._suspend_sync():
            return iter(self._data)

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

    def _as_dict(self):
        with self._suspend_sync():
            return self._convert_to_dict(self._data.copy())

    def __call__(self):
        self.load()
        return self._as_dict()

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self() == other()
        else:
            return self() == other

    def __ne__(self, other):
        return not self.__eq__(other)
