# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Synchronized dictionary."
import logging
from contextlib import contextmanager
from copy import deepcopy

from ..common import six

if six.PY2:
    from collections import Collection
    from collections import Mapping
    from collections import Sequence
    from collections import MutableMapping
    from collections import MutableSequence
else:
    from collections.abc import Collection
    from collections.abc import Mapping
    from collections.abc import Sequence
    from collections.abc import MutableMapping
    from collections.abc import MutableSequence
try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


logger = logging.getLogger(__name__)


def _is_nonstring_sequence(obj):
    return isinstance(obj, Sequence) and not isinstance(obj, six.string_types)


class _SyncedCollection(Collection):

    def __init__(self, initialdata, parent=None):
        self._suspend_sync_ = 1
        self._parent = parent
        super(_SyncedCollection, self).__init__()
        self._data = initialdata
        self._suspend_sync_ = 0

    @staticmethod
    def _validate_key(key):
        "Emit a warning or raise an exception if key is invalid. Returns key."
        if isinstance(key, six.string_types) and '.' in key:
            from ..errors import InvalidKeyError
            raise InvalidKeyError(
                "\nThe use of '.' (dots) in keys is invalid.\n\n"
                "See https://signac.io/document-wide-migration/ "
                "for a recipe on how to replace dots in existing keys.")
        return key

    def _dfs_convert(self, root):
        if isinstance(root, _SyncedDict):
            for k in root:
                root[k] = self._dfs_convert(root[k])
        elif isinstance(root, _SyncedList):
            for i, k in enumerate(root):
                root[i] = self._dfs_convert(k)
        elif isinstance(root, Mapping):
            ret = _SyncedDict(parent=self)
            with ret._suspend_sync():
                for k in root:
                    ret[k] = root[k]
            return ret
        elif _is_nonstring_sequence(root):
            return _SyncedList(root, self)
        elif NUMPY:
            if isinstance(root, numpy.number):
                return root.item()
            elif isinstance(root, numpy.ndarray):
                return _SyncedList(root.tolist(), self)
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
        elif type(root) is dict:
            for k in root:
                root[k] = cls._convert_to_dict(root[k])
        elif type(root) is _SyncedList:
            return [cls._convert_to_dict(item) for item in root]
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
        if isinstance(old, Mapping) and isinstance(new, Mapping):
            for key in new:
                if key in old:
                    if old[key] == new[key]:
                        continue
                    elif isinstance(new[key], Collection) and isinstance(old[key], Collection):
                        self._dfs_update(old[key], new[key])
                        continue
                old[key] = self._dfs_convert(new[key])
            remove = set()
            for key in old:
                if key not in new:
                    remove.add(key)
            for key in remove:
                del old[key]
        elif _is_nonstring_sequence(old) and _is_nonstring_sequence(new):
            for index, item in enumerate(new):
                if index < len(old):
                    if old[index] == new[index]:
                        continue
                    elif isinstance(new[index], Collection) and isinstance(old[index], Collection):
                        self._dfs_update(old[index], new[index])
                        continue
                old[index] = self._dfs_convert(new[index])
            for index in range(len(new), len(old)):
                del old[index]

    def _synced_load(self):
        self.load()

    def load(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load()
                if data is not None:
                    with self._suspend_sync():
                        self._dfs_update(self._data, data)
                    for value in self._data:
                        if isinstance(value, Mapping):
                            assert type(value) == _SyncedDict
                        elif _is_nonstring_sequence(value):
                            assert type(value) == _SyncedList
            else:
                self._parent.load()

    def _synced_save(self):
        self.save()

    def save(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._save()
            else:
                self._parent.save()

    def __setitem__(self, key, value):
        self._synced_load()
        with self._suspend_sync():
            self._data[self._validate_key(key)] = self._dfs_convert(value)
        self._synced_save()
        return value

    def __getitem__(self, key):
        self._synced_load()
        return self._data[key]

    def __delitem__(self, key):
        self._synced_load()
        del self._data[key]
        self._synced_save()

    def pop(self, key, default=None):
        self._synced_load()
        ret = self._data.pop(key, default)
        self._synced_save()
        return ret

    def __len__(self):
        self._synced_load()
        return len(self._data)

    def __contains__(self, key):
        self._synced_load()
        return key in self._data

    def __iter__(self):
        self._synced_load()
        with self._suspend_sync():
            return iter(self._data)

    def __repr__(self):
        return repr(self())

    def __str__(self):
        return str(self())

    def _as_dict(self):
        with self._suspend_sync():
            return self._convert_to_dict(self._data.copy())

    def __call__(self):
        self._synced_load()
        return self._as_dict()

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self() == other()
        else:
            return self() == other

    def __ne__(self, other):
        return not self.__eq__(other)


class _SyncedList(_SyncedCollection, MutableSequence):

    def __init__(self, initialdata=None, parent=None):
        if initialdata is None:
            initialdata = []
        else:
            initialdata = [self._dfs_convert(v) for v in initialdata]

        super(_SyncedList, self).__init__(initialdata, parent=parent)

    def __deepcopy__(self, memo):
        ret = type(self)([], deepcopy(self._parent, memo))
        for item in self:
            super(_SyncedList, ret).append(deepcopy(item, memo))
        return ret

    def insert(self, index, item):
        self._synced_load()
        self._data.insert(index, item)
        self._synced_save()

    def append(self, item):
        self._synced_load()
        self._data.append(item)
        self._synced_save()

    def __reversed__(self):
        self._synced_load()
        return reversed(self._data)

    def extend(self, iterable):
        self._synced_load()
        self._data.extend(iterable)
        self._synced_save()

    def remove(self, item):
        self._synced_load()
        self._data.remove(item)
        self._synced_save()

    def __iadd__(self, iterable):
        self._synced_load()
        self._data += iterable
        self._synced_save()


class _SyncedDict(_SyncedCollection, MutableMapping):

    def __init__(self, initialdata=None, parent=None):
        if initialdata is None:
            initialdata = {}
        else:
            initialdata = {
                self._validate_key(k): self._dfs_convert(v)
                for k, v in initialdata.items()
            }

        super(_SyncedDict, self).__init__(initialdata, parent=parent)

    def get(self, key, default=None):
        self._synced_load()
        return self._data.get(key, default)

    def popitem(self):
        self._synced_load()
        key, value = self._data.popitem()
        self._synced_save()
        return key, value._as_dict()

    def setdefault(self, key, default=None):
        self._synced_load()
        ret = self._data.setdefault(key, self._dfs_convert(default))
        self._synced_save()
        return ret

    def clear(self):
        with self._suspend_sync():
            self._data.clear()
        self._synced_save()

    def _update(self, mapping):
        with self._suspend_sync():
            for key in mapping.keys():
                self[key] = mapping[key]

    def update(self, mapping):
        self._synced_load()
        self._update(mapping)
        self._synced_save()

    def keys(self):
        self._synced_load()
        return self._data.keys()

    def values(self):
        self._synced_load()
        return self._convert_to_dict(self._data).values()

    def items(self):
        self._synced_load()
        return self._convert_to_dict(self._data).items()
