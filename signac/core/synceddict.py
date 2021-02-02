# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Synchronized dictionary."""
import logging
from collections.abc import Mapping, MutableMapping
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


logger = logging.getLogger(__name__)


class _SyncedList(list):
    def __init__(self, iterable, parent):
        self._parent = parent
        super().__init__(iterable)

    def __deepcopy__(self, memo):
        ret = type(self)([], deepcopy(self._parent, memo))
        for item in self:
            super(_SyncedList, ret).append(deepcopy(item, memo))
        return ret

    def __getitem__(self, key):
        self._parent.load()
        ret = super().__getitem__(key)
        return ret

    def __setitem__(self, key, value):
        self._parent.load()
        ret = super().__setitem__(key, value)
        self._parent.save()
        return ret

    def __delitem__(self, key):
        self._parent.load()
        super().__delitem__(key)
        self._parent.save()

    def __getattribute__(self, name):
        outer = super().__getattribute__(name)

        if name in (
            "append",
            "clear",
            "extend",
            "insert",
            "pop",
            "remove",
            "reverse",
            "sort",
        ):

            @wraps(outer)
            def outer_wrapped_in_load_and_save(*args, **kwargs):
                if hasattr(self, "_parent"):
                    self._parent.load()
                    ret = outer(*args, **kwargs)
                    self._parent.save()
                    return ret
                else:
                    return outer(*args, **kwargs)

            return outer_wrapped_in_load_and_save
        else:
            return outer


class _SyncedDict(MutableMapping):

    VALID_KEY_TYPES = (str, int, bool, type(None))

    def __init__(self, initialdata=None, parent=None):
        self._suspend_sync_ = 1
        self._parent = parent
        super().__init__()
        if initialdata is None:
            self._data = {}
        else:
            self._data = {
                self._validate_key(k): self._dfs_convert(v)
                for k, v in initialdata.items()
            }
        self._suspend_sync_ = 0

    @classmethod
    def _validate_key(cls, key):
        """Emit a warning or raise an exception if key is invalid. Returns key."""
        if isinstance(key, str):
            if "." in key:
                from ..errors import InvalidKeyError

                raise InvalidKeyError(f"keys may not contain dots ('.'): {key}")
            else:
                return key
        elif isinstance(key, cls.VALID_KEY_TYPES):
            return cls._validate_key(str(key))
        else:
            from ..errors import KeyTypeError

            raise KeyTypeError(
                "keys must be str, int, bool or None, not {}".format(type(key).__name__)
            )

    def _dfs_convert(self, root):
        if type(root) in (bool, float, int, type(None), str):
            # Allow common types to exit early. This must use type equality
            # checks instead of isinstance() to avoid catching NumPy values.
            return root
        elif type(root) is type(self):
            for k in root:
                root[k] = self._dfs_convert(root[k])
        elif isinstance(root, Mapping):
            ret = type(self)(parent=self)
            with ret._suspend_sync():
                for k in root:
                    ret[k] = root[k]
            return ret
        elif type(root) in (list, tuple):
            return _SyncedList(root, parent=self)
        elif NUMPY:
            if isinstance(root, numpy.ndarray):
                if root.shape == ():
                    return root.item()
                else:
                    return _SyncedList(root.tolist(), parent=self)
            elif isinstance(root, numpy.number):
                return root.item()
        return root

    @classmethod
    def _convert_to_dict(cls, root):
        """Convert (nested) values to dict."""
        if type(root) in (bool, float, int, type(None), str):
            # Allow common types to exit early. This must use type equality
            # checks instead of isinstance() to avoid catching NumPy values.
            return root
        elif type(root) is cls:
            ret = {}
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
                            assert type(value) is type(self)
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

    def get(self, key, default=None):
        self._synced_load()
        return self._data.get(key, default)

    def pop(self, key, default=None):
        self._synced_load()
        ret = self._data.pop(key, default)
        self._synced_save()
        return ret

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

    def __delitem__(self, key):
        self._synced_load()
        del self._data[key]
        self._synced_save()

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

    def keys(self):
        self._synced_load()
        return self._data.keys()

    def values(self):
        self._synced_load()
        return self._convert_to_dict(self).values()

    def items(self):
        self._synced_load()
        return self._convert_to_dict(self).items()

    def __repr__(self):
        return repr(self())

    def __str__(self):
        return str(self())

    def _as_dict(self):
        with self._suspend_sync():
            return self._convert_to_dict(self)

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
