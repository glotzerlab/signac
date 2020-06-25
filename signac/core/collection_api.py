# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import json
import errno
import uuid
from copy import copy
from contextlib import contextmanager
from collections.abc import Mapping
from collections.abc import MutableMapping
from collections.abc import Sequence
from collections.abc import MutableSequence
from abc import abstractmethod

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False

try:
    from collections.abc import Collection
except ImportError:
    # Collection does not exist in Python 3.5, only Python 3.6 or newer.

    from collections.abc import Sized, Iterable, Container

    def _check_methods(C, *methods):
        mro = C.__mro__
        for method in methods:
            for B in mro:
                if method in B.__dict__:
                    if B.__dict__[method] is None:
                        return NotImplemented
                    break
            else:
                return NotImplemented
        return True

    class Collection(Sized, Iterable, Container):  # type: ignore
        @classmethod
        def __subclasshook__(cls, C):
            if cls is Collection:
                return _check_methods(C,  "__len__", "__iter__", "__contains__")
            return NotImplemented


class SyncedCollection(Collection):

    def __init__(self, parent=None):
        self._data = None
        self._suspend_sync_ = 0
        self._parent = parent

    # TODO add back-end
    @classmethod
    def from_base(self, data, filename=None, parent=None):
        if isinstance(data, Mapping):
            return JSONDict(filename=filename, data=data, parent=parent)
        elif isinstance(data, Sequence) and not isinstance(data, str):
            return JSONList(filename=filename, data=data, parent=parent)
        elif NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
            elif isinstance(data, numpy.ndarray):
                return JSONList(filename=filename, data=data.tolist(), parent=parent)
        return data

    @abstractmethod
    def to_base(self):
        pass

    @contextmanager
    def _suspend_sync(self):
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    @abstractmethod
    def _load(self):
        pass

    @abstractmethod
    def _sync(self):
        pass

    @contextmanager
    def _safe_sync(self):
        backup = self._data
        try:
            yield
        except BaseException:
            self._data = backup
            raise

    def sync(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._sync()
            else:
                self._parent.sync()

    def load(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load()
                with self._suspend_sync():
                    self.reset(data)
            else:
                self._parent.load()


class _SyncedDict(SyncedCollection, MutableMapping):

    _PROTECTED_KEYS = ('_data', '_suspend_sync_', '_load', '_sync', '_parent')

    VALID_KEY_TYPES = (str, int, bool, type(None))

    def __init__(self, **kwargs):
        data = kwargs.pop('data', None)
        super().__init__(**kwargs)
        if data is None:
            self._data = {}
        else:
            self._data = {
                self._validate_key(key): self.from_base(data=value, parent=self)
                for key, value in data.items()
            }

    def to_base(self):
        converted = {}
        for key, value in self._data.items():
            if isinstance(value, SyncedCollection):
                converted[key] = value.to_base()
            else:
                converted[key] = value
        return converted

    def reset(self, data=None):
        if data is None:
            data = {}
        backup = self._data
        if isinstance(data, Mapping):
            try:
                with self._suspend_sync():
                    for key in data:
                        if key in self._data:
                            if data[key] == self._data[key]:
                                continue
                            try:
                                self._data[key].reset(key)
                                continue
                            except (ValueError, AttributeError):
                                pass
                        self._data[key] = self.from_base(data=data[key], parent=self)
                    remove = set()
                    for key in self._data:
                        if key not in data:
                            remove.add(key)
                    for key in remove:
                        del self._data[key]
                self.sync()
            except BaseException:  # rollback
                self._data = backup
                raise
        else:
            raise ValueError("The data must be a mapping or None not {}.".format(type(data)))

    @staticmethod
    def _validate_key(key):
        "Emit a warning or raise an exception if key is invalid. Returns key."
        if isinstance(key, _SyncedDict.VALID_KEY_TYPES):
            key = str(key)
            if '.' in key:
                from ..errors import InvalidKeyError
                raise InvalidKeyError(
                    "keys may not contain dots ('.'): {}".format(key))
            else:
                return key
        else:
            from ..errors import KeyTypeError
            raise KeyTypeError(
                "keys must be str, int, bool or None, not {}".format(type(key).__name__))

    def __delitem__(self, item):
        self.load()
        with self._safe_sync():
            del self._data[item]
            self.sync()

    def __setitem__(self, key, value):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                self._data[self._validate_key(key)] = self.from_base(data=value, parent=self)
            self.sync()

    def __getitem__(self, key):
        self.load()
        return self._data[key]

    def __iter__(self):
        self.load()
        return iter(self._data)

    def __call__(self):
        self.load()
        return self.to_base()

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self() == other()
        else:
            return self() == other

    def __len__(self):
        self.load()
        return len(self._data)

    def __repr__(self):
        return repr(self())

    def __str__(self):
        return str(self())

    def keys(self):
        self.load()
        return self._data.keys()

    def values(self):
        self.load()
        return self.to_base().values()

    def items(self):
        self.load()
        return self.to_base().items()

    def get(self, key, default=None):
        self.load()
        return self._data.get(key, default)

    def pop(self, key, default=None):
        self.load()
        with self._safe_sync():
            ret = self._data.pop(key, default)
            self.sync()
        return ret

    def popitem(self, key, default=None):
        self.load()
        with self._safe_sync():
            ret = self._data.pop(key, default)
            self.sync()
        return ret

    def clear(self):
        self.load()
        with self._safe_sync():
            self._data = {}
            self.sync()

    def update(self, mapping):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                for key, value in mapping.items():
                    self[key] = self.from_base(data=value, parent=self)
            self.sync()

    def setdefault(self, key, default=None):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                ret = self._data.setdefault(key, self.from_base(data=default, parent=self))
            self.sync()
        return ret


class SyncedAttrDict(_SyncedDict):

    def __getattr__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if name.startswith('__'):
                raise
            try:
                return self.__getitem__(name)
            except KeyError as e:
                raise AttributeError(e)

    def __setattr__(self, key, value):
        try:
            super().__getattribute__('_data')
        except AttributeError:
            super().__setattr__(key, value)
        else:
            if key.startswith('__') or key in self._PROTECTED_KEYS:
                super().__setattr__(key, value)
            else:
                self.__setitem__(key, value)

    def __delattr__(self, key):
        if key.startswith('__') or key in self._PROTECTED_KEYS:
            super().__delattr__(key)
        else:
            self.__delitem__(key)


class SyncedList(SyncedCollection, MutableSequence):

    def __init__(self, **kwargs):
        data = kwargs.pop('data', None)
        super().__init__(**kwargs)
        if data is None:
            self._data = []
        else:
            self._data = [self.from_base(data=value, parent=self) for value in data]

    def to_base(self):
        converted = list()
        for value in self._data:
            if isinstance(value, SyncedCollection):
                converted.append(value.to_base())
            else:
                converted.append(value)
        return converted

    def reset(self, data=None):
        if data is None:
            data = []
        if isinstance(data, Sequence) and not isinstance(data, str):
            backup = copy(self._data)
            try:
                with self._suspend_sync():
                    for i in range(min(len(self), len(data))):
                        if data[i] == self._data[i]:
                            continue
                        try:
                            self._data[i].reset(data[i])
                            continue
                        except (ValueError, AttributeError):
                            pass
                        self._data[i] = self.from_base(data=data[i], parent=self)
                    if len(self._data) > len(data):
                        self._data = self._data[:len(data)]
                    else:
                        self.extend(data[len(self):])
                self.sync()
            except BaseException:  # rollback
                self._data = backup
                raise
        else:
            raise ValueError("The data must be a non-string sequence or None.")

    def __delitem__(self, item):
        self.load()
        with self._safe_sync():
            del self._data[item]
            self.sync()

    def __setitem__(self, key, value):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                self._data[key] = self.from_base(data=value, parent=self)
            self.sync()

    def __getitem__(self, key):
        self.load()
        return self._data[key]

    def __iter__(self):
        self.load()
        return iter(self._data)

    def __len__(self):
        self.load()
        return len(self._data)

    def __reversed__(self):
        self.load()
        return reversed(self._data)

    def __iadd__(self, iterable):
        self.load()
        with self._safe_sync():
            self._data += [self.from_base(data=value, parent=self) for value in iterable]
            self.sync()

    def __call__(self):
        self.load()
        return self.to_base()

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self() == other()
        else:
            return self() == other

    def __repr__(self):
        return repr(self())

    def __str__(self):
        return str(self())

    def insert(self, index, item):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                self._data.insert(index, self.from_base(data=item, parent=self))
            self.sync()

    def append(self, item):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                self._data.append(self.from_base(data=item, parent=self))
            self.sync()

    def extend(self, iterable):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                self._data.extend([self.from_base(data=value, parent=self) for value in iterable])
            self.sync()

    def remove(self, item):
        self.load()
        with self._safe_sync():
            with self._suspend_sync():
                self._data.remove(self.from_base(data=item, parent=self))
            self.sync()

    def clear(self):
        with self._safe_sync():
            self._data = []
            self.sync()


class JSONCollection(SyncedCollection):

    def __init__(self, **kwargs):
        filename = kwargs.pop('filename', None)
        self._filename = None if filename is None else os.path.realpath(filename)
        self._write_concern = kwargs.pop('write_concern', True)
        super().__init__(**kwargs)

    def _load(self):
        try:
            with open(self._filename, 'rb') as file:
                blob = file.read()
                return json.loads(blob.decode())
        except IOError as error:
            if error.errno == errno.ENOENT:
                return None

    def _sync(self):
        data = self.to_base()
        # Serialize data:
        blob = json.dumps(data).encode()

        if self._write_concern:
            dirname, filename = os.path.split(self._filename)
            fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
                uid=uuid.uuid4(), fn=filename))
            with open(fn_tmp, 'wb') as tmpfile:
                tmpfile.write(blob)
            os.replace(fn_tmp, self._filename)
        else:
            with open(self._filename, 'wb') as file:
                file.write(blob)


class JSONDict(JSONCollection, SyncedAttrDict):
    def __init__(self, filename=None, data=None, parent=None, write_concern=False):
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        super().__init__(filename=filename, data=data, parent=parent, write_concern=write_concern)


class JSONList(JSONCollection, SyncedList):
    def __init__(self, filename=None, data=None, parent=None, write_concern=False):
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        super().__init__(filename=filename, data=data, parent=parent, write_concern=write_concern)
