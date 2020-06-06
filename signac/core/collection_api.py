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

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False

try:
    from collections.abc import Collection
except ImportError:
    from collections.abc import Sized, Iterable, Container
    from _collections_abc import _check_methods

    class Collection(Sized, Iterable, Container):
        @classmethod
        def __subclasshook__(cls, C):
            if cls is Collection:
                return _check_methods(C,  "__len__", "__iter__", "__contains__")
            return NotImplemented

        @classmethod
        def __instancecheck__(cls, instance):
            for parent in cls.__mro__:
                if not isinstance(instance, parent):
                    return False
            return True


class SyncedCollection(Collection):

    def __init__(self):
        self._data = None
        self._suspend_sync_ = 0

    @classmethod
    def __instancecheck__(cls, instance):
        if not isinstance(instance, Collection):
            return False
        else:
            return all(
                [hasattr(instance, attr) for attr in
                 ['sync', 'load', 'to_base', 'from_base']])

    @classmethod
    def from_base(self, data):
        if isinstance(data, Mapping):
            return SyncedDict(data)
        elif isinstance(data, Sequence) and not isinstance(data, str):
            return SyncedList(data)
        elif NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
            elif isinstance(data, numpy.ndarray):
                return SyncedList(data.tolist())
        return data

    def to_base(self):
        pass

    @contextmanager
    def _suspend_sync(self):
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    def _load(self):
        pass

    def _sync(self):
        pass

    def sync(self):
        pass

    def load(self):
        pass


class _SyncedDict(SyncedCollection, MutableMapping):

    VALID_KEY_TYPES = (str, int, bool, type(None))

    def __init__(self, data=None):
        self._suspend_sync_ = 0
        if data is None:
            self._data = {}
        else:
            self._data = {
                key: self.from_base(value)
                for key, value in data.items()
            }

    def to_base(self):
        """Recursively crawl a SyncedDict and convert all elements to
        corresponding synced elements."""
        converted = {}
        for key, value in self._data.items():
            if isinstance(value, SyncedCollection):
                converted[key] = value.to_base()
            else:
                converted[key] = value
        return converted

    def reset(self, data=None):
        if isinstance(data, Mapping) or data is None:
            with self._suspend_sync():
                backup = copy(self._data)
                try:
                    if data is None:
                        self._data = {}
                    else:
                        self._data = {
                            key: self.from_base(value)
                            for key, value in data.items()
                        }
                    self.sync()
                except BaseException:  # rollback
                    self._data = backup
                    raise
        else:
            raise ValueError("The data must be a mapping or None.")

    @classmethod
    def _validate_key(cls, key):
        "Emit a warning or raise an exception if key is invalid. Returns key."
        if isinstance(key, str):
            if '.' in key:
                from ..errors import InvalidKeyError
                raise InvalidKeyError(
                    "keys may not contain dots ('.'): {}".format(key))
            else:
                return key
        elif isinstance(key, cls.VALID_KEY_TYPES):
            return cls._validate_key(str(key))
        else:
            from ..errors import KeyTypeError
            raise KeyTypeError(
                "keys must be str, int, bool or None, not {}".format(type(key).__name__))

    def __delitem__(self, item):
        self.load()
        del self._data[item]
        self.sync()

    def __setitem__(self, key, value):
        self.load()
        with self._suspend_sync():
            self._data[self._validate_key(key)] = self.from_base(value)
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
        ret = self._data.pop(key, default)
        self.sync()
        return ret

    def popitem(self, key, default=None):
        self.load()
        ret = self._data.pop(key, default)
        self.sync()
        return ret

    def clear(self):
        self._data = {}
        self.sync()

    def update(self, mapping):
        self.load()
        with self._suspend_sync():
            for key, value in mapping.items():
                self[key] = self.from_base(value)
        self.sync()

    def setdefault(self, key, default=None):
        self.load()
        with self._suspend_sync():
            ret = self._data.setdefault(key, self.from_base(default))
        self.sync()
        return ret


class SyncedDict(_SyncedDict):

    _PROTECTED_KEYS = ('_data', '_suspend_sync_', '_load', '_sync')

    def __getattr__(self, name):
        try:
            return super(SyncedDict, self).__getattribute__(name)
        except AttributeError:
            if name.startswith('__'):
                raise
            try:
                return self.__getitem__(name)
            except KeyError as e:
                raise AttributeError(e)

    def __setattr__(self, key, value):
        try:
            super(SyncedDict, self).__getattribute__('_data')
        except AttributeError:
            super(SyncedDict, self).__setattr__(key, value)
        else:
            if key.startswith('__') or key in self.__getattribute__('_PROTECTED_KEYS'):
                super(SyncedDict, self).__setattr__(key, value)
            else:
                self.__setitem__(key, value)

    def __delattr__(self, key):
        if key.startswith('__') or key in self._PROTECTED_KEYS:
            super(SyncedDict, self).__delattr__(key)
        else:
            self.__delitem__(key)


class SyncedList(SyncedCollection, MutableSequence):

    def __init__(self, data=None):
        super(SyncedList, self).__init__()
        if data is None:
            self._data = []
        else:
            self._data = [self.from_base(value) for value in data]

    def to_base(self):
        """Recursively crawl a SyncedList and convert all elements to
        corresponding synced elements."""
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
            with self._suspend_sync():
                backup = copy(self._data)
                try:
                    self._data = [self.from_base(value) for value in data]
                    self.sync()
                except BaseException:  # rollback
                    self._data = backup
                    raise
        else:
            raise ValueError("The data must be a sequence or None.")

    def __delitem__(self, item):
        self.load()
        del self._data[item]
        self.sync()

    def __setitem__(self, key, value):
        self.load()
        with self._suspend_sync():
            self._data[key] = self.from_base(value)
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
        self._data += [self.from_base(value) for value in iterable]
        self.sync()

    def insert(self, index, item):
        self.load()
        with self._suspend_sync():
            self._data.insert(index, self.from_base(item))
        self.sync()

    def append(self, item):
        self.load()
        with self._suspend_sync():
            self._data.append(self.from_base(item))
        self.sync()

    def extend(self, iterable):
        self.load()
        with self._suspend_sync():
            self._data.extend([self.from_base(value) for value in iterable])
        self.sync()

    def remove(self, item):
        self.load()
        with self._suspend_sync():
            self._data.remove(self.from_base(item))
        self.sync()

    def clear(self):
        self._data = []
        self.sync()


class JSONCollection(SyncedCollection):

    def __init__(self, filename, write_concern=False):
        self._filename = os.path.realpath(filename)
        self._write_concern = write_concern

    def _load_from_disk(self):
        try:
            with open(self._filename, 'rb') as file:
                blob = file.read()
                return json.loads(blob.decode())
        except IOError as error:
            if error.errno == errno.ENOENT:
                return None

    def load(self):
        data = self._load_from_disk()
        with self._suspend_sync():
            self.reset(data)

    def sync(self):
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


class JSONDict(JSONCollection, SyncedDict):
    def __init__(self, filename, data=None, write_concern=False):
        super(JSONDict, self).__init__(filename, write_concern=write_concern)
        super(JSONCollection, self).__init__(data=data)
