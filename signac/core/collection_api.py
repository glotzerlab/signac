from contextlib import contextmanager
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import MutableMapping
from collections.abc import Sequence
from collections.abc import MutableSequence

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


class SyncedCollection(Collection):

    def __init__(self):
        self._data = None
        self._suspend_sync_ = 0

    def __instancecheck__(self, instance):
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
        if self._suspend_sync_ <= 0:
            self._sync()

    def load(self):
        if self._suspend_sync_ <= 0:
            data = self._load()
            if data is not None:
                with self._suspend_sync():
                    self._data = self.from_base(data)


class SyncedDict(SyncedCollection, MutableMapping):

    VALID_KEY_TYPES = (str, int, bool, type(None))

    _PROTECTED_KEYS = ('_data', '_suspend_sync_', '_load', '_sync')

    def __init__(self, data=None):
        super(SyncedDict, self).__init__()
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
            self.__getattribute__('_data')
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
