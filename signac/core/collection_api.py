# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import json
import errno
import uuid
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
    """The base synced collection represents a collection that is synced with a
    file.
    The class is intended for use as an ABC.In addition, it declares as abstract
    methods the methods that must be implemented by any subclass.
    """

    def __init__(self):
        self._data = None
        self._suspend_sync_ = 0

    # TODO add back-end
    @classmethod
    def from_base(self, data, filename=None, parent=None):
        """This method dynamically resolve the type of object to the
        corresponding synced collection.

        Parameters
        ----------
        data : any
            Data to be converted to base class.
        filename: str
            The signac project.
        parent : object
            Parent.

        Returns:
        --------
        data : object
            Synced object of corresponding base type.
        """
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
        """Dynamically resolve the object synced collection to the corresponding base type."""
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

    # TODO optimization
    @contextmanager
    def _safe_load_sync(self):
        data = self.load()
        backup = self.to_base() if data is None else data
        print(backup)
        try:
            yield
            self.sync()
        except BaseException:
            with self._suspend_sync():
                self._update(backup)
            raise

    def sync(self):
        """Write the data to file."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._sync()
            else:
                self._parent.sync()

    def load(self):
        """Loads the data from file."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load()
                with self._suspend_sync():
                    self._update(data)
                return data
            self._parent.load()

    # defining common methods
    def __getitem__(self, key):
        self.load()
        return self._data[key]

    def __delitem__(self, item):
        with self._safe_load_sync():
            del self._data[item]

    def __iter__(self):
        self.load()
        return iter(self._data)

    def __len__(self):
        self.load()
        return len(self._data)

    def __call__(self):
        self.load()
        return self.to_base()

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self() == other()
        else:
            return self() == other

    def __repr__(self):
        return repr(self._data)

    def __str__(self):
        return str(self._data)


class _SyncedDict(SyncedCollection, MutableMapping):
    """Implements the dict data structures"""

    _PROTECTED_KEYS = ('_data', '_suspend_sync_', '_load', '_sync', '_parent')

    VALID_KEY_TYPES = (str, int, bool, type(None))

    def __init__(self, data=None, **kwargs):
        super().__init__(**kwargs)
        if data is None:
            self._data = {}
        else:
            with self._suspend_sync():
                self._data = {
                    self._validate_key(key): self.from_base(data=value, parent=self)
                    for key, value in data.items()
                }
            self.sync()

    def to_base(self):
        """Converts the SyncedDict object to Dictionary"""
        converted = {}
        for key, value in self._data.items():
            if isinstance(value, SyncedCollection):
                converted[key] = value.to_base()
            else:
                converted[key] = value
        return converted

    def _update(self, data=None):
        """Updates the instance with data by using dfs.

        Parameters
        ----------
        data : mapping
            Data .

        Raises
        ------
        ValueError
            If data is not a mapping or None.
        """
        if data is None:
            data = {}
        if isinstance(data, Mapping):
            with self._suspend_sync():
                for key in data:
                    if key in self._data:
                        if data[key] == self._data[key]:
                            continue
                        if isinstance(self._data[key], SyncedCollection):
                            try:
                                self._data[key]._update(key)
                                continue
                            except (ValueError):
                                pass
                    self[key] = data[key]
                remove = set()
                for key in self._data:
                    if key not in data:
                        remove.add(key)
                for key in remove:
                    del self._data[key]
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a mapping or None.".format(type(data)))

    @staticmethod
    def _validate_key(key):
        "Emit a warning or raise an exception if key is invalid. Returns key."
        if isinstance(key, _SyncedDict.VALID_KEY_TYPES):
            key = str(key)
            if '.' in key:
                from ..errors import InvalidKeyError
                raise InvalidKeyError(
                    "SyncedDict keys may not contain dots ('.'): {}".format(key))
            else:
                return key
        else:
            from ..errors import KeyTypeError
            raise KeyTypeError(
                "SyncedDict keys must be str, int, bool or None, not {}".format(type(key).__name__))

    def __setitem__(self, key, value):
        self.load()
        with self._safe_load_sync():
            with self._suspend_sync():
                self._data[self._validate_key(key)] = self.from_base(data=value, parent=self)
            self.sync()

    def reset(self, data=None):
        if data is None:
            data = {}
        if isinstance(data, Mapping):
            with self._safe_load_sync():
                with self._suspend_sync():
                    self._data = {
                        self._validate_key(key): self.from_base(data=value, parent=self)
                        for key, value in data.items()
                    }
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a mapping or None.".format(type(data)))

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
        with self._safe_load_sync():
            ret = self._data.pop(key, default)
            self.sync()
        return ret

    def popitem(self, key, default=None):
        self.load()
        with self._safe_load_sync():
            ret = self._data.pop(key, default)
            self.sync()
        return ret

    def clear(self):
        self.load()
        with self._safe_load_sync():
            self._data = {}
            self.sync()

    def update(self, mapping):
        self.load()
        with self._safe_load_sync():
            with self._suspend_sync():
                for key, value in mapping.items():
                    self[key] = self.from_base(data=value, parent=self)
            self.sync()

    def setdefault(self, key, default=None):
        self.load()
        with self._safe_load_sync():
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

    def __init__(self, data=None, **kwargs):
        super().__init__(**kwargs)
        if data is None:
            self._data = []
        else:
            with self._suspend_sync():
                self._data = [self.from_base(data=value, parent=self) for value in data]
            self.sync()

    def to_base(self):
        converted = list()
        for value in self._data:
            if isinstance(value, SyncedCollection):
                converted.append(value.to_base())
            else:
                converted.append(value)
        return converted

    def _update(self, data=None):
        if data is None:
            data = []
        if isinstance(data, Sequence) and not isinstance(data, str):
            with self._suspend_sync():
                for i in range(min(len(self), len(data))):
                    if data[i] == self._data[i]:
                        continue
                    if isinstance(self._data[i], SyncedCollection):
                        try:
                            self._data[i]._update(i)
                            continue
                        except (ValueError):
                            pass
                    self._data[i] = self.from_base(data=data[i], parent=self)
                if len(self._data) > len(data):
                    self._data = self._data[:len(data)]
                else:
                    self.extend(data[len(self):])
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None."
                .format(type(data)))

    def __setitem__(self, key, value):
        self.load()
        with self._safe_load_sync():
            with self._suspend_sync():
                self._data[key] = self.from_base(data=value, parent=self)

    def __reversed__(self):
        self.load()
        return reversed(self._data)

    def __iadd__(self, iterable):
        self.load()
        with self._safe_load_sync():
            self._data += [self.from_base(data=value, parent=self) for value in iterable]

    def insert(self, index, item):
        self.load()
        with self._safe_load_sync():
            with self._suspend_sync():
                self._data.insert(index, self.from_base(data=item, parent=self))

    def append(self, item):
        self.load()
        with self._safe_load_sync():
            with self._suspend_sync():
                self._data.append(self.from_base(data=item, parent=self))

    def extend(self, iterable):
        self.load()
        with self._safe_load_sync():
            with self._suspend_sync():
                self._data.extend([self.from_base(data=value, parent=self) for value in iterable])

    def remove(self, item):
        self.load()
        with self._safe_load_sync():
            with self._suspend_sync():
                self._data.remove(self.from_base(data=item, parent=self))

    def clear(self):
        with self._safe_load_sync():
            self._data = []

    def reset(self, data=None):
        if data is None:
            data = []
        if isinstance(data, Sequence) and not isinstance(data, str):
            with self._suspend_sync():
                self._data = [self.from_base(data=value, parent=self) for value in data]
            self.sync()
        else:
            raise ValueError(
                "Unsupported type: {}. The data must be a non-string sequence or None."
                .format(type(data)))


class JSONCollection(SyncedCollection):
    """Implement sync and load using a JSON back end."""

    def __init__(self, filename=None, parent=None, write_concern=False, **kwargs):
        self._parent = parent
        self._filename = os.path.realpath(filename) if filename is not None else None
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        self._write_concern = False
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
    pass


class JSONList(JSONCollection, SyncedList):
    pass
