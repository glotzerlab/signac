# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Data store implementation with backend HDF5 file."
import logging
import os
import warnings
import array
from threading import RLock

from ..common import six

if six.PY2:
    from collections import Mapping
    from collections import MutableMapping
else:
    from collections.abc import Mapping
    from collections.abc import MutableMapping


def _group_is_pandas_type(group):
    return 'pandas_type' in group.attrs


_is_pandas_type = None
_pandas = None


def _load_pandas():
    # Late binding to improve import performance.
    global _pandas
    global _is_pandas_type

    if _pandas is None or _is_pandas_type is None:
        try:
            import pandas

            def _is_pandas_type(value):
                return isinstance(value, pandas.core.generic.PandasObject)
        except ImportError:
            def _is_pandas_type(value):
                return False    # Must be False when pandas is not available.
        else:
            _pandas = pandas


def _requires_tables():
    try:
        import tables  # noqa
    except ImportError:
        raise ImportError(
            "Storing and loading pandas objects requires the PyTables package.")


logger = logging.getLogger(__name__)


class H5StoreClosedError(RuntimeError):
    "Raised when trying to access a closed store."


class H5StoreAlreadyOpenError(OSError):
    """Indicates that the underlying HDF5-file is already openend."""


def _h5set(file, grp, key, value, path=None):
    """Set a key in an h5py container, recursively converting Mappings to h5py
    groups and transparently handling None."""
    import h5py
    import numpy    # h5py depends on numpy, so this is safe.
    path = path + '/' + key if path else key

    # Guard against assigning a group to itself, e.g., `h5s[key] = h5s[key]`,
    # where h5s[key] is a mapping. This is necessary, because the original
    # mapping would be deleted prior to assignment.
    if isinstance(value, H5Group) and key in grp:
        if grp[key] == value._group:
            return  # Groups are identical, do nothing.

    # Delete any existing data
    if key in grp:
        del grp[key]

    # Mapping-types
    if isinstance(value, Mapping):
        subgrp = grp.create_group(key)
        for k, v in value.items():
            _h5set(file, subgrp, k, v, path)

    # Regular built-in types:
    elif value is None:
        grp.create_dataset(key, data=None, shape=None, dtype='f')
    elif isinstance(value, (int, float, str, bool, array.array)):
        grp[key] = value
    elif isinstance(value, bytes):
        grp[key] = numpy.bytes_(value)

    # NumPy types
    elif type(value).__module__ == numpy.__name__:
        grp[key] = value

    # h5py native types
    elif isinstance(value, h5py._hl.dataset.Dataset):
        grp[key] = value[()]    # Create a copy, not a hard link!

    # Other types
    else:
        _load_pandas()   # might be a pandas type
        if _is_pandas_type(value):
            _requires_tables()
            file.close()
            with _pandas.HDFStore(file._filename) as store:
                store[path] = value
            file.open()
        else:
            grp[key] = value
            warnings.warn(
                "Storage for object of type '{}' appears to have succeeded, but this "
                "type is not officially supported!".format(type(value)))


def _h5get(file, grp, key, path=None):
    """Retrieve the underlying data for a key from its h5py container."""
    path = path + '/' + key if path else key
    result = grp[key]

    if _group_is_pandas_type(result):
        _load_pandas()
        _requires_tables()
        grp.file.flush()
        with _pandas.HDFStore(grp.file.filename) as store:
            return store[path]
    try:
        shape = result.shape
        if shape is None:
            return None
        elif shape:
            return result
        else:
            return result[()]
    except AttributeError:
        if isinstance(result, MutableMapping):
            return H5Group(file, path)
        else:
            return result


def _validate_key(key):
    "Emit a warning or raise an exception if key is invalid. Returns key."
    if '.' in key:
        from ..warnings import SignacDeprecationWarning
        warnings.warn(
            "\nThe use of '.' (dots) in keys is deprecated and may lead to "
            "unexpected behavior!\nSee http://www.signac.io/document-wide-migration/ "
            "for a recipe on how to replace dots in all keys.",
            SignacDeprecationWarning)
    return key


class _ensure_open(object):

    __slots__ = ['file', 'open']

    def __init__(self, file):
        self.file = file
        self.open = False

    def __enter__(self):
        if self.file._file is None:
            self.file.open()
            self.open = True

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.open:
            self.file.close()
            self.open = False


class H5Group(MutableMapping):
    """An abstraction layer over h5py's Group objects, to manage and return data."""

    __slots__ = ['_file', '_path']

    def __init__(self, file, path):
        self._file = file
        self._path = path

    @property
    def _group(self):
        return self._file.file[self._path]

    def __getitem__(self, key):
        with _ensure_open(self._file):
            return _h5get(self._file, self._group, key, self._path)

    def __setitem__(self, key, value):
        with _ensure_open(self._file):
            _h5set(self._file, self._group, _validate_key(key), value, self._path)
            return value

    def __delitem__(self, key):
        with _ensure_open(self._file):
            del self._group[key]

    def __getattr__(self, name):
        with _ensure_open(self._file):
            if name in self._group.keys():
                return self.__getitem__(name)
            else:
                return getattr(self._group, name)

    def __setattr__(self, key, value):
        if key.startswith('__') or key in self.__slots__:
            super(H5Group, self).__setattr__(key, value)
        else:
            self.__setitem__(key, value)

    def __iter__(self):
        # The generator below should be refactored to use 'yield from'
        # once we drop Python 2.7 support.
        with _ensure_open(self._file):
            for key in self._group.keys():
                yield key

    def __len__(self):
        with _ensure_open(self._file):
            return len(self._group)

    def __eq__(self, other):
        with _ensure_open(self._file):
            if isinstance(self, Mapping) and isinstance(other, Mapping):
                return super(H5Group, self).__eq__(other)
            elif type(other) == type(self):
                return self._group == other._group
            else:
                return super(H5Group, self).__eq__(other)


class H5Store(MutableMapping):
    """An HDF5-backed container for storing array-like and dictionary-like data.

    The H5Store API is a :class:`collections.abc.MutableMapping` and therefore
    behaves similar to a :class:`dict`, but all data is stored persistently in
    the associated HDF5-file on disk.

    Supported types include:

      * built-in types (int, float, str, bool, NoneType, array)
      * numpy arrays
      * pandas data frames (requires pandas and pytables),

    as well as mappings of values of these types. Values can be accessed as
    attributes (`h5s.foo`) or via key index (`h5s['foo']`).

    Example:

    .. code-block:: python

        with H5Store('file.h5') as h5s:
            h5s['foo'] = 'bar'
            assert 'foo' in h5s
            assert h5s.foo == 'bar'
            assert h5s['foo'] == 'bar'

    :param filename:
        The filename of the underlying HDF5-file.
    :param mode:
        The file open mode to use. Defaults to 'a' (append).
    :param kwargs:
        Additional keyword arguments to be forward to the h5py.File constructor
        See documentation for :class:`h5py.File` for more information.
    """
    __slots__ = ['_filename', '_file', '_mode', '_kwargs']

    _thread_lock = RLock()

    def __init__(self, filename, mode=None, **kwargs):
        if not (isinstance(filename, six.string_types) and len(filename) > 0):
            raise ValueError('H5Store filename must be a non-empty string.')
        if mode is None:
            mode = 'a'
        self._filename = os.path.realpath(filename)
        self._file = None
        self._mode = mode
        self._kwargs = kwargs

    def __repr__(self):
        return "<{}(filename={})>".format(type(self).__name__, os.path.relpath(self._filename))

    def __str__(self):
        return "<{}(filename={})>".format(type(self).__name__, os.path.basename(self._filename))

    def __del__(self):
        self.close()

    def __enter__(self):
        try:
            self.open()
        except H5StoreAlreadyOpenError:
            pass
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def open(self):
        """Open the underlying HDF5-file."""
        if self._file is not None:
            raise H5StoreAlreadyOpenError(self)
        import h5py
        self._thread_lock.acquire()
        try:
            self._file = h5py.File(self._filename, mode=self._mode, **self._kwargs)
        except:  # noqa We need to release under **all** circumstances upon error!
            self._thread_lock.release()
            raise
        return self

    def close(self):
        """Close the underlying HDF5-file."""
        locked = True
        try:
            self._file.close()
            self._file = None
        except AttributeError:
            locked = False
        finally:
            if locked:
                self._thread_lock.release()

    @property
    def file(self):
        if self._file is None:
            raise H5StoreClosedError(self._filename)
        else:
            return self._file

    @property
    def mode(self):
        return self._mode

    def flush(self):
        """Flush the underlying HDF5-file."""
        self._file.flush()

    def __getitem__(self, key):
        key = key if key.startswith('/') else '/' + key
        with _ensure_open(self):
            return _h5get(self, self._file, key)

    def __setitem__(self, key, value):
        with _ensure_open(self):
            _h5set(self, self._file, _validate_key(key), value)
            return value

    def __delitem__(self, key):
        with _ensure_open(self):
            del self._file[key]

    def __getattr__(self, name):
        try:
            return super(H5Store, self).__getattribute__(name)
        except AttributeError:
            if name.startswith('__') or name in self.__slots__:
                raise
            try:
                return self.__getitem__(name)
            except KeyError as e:
                raise AttributeError(e)

    def __setattr__(self, key, value):
        if key.startswith('__') or key in self.__slots__:
            super(H5Store, self).__setattr__(key, value)
        else:
            self.__setitem__(key, value)

    def __iter__(self):
        with _ensure_open(self):
            # The generator below should be refactored to use 'yield from'
            # once we drop Python 2.7 support.
            for key in self._file.keys():
                yield key

    def __len__(self):
        with _ensure_open(self):
            return len(self._file)

    def __contains__(self, key):
        with _ensure_open(self):
            return key in self._file

    def clear(self):
        """Remove all data from this store.

            .. danger::

                All data will be removed, this action cannot be reversed!
        """
        with _ensure_open(self):
            self._file.clear()
