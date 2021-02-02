# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Data store implementation with backend HDF5 file."""
import array
import errno
import logging
import os
import warnings
from collections.abc import Mapping, MutableMapping
from threading import RLock

from ..errors import InvalidKeyError
from .dict_manager import DictManager

__all__ = [
    "H5Store",
    "H5Group",
    "H5StoreManager",
    "H5StoreClosedError",
    "H5StoreAlreadyOpenError",
]


def _group_is_pandas_type(group):
    return "pandas_type" in group.attrs


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
                return False  # Must be False when pandas is not available.

        else:
            _pandas = pandas


def _requires_tables():
    try:
        import tables  # noqa
    except ImportError:
        raise ImportError(
            "Storing and loading pandas objects requires the PyTables package."
        )


logger = logging.getLogger(__name__)


class H5StoreClosedError(RuntimeError):
    """Raised when trying to access a closed store."""


class H5StoreAlreadyOpenError(OSError):
    """Indicates that the underlying HDF5 file is already openend."""


def _h5set(store, grp, key, value, path=None):
    """Set a key in an h5py container.

    This method recursively converts Mappings to h5py groups and transparently
    handles None values.
    """
    import h5py
    import numpy  # h5py depends on numpy, so this is safe.

    path = path + "/" + key if path else key

    # Guard against assigning a group to itself, e.g., `h5s[key] = h5s[key]`,
    # where h5s[key] is a mapping. This is necessary, because the original
    # mapping would be deleted prior to assignment.
    if key in grp:
        if isinstance(value, H5Group):
            if grp[key] == value._group:
                return  # Groups are identical, do nothing.
        elif isinstance(value, h5py._hl.dataset.Dataset):
            if grp == value.parent:
                return  # Dataset is identical, do nothing.

        # Delete any existing data
        del grp[key]

    # Mapping-types
    if isinstance(value, Mapping):
        subgrp = grp.create_group(key)
        for k, v in value.items():
            _h5set(store, subgrp, k, v, path)

    # Regular built-in types:
    elif value is None:
        grp.create_dataset(key, data=None, shape=None, dtype="f")
    elif isinstance(value, (int, float, str, bool, array.array)):
        grp[key] = value
    elif isinstance(value, bytes):
        grp[key] = numpy.bytes_(value)

    # NumPy types
    elif type(value).__module__ == numpy.__name__:
        grp[key] = value

    # h5py native types
    elif isinstance(value, h5py._hl.dataset.Dataset):
        grp[key] = value  # Creates hard-link!

    # Other types
    else:
        _load_pandas()  # might be a pandas type
        if _is_pandas_type(value):
            _requires_tables()
            store.close()
            with _pandas.HDFStore(store._filename, mode="a") as store_:
                store_[path] = value
            store.open()
        else:
            grp[key] = value
            warnings.warn(
                "Storage for object of type '{}' appears to have succeeded, but this "
                "type is not officially supported!".format(type(value))
            )


def _h5get(store, grp, key, path=None):
    """Retrieve the underlying data for a key from its h5py container."""
    import h5py

    path = path + "/" + key if path else key
    result = grp[key]

    if _group_is_pandas_type(result):
        _load_pandas()
        _requires_tables()
        grp.file.flush()
        # The store must be closed for pandas to open it safely, but first we
        # copy the filename since it is not accessible after closing the file.
        # The pandas data is returned by copy, so the HDFStore can be closed.
        # Then we re-open the store.
        filename = grp.file.filename
        store.close()
        with _pandas.HDFStore(filename, mode="r") as store_:
            data = store_[path]
        store.open()
        return data
    try:
        shape = result.shape
        if shape is None:
            return None
        elif shape:
            return result
        elif (
            h5py.version.version_tuple.major >= 3
            and h5py.check_dtype(vlen=result.dtype) is str
        ):
            # h5py >=3.0.0 returns strings as bytes. This returns str for
            # consistency with past behavior in signac.
            return result.asstr()[()]
        else:
            return result[()]
    except AttributeError:
        if isinstance(result, MutableMapping):
            return H5Group(store, path)
        else:
            return result


class _ensure_open:

    __slots__ = ["file", "open", "kwargs"]

    def __init__(self, file, **kwargs):
        self.file = file
        self.open = False
        self.kwargs = kwargs

    def __enter__(self):
        if self.file._file is None:
            self.file._open(**self.kwargs)
            self.open = True

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.open:
            self.file.close()
            self.open = False


class H5Group(MutableMapping):
    """An abstraction layer over h5py's Group objects, to manage and return data."""

    __slots__ = ["_store", "_path"]

    def __init__(self, store, path):
        self._store = store
        self._path = path

    def __repr__(self):
        return "{}(store={}, path={})".format(
            type(self).__name__, repr(self._store), repr(self._path)
        )

    @property
    def _group(self):
        return self._store.file[self._path]

    def __getitem__(self, key):
        with _ensure_open(self._store):
            return _h5get(self._store, self._group, key, self._path)

    def __setitem__(self, key, value):
        with _ensure_open(self._store):
            _h5set(
                self._store,
                self._group,
                self._store._validate_key(key),
                value,
                self._path,
            )
            return value

    def __delitem__(self, key):
        with _ensure_open(self._store):
            del self._group[key]

    def __getattr__(self, name):
        with _ensure_open(self._store):
            if name in self._group.keys():
                return self.__getitem__(name)
            else:
                return getattr(self._group, name)

    def __setattr__(self, key, value):
        if key.startswith("__") or key in self.__slots__:
            super().__setattr__(key, value)
        else:
            self.__setitem__(key, value)

    def setdefault(self, key, value):
        """Set a value for a key if that key is not already set."""
        super().setdefault(key, value)
        return self.__getitem__(key)

    def __iter__(self):
        with _ensure_open(self._store):
            yield from self._group.keys()

    def __len__(self):
        with _ensure_open(self._store):
            return len(self._group)

    def __eq__(self, other):
        with _ensure_open(self._store):
            if isinstance(self, Mapping) and isinstance(other, Mapping):
                return super().__eq__(other)
            elif type(other) == type(self):
                return self._group == other._group
            else:
                return super().__eq__(other)


class H5Store(MutableMapping):
    r"""An HDF5-backed container for storing array-like and dictionary-like data.

    The H5Store is a :class:`~collections.abc.MutableMapping` and therefore
    behaves similar to a :class:`dict`, but all data is stored persistently in
    the associated HDF5 file on disk.

    Supported types include:

      * built-in types (int, float, str, bool, NoneType, array)
      * numpy arrays
      * pandas data frames (requires pandas and pytables)
      * mappings with values that are supported types

    Values can be accessed as attributes (``h5s.foo``) or via key index
    (``h5s['foo']``).

    Examples
    --------
    >>> from signac import H5Store
    >>> with H5Store('file.h5') as h5s:
    ...     h5s['foo'] = 'bar'
    ...     assert 'foo' in h5s
    ...     assert h5s.foo == 'bar'
    ...     assert h5s['foo'] == 'bar'
    >>>

    The H5Store can be used as a context manager to ensure that the underlying
    file is opened, however most built-in types (excluding arrays) can be read
    and stored without the need to _explicitly_ open the file. **To
    access arrays (reading or writing), the file must always be opened!**

    To open a file in read-only mode, use the :py:meth:`.open` method with ``mode='r'``:

    >>> with H5Store('file.h5').open(mode='r') as h5s:
    ...     pass
    >>>

    Parameters
    ----------
    filename : str
        The filename of the underlying HDF5 file.
    \*\*kwargs
        Additional keyword arguments to be forwarded to the ``h5py.File``
        constructor. See the documentation for the `h5py.File constructor
        <http://docs.h5py.org/en/latest/high/file.html#File>`_ for more
        information.

    """
    __slots__ = ["_filename", "_file", "_kwargs"]

    _thread_lock = RLock()

    def __init__(self, filename, **kwargs):
        if not (isinstance(filename, str) and len(filename) > 0):
            raise ValueError("H5Store filename must be a non-empty string.")
        self._filename = os.path.abspath(filename)
        self._file = None
        self._kwargs = kwargs

    @property
    def filename(self):
        """Return the H5Store filename."""
        return self._filename

    def __repr__(self):
        return "{}(filename={})".format(
            type(self).__name__, repr(os.path.relpath(self._filename))
        )

    def __str__(self):
        return "{}(filename={})".format(
            type(self).__name__, repr(os.path.basename(self._filename))
        )

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

    def _open(self, **kwargs):
        if self._file is not None:
            raise H5StoreAlreadyOpenError(self)
        import h5py

        # We use the default file parameters, which can optionally be overridden
        # by additional keyword arguments (kwargs). This option is intentionally
        # not exposed to the public API.
        parameters = dict(self._kwargs)
        parameters.update(kwargs)
        if parameters.get("mode", None) is None:
            parameters["mode"] = "a"

        self._thread_lock.acquire()
        try:
            self._file = h5py.File(self._filename, **parameters)
        except:  # noqa We need to release under **all** circumstances upon error!
            self._thread_lock.release()
            raise
        return self

    def open(self, mode=None):
        """Open the underlying HDF5 file.

        :param mode:
            The file open mode to use. Defaults to 'a' (append).
        :returns:
            This H5Store instance.
        """
        if mode is None:
            mode = self._kwargs.get("mode", "a")
        return self._open(mode=mode)

    def close(self):
        """Close the underlying HDF5 file."""
        locked = True
        try:
            self._file.close()
            self._file = None
        except AttributeError:
            locked = False
        finally:
            if locked:
                try:
                    self._thread_lock.release()
                except RuntimeError as error:
                    if "cannot release un-acquired lock" not in str(error):
                        raise

    @property
    def file(self):
        """Access the underlying instance of h5py.File.

        This property exposes the underlying ``h5py.File`` object enabling
        use of functions such as ``create_dataset()`` or ``requires_dataset()``.

        .. note::

            The store must be open to access this property!

        :returns:
            The ``h5py`` file-object that this store is operating on.
        :rtype:
            ``h5py.File``
        :raises H5StoreClosedError:
            When the store is closed at the time of accessing this property.
        """
        if self._file is None:
            raise H5StoreClosedError(self._filename)
        else:
            return self._file

    @property
    def mode(self):
        """Return the default opening mode of this H5Store."""
        return self._mode

    def flush(self):
        """Flush the underlying HDF5 file."""
        if self._file is None:
            raise H5StoreClosedError(self._filename)
        else:
            self._file.flush()

    def __getitem__(self, key):
        key = key if key.startswith("/") else "/" + key
        with _ensure_open(self):
            return _h5get(self, self._file, key)

    @staticmethod
    def _validate_key(key):
        """Emit a warning or raise an exception if key is invalid. Returns key."""
        if "." in key:
            raise InvalidKeyError("Keys for the H5Store may not contain dots ('.').")
        return key

    def __setitem__(self, key, value):
        with _ensure_open(self):
            _h5set(self, self._file, self._validate_key(key), value)
            return value

    def __delitem__(self, key):
        with _ensure_open(self):
            del self._file[key]

    def __getattr__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if name.startswith("__") or name in self.__slots__:
                raise
            try:
                return self.__getitem__(name)
            except KeyError as e:
                raise AttributeError(e)

    def __setattr__(self, key, value):
        if key.startswith("__") or key in self.__slots__:
            super().__setattr__(key, value)
        else:
            self.__setitem__(key, value)

    def __delattr__(self, key):
        if key.startswith("__") or key in self.__slots__:
            super().__delattr__(key)
        else:
            self.__delitem__(key)

    def setdefault(self, key, value):
        """Set a value for a key if that key is not already set."""
        super().setdefault(key, value)
        return self.__getitem__(key)

    def __iter__(self):
        with _ensure_open(self):
            yield from self._file.keys()

    def __len__(self):
        try:
            with _ensure_open(self, mode="r"):
                return len(self._file)
        except OSError as error:
            if f"errno = {errno.ENOENT}" in str(error):
                return 0  # file does not exist
            else:
                raise

    def __contains__(self, key):
        try:
            with _ensure_open(self, mode="r"):
                return key in self._file
        except OSError as error:
            if f"errno = {errno.ENOENT}" in str(error):
                return False  # file does not exist
            else:
                raise

    def clear(self):
        """Remove all data from this store.

        .. danger::
            All data will be removed, this action cannot be reversed!
        """
        with _ensure_open(self):
            self._file.clear()


class H5StoreManager(DictManager):
    """Helper class to manage multiple instances of :class:`~.H5Store` within a directory.

    Example (assuming that the 'stores/' directory exists):

    .. code-block:: python

        >>> stores = H5StoreManager('stores/')
        >>> stores.data
        <H5Store(filename=stores/data.h5)>
        >>> stores.data.foo = True
        >>> dict(stores.data)
        {'foo': True}

    :param prefix:
        The directory prefix shared by all stores managed by this class.
    """

    cls = H5Store  # type: ignore
    suffix = ".h5"  # type: ignore

    @staticmethod
    def _validate_key(key):
        """Emit a warning or raise an exception if key is invalid. Returns key."""
        if "." in key:
            raise InvalidKeyError(
                "Keys for the H5StoreManager may not contain dots ('.')."
            )
        return key
