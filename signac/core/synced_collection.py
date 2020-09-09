# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the SyncedCollection class.

SyncedCollection encapsulates the synchronization of different data-structures.
These features are implemented in different subclasses which enable us to use a
backend with different data-structures or vice-versa. It declares as abstract
methods the methods that must be implemented by any subclass to match the API.
"""
import inspect
import logging

from typing import List, Callable
from contextlib import contextmanager
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Collection
from typing import List, DefaultDict, Any

from .errors import Error

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False

logger = logging.getLogger(__name__)

_BUFFERED_MODE = 0
_BUFFERED_MODE_FORCE_WRITE = None
_BUFFERED_BACKNDS: List[Any] = list()


class BufferException(Error):
    """An exception occured in buffered mode."""


class BufferedError(BufferException):
    """Raised when an error occured while flushing one or more buffered files.

    Attribute
    ---------
    names:
        A dictionary of names that caused issues during the flush operation,
        mapped to a possible reason for the issue or None in case that it
        cannot be determined.
    """

    def __init__(self, files):
        self.files = files

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.files)


def flush_all():
    """Execute all deferred write operations.

    Raises
    ------
    BufferedFileError
    """
    logger.debug("Flushing buffer...")
    issues = dict()
    for backend in _BUFFERED_BACKNDS:
        try:
            # try to sync the data to backend
            issue = backend._flush_buffer()
            issues.update(issue)
        except OSError as error:
            logger.error(str(error))
            issues[backend] = error
    if issues:
        raise BufferedError(issues)


def _get_buffer_force_mode():
    """Return True if buffer force mode enabled."""
    return _BUFFERED_MODE_FORCE_WRITE


def _in_buffered_mode():
    """Return True if in buffered read/write mode."""
    return _BUFFERED_MODE > 0


def _register_buffered_backend(backend):
    """Register the backend.

    The registry is used in the :meth:`flush_all`.
    Every backend to register should have ``_flush_buffer`` method.
    """
    _BUFFERED_BACKNDS.append(backend)


@contextmanager
def buffer_reads_writes(force_write=False):
    """Enter a global buffer mode for all SyncedCollection instances.

    All future write operations are written to the buffer, read
    operations are performed from the buffer whenever possible.

    All write operations are deferred until the flush_all() function
    is called, the buffer overflows, or upon exiting the buffer mode.

    Parameters
    ----------
    force_write: bool
        If true, overwrites the metadata check.

    Raises
    ------
    BufferException
    """
    global _BUFFERED_MODE
    global _BUFFERED_MODE_FORCE_WRITE
    assert _BUFFERED_MODE >= 0

    # Can't switch force modes.
    if _BUFFERED_MODE_FORCE_WRITE is not None and (force_write != _BUFFERED_MODE_FORCE_WRITE):
        raise BufferException(
            "Unable to enter buffered mode with force write enabled, because "
            "we are already in buffered mode with force write disabled and vise-versa.")

    _BUFFERED_MODE_FORCE_WRITE = force_write
    _BUFFERED_MODE += 1
    try:
        yield
    finally:
        _BUFFERED_MODE -= 1
        if _BUFFERED_MODE == 0:
            try:
                flush_all()
            finally:
                _BUFFERED_MODE_FORCE_WRITE = None


class SyncedCollection(Collection):
    """The base synced collection represents a collection that is synced with a backend.

    The class is intended for use as an ABC. The SyncedCollection is a
    :class:`~collections.abc.Collection` where all data is stored persistently
    in the underlying backend. The backend name wil be same as the module name.
    """

    _backend = None
    registry: DefaultDict[str, List[Any]] = defaultdict(list)
    _validators: List[Callable] = []

    def __init__(self, name=None, parent=None):
        self._data = None
        self._parent = parent
        self._name = name
        self._supports_buffering = False
        self._buffered = 0
        self._suspend_sync_ = 0
        if (name is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or name must be None, but not both.")

    @classmethod
    def __init_subclass__(cls):
        """Add  ``_validator`` attribute to every subclass.

        Subclasses contain a list of validators that are applied to collection input data.
        Every subclass must have a separate list so that distinct sets of validators can
        be registered to each of them.
        """
        cls._validators = []

    @classmethod
    def register(cls, *args):
        r"""Register the synced data structures.

        The registry is used by :meth:`from_base` to determine the appropriate subclass
        of :class:`SyncedCollection` that should be constructed from a particular object.
        This functionality is necessary for converting nested data structures because
        given, for instance, a dict of lists, it must be possible to map the nested lists to
        the appropriate subclass of :class:`SyncedList` corresponding to the desired
        backend.

        Parameters
        ----------
        \*args
            Classes to register
        """
        for base_cls in args:
            cls.registry[base_cls._backend].append(base_cls)

    @property
    def validators(self):
        """Return the list of validators applied to the instance."""
        validators = []
        # Classes inherit the validators of their parent classes.
        for base_cls in type(self).__mro__:
            if hasattr(base_cls, '_validators'):
                validators.extend([v for v in base_cls._validators if v not in validators])
        return validators

    @classmethod
    def add_validator(cls, *args):
        r"""Register validator.

        Parameters
        ----------
        \*args
            Validator(s) to register.
        """
        cls._validators.extend([v for v in args if v not in cls._validators])

    @classmethod
    def from_base(cls, data, backend=None, **kwargs):
        r"""Dynamically resolve the type of object to the corresponding synced collection.

        Parameters
        ----------
        data : any
            Data to be converted from base class.
        backend: str
            Name of backend for synchronization. Default to backend of class.
        \*\*kwargs:
            Kwargs passed to instance of synced collection.

        Returns
        -------
        data : object
            Synced object of corresponding base type.
        """
        backend = cls._backend if backend is None else backend
        if backend is None:
            raise ValueError("No backend found.")
        for base_cls in cls.registry[backend]:
            if base_cls.is_base_type(data):
                return base_cls(data=data, **kwargs)
        if NUMPY:
            if isinstance(data, numpy.number):
                return data.item()
        return data

    @abstractmethod
    def to_base(self):
        """Dynamically resolve the synced collection to the corresponding base type."""
        pass

    @contextmanager
    def _suspend_sync(self):
        """Prepare context where load and sync are suspended."""
        self._suspend_sync_ += 1
        yield
        self._suspend_sync_ -= 1

    @classmethod
    @abstractmethod
    def is_base_type(cls, data):
        """Check whether data is of the same base type (such as list or dict) as this class."""
        pass

    @abstractmethod
    def _load(self):
        """Load data from underlying backend."""
        pass

    @abstractmethod
    def _sync(self, data):
        """Write data to underlying backend."""
        pass

    def sync(self):
        """Synchronize the data with the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                self._sync()
            else:
                self._parent.sync()

    def load(self):
        """Load the data from the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                data = self._load()
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent.load()

    @contextmanager
    def buffered(self):
        """Context manager for buffering read and write operations.

        This context manager activates the "buffered" mode, which
        means that all read operations are cached, and all write operations
        are deferred until the buffered mode is deactivated.
        """
        if self._supports_buffering:
            self._buffered += 1
            try:
                yield
            finally:
                self._buffered -= 1
                if self._buffered == 0:
                    self.flush()
        else:
            raise BufferException(f"{type(self).__name__} does not support buffering.")

    def _validate(self, data):
        """Validate the input data."""
        for validator in self.validators:
            validator(data)

    # The following methods share a common implementation for
    # all data structures and regardless of backend.

    def __getitem__(self, key):
        self.load()
        return self._data[key]

    def __delitem__(self, item):
        del self._data[item]
        self.sync()

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
        self.load()
        return repr(self._data)

    def __str__(self):
        self.load()
        return str(self._data)
