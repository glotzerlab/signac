# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the buffering feature to  SyncedCollection API."""
import logging
from contextlib import contextmanager
from abc import abstractmethod

from .synced_collection import SyncedCollection
from .synced_list import SyncedList
from .syncedattrdict import SyncedAttrDict
from .errors import Error

logger = logging.getLogger(__name__)

_BUFFERED_MODE = 0
_BUFFERED_MODE_FORCE_WRITE = None
_BUFFER_BACKEND = list()

class BufferException(Error):
    """An exception occured in buffered mode."""


class BufferedFileError(BufferException):
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


def _store_backend_in_buffer(backend):
    """Store the backend data to the buffer"""
    if backend not in _BUFFER_BACKEND:
        _BUFFER_BACKEND.append(backend)


def flush_all():
    """Execute all deferred write operations.

    Raises
    ------
    BufferedFileError
    """
    logger.debug("Flushing buffer...")
    issues = dict()
    while _BUFFER_BACKEND:
        backend = _BUFFER_BACKEND.pop()
        backend_class = SyncedCollection.from_backend(backend)

        try:
            # try to sync the data to backend
            issue = backend_class._flush_buffer()
            issues.update(issue)
        except OSError as error:
            logger.error(str(error))
            issues[backend] = error
    if issues:
        raise BufferedFileError(issues)


def get_buffer_force_mode():
    """Return True if buffer force mode enabled."""
    return _BUFFERED_MODE_FORCE_WRITE


def in_buffered_mode():
    """Return True if in buffered read/write mode."""
    return _BUFFERED_MODE > 0


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
                assert not _BUFFER_BACKEND


class BufferedSyncedCollection(SyncedCollection):
    """Implement in-memory buffering, which is independent of backend and data-type."""

    def _sync_to_backend(self):
        if _BUFFERED_MODE > 0:
            # Storing in buffer
            _store_backend_in_buffer(self.backend)
            self._write_to_buffer(synced_data=False)
        else:
            # Saving to underlying backend:
            self._sync()

    def _load_from_backend(self):
        if _BUFFERED_MODE > 0:
            # Loading for buffer
            data = self._read_from_buffer()
            if data is None:
                # No data in buffer
                data = self._load()
                _store_backend_in_buffer(self.backend)
                self._write_to_buffer(data=data, synced_data=True)
            return data
        else:
            # load from underlying backend
            return self._load()

    # These methods are used to read the from cache while flushing buffer
    @abstractmethod
    def _write_to_buffer(self, data=None, synced_data=False):
        """Write the data to the buffer

        Parameters
        ----------
        data:
            Data write to the buffer.
        synced_data:
            True if the data is synchronized with backend, otherwise False.
        """
        pass

    @classmethod
    @abstractmethod
    def _flush_buffer(cls):
        """Flush the data from buffer

        Returns
        -------
        issues: dict
            Dictionary of names that caused issues during the flushing
            mapped to possible reason of error.
        """
        pass

    @abstractmethod
    def _read_from_buffer(self):
        """Read the data from the buffer."""
        pass

    @contextmanager
    def buffered(self):
        """Context manager for buffering read and write operations.

        This context manager activates the "buffered" mode, which
        means that all read operations are cached, and all write operations
        are deferred until the buffered mode is deactivated.

        Yields
        ------
        buffered_collection : object
            Buffered SyncedCollection object of corresponding base type.
        """
        buffered_collection = self.from_base(data=self, backend='buffered', parent=self)
        try:
            yield buffered_collection
        finally:
            buffered_collection.flush()


class BufferedCollection(SyncedCollection):
    """Implement buffered backend.

    Saves all changes in memory but does not write them to disk
    until :meth:`~.flush` is called. This backend is used to provide
    buffering for an instance of :class:`SyncedCollection`.
    """

    backend = 'buffered'  # type: ignore

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self._parent is None:
            ValueError("Parent argument can't be None.")

    # overwriting load and sync methods
    def load(self):
        pass

    def sync(self):
        pass

    # defining abstractmethods
    def _load(self):
        pass

    def _sync(self):
        pass

    def flush(self):
        """Save buffered changes to the underlying file."""
        self._parent.reset(self.to_base())


class BufferedSyncedAttrDict(BufferedCollection, SyncedAttrDict):
    pass


class BufferedSyncedList(BufferedCollection, SyncedList):
    pass


SyncedCollection.register(BufferedCollection, BufferedSyncedAttrDict, BufferedSyncedList)
