# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the buffering feature to  SyncedCollection API."""
import os
import errno
import logging
from contextlib import contextmanager
from abc import abstractmethod

from .synced_collection import SyncedCollection
from .caching import CachedSyncedCollection
from .synced_list import SyncedList
from .syncedattrdict import SyncedAttrDict
from .errors import Error

logger = logging.getLogger(__name__)

_BUFFERED_MODE = 0
_BUFFERED_MODE_FORCE_WRITE = None
_BUFFER = dict()
_FILEMETA = dict()


class BufferException(Error):
    """An exception occured in buffered mode."""


class BufferedFileError(BufferException):
    """Raised when an error occured while flushing one or more buffered files.

    Attribute
    ---------
    files:
        A dictionary of files that caused issues during the flush operation,
        mapped to a possible reason for the issue or None in case that it
        cannot be determined.
    """

    def __init__(self, files):
        self.files = files

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.files)


def _get_filemetadata(filename):
    try:
        metadata = os.stat(filename)
        return metadata.st_size, metadata.st_mtime
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise


def _store_in_buffer(_id, backend, backend_kwargs, cache, metadata=None):
    """Store the backend data to the buffer"""
    assert _BUFFERED_MODE > 0
    if _id in buffer:
        _, _, stored_cache= _BUFFER[_id]
        if not stored_cache is cache:
            raise BufferException(f'Found multiple cache linked to {_id}')
    else:
        _BUFFER[_id] = (backend, backend_kwargs, cache)
        # if force mode we ignore metadata
        if not _BUFFERED_MODE_FORCE_WRITE:
            _FILEMETA[_id] = metadata


def flush_all():
    """Execute all deferred write operations.

    Raises
    ------
    BufferedFileError
    """
    logger.debug("Flushing buffer...")
    issues = dict()
    while _BUFFER:
        _id, (backend, backend_kwargs, cache) = _BUFFER.popitem()
        # metadata is not stored in force mode 
        metadata = None if _BUFFERED_MODE_FORCE_WRITE else _FILEMETA.pop(_id)

        backend_class = SyncedCollection.from_backend(backend)

        try:
            # try to sync the data to backend
            metadata_check = backend_class._sync_from_buffer(_id, backend_kwargs, cache, metadata)
            if not metadata_check:
                issues[_id] = 'File appears to have been externally modified.'
        except OSError as error:
            logger.error(str(error))
            issues[_id] = error
    if issues:
        raise BufferedFileError(issues)


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
                assert not _BUFFER
                assert not _FILEMETA
                _BUFFERED_MODE_FORCE_WRITE = None


class BufferedSyncedCollection(CachedSyncedCollection):
    """Implement in-memory buffering, which is independent of backend and data-type."""

    def _sync_to_backend(self):
        if _BUFFERED_MODE > 0:
            # Storing in buffer
            self._write_to_buffer()
        else:
            # Saving to underlying backend:
            self._sync()

    # These methods are used to read the from cache while flushing buffer
    @abstractmethod
    def _write_to_buffer(self):
        """Write the data from buffer"""
        pass

    @classmethod
    @abstractmethod
    def _sync_from_buffer(cls, id, backend_kwargs, cache, metadata=None):
        """Sync the data stored in buffer

        Returns
        -------
        metacheck: bool
            False if metadata check passes and True otherwise"""
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
            self.refresh_cache()


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
        self._parent._sync(self.to_base())


class BufferedSyncedAttrDict(BufferedCollection, SyncedAttrDict):
    pass


class BufferedSyncedList(BufferedCollection, SyncedList):
    pass


SyncedCollection.register(BufferedCollection, BufferedSyncedAttrDict, BufferedSyncedList)
