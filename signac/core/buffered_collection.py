# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the buffering feature to  SyncedCollection API."""
import os
import errno
import sys
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
_BUFFER = dict()
_FILEMETA = dict()


class BufferException(Error):
    """An exception occured in buffered mode."""

    pass


class BufferedFileError(BufferException):
    """Raised when an error occured while flushing one or more buffered files.

    .. attribute:: files

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
        return os.path.getsize(filename), os.path.getmtime(filename)
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise


def _store_in_buffer(id, backend_kwargs, cache, metadata=None):
    assert _BUFFERED_MODE > 0
    _BUFFER[id] = (backend_kwargs, cache)
    if not _BUFFERED_MODE_FORCE_WRITE:
        _FILEMETA[id] = metadata


def flush_all():
    """Execute all deferred JSONDict write operations."""
    logger.debug("Flushing buffer...")
    issues = dict()
    while _BUFFER:
        id, (backend_kwargs, cache) = _BUFFER.popitem()
        if not _BUFFERED_MODE_FORCE_WRITE:
            meta = _FILEMETA.pop(id)
            backend_class = SyncedCollection.from_backend(backend_kwargs['backend'])
            data = backend_class._read_from_cache(id, cache)
            obj = SyncedCollection.from_base(data, cache=cache, no_sync=True, **backend_kwargs)
            if obj._get_metadata() != meta:
                issues[id] = 'File appears to have been externally modified.'
                continue
            try:
                obj._sync()
            except OSError as error:
                logger.error(str(error))
                issues[id] = error
    if issues:
        raise BufferedFileError(issues)


def in_buffered_mode():
    """Return true if in buffered read/write mode."""
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

    force_mode: bool
        If true, overwrites the metadata check.

    Raises
    ------
    BufferException
    """
    global _BUFFERED_MODE
    global _BUFFERED_MODE_FORCE_WRITE
    assert _BUFFERED_MODE >= 0

    # Can't enter force write mode, if already in non-force write mode:
    if _BUFFERED_MODE_FORCE_WRITE is not None and (force_write and not _BUFFERED_MODE_FORCE_WRITE):
        raise BufferException(
            "Unable to enter buffered mode with force write enabled, because "
            "we are already in buffered mode with force write disabled.")

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


class BufferedSyncedCollection(SyncedCollection):
    """Implement in-memory buffering, which is independent of back-end and data-type."""

    def _sync_to_backend(self):
        if _BUFFERED_MODE > 0:
            # Storing in buffer
            _store_in_buffer(
                self._id, self.backend_kwargs, self._cache, metadata=self._get_metadata())
        else:
            # Saving to underlying backend:
            self._sync()

    @classmethod
    @abstractmethod
    def _write_to_cache(cls, id, data, cache):
        pass

    @classmethod
    @abstractmethod
    def _read_from_cache(cls, id, cache):
        pass


class BufferedCollection(SyncedCollection):
    """Implement buffered backend.

    Saves all changes in memory but does not write them to disk
    until :meth:`~.flush` is called. This backend is used to provide
    buffering for a instance of `SyncedCollection`.
    """

    backend = 'buffered'  # type: ignore

    def load(self):
        pass

    def sync(self):
        pass

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


SyncedCollection.register(BufferedSyncedAttrDict, BufferedSyncedList)
