# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the buffering feature to  SyncedCollection API."""
import os
import errno
import sys
import logging
from contextlib import contextmanager

from .synced_collection import SyncedCollection
from .synced_list import SyncedList
from .syncedattrdict import SyncedAttrDict
from .errors import Error

logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE = 32 * 2**20    # 32 MB

_BUFFERED_MODE = 0
_BUFFERED_MODE_FORCE_WRITE = None
_BUFFER_SIZE = None
_BUFFER = dict()
_BACKEND_DATA = dict()
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


def _store_in_buffer(id, blob, backend_kwargs, metadata=None, synced_data=False):
    assert _BUFFERED_MODE > 0
    blob_size = sys.getsizeof(blob)
    buffer_load = get_buffer_load()
    if _BUFFER_SIZE > 0:
        if blob_size > _BUFFER_SIZE:
            return False
        elif blob_size + buffer_load > _BUFFER_SIZE:
            logger.debug("Buffer overflow, flushing...")
            flush_all()
    _BUFFER[id] = blob
    _BACKEND_DATA[id] = (backend_kwargs, synced_data)
    if not _BUFFERED_MODE_FORCE_WRITE:
        _FILEMETA[id] = metadata
    return True


def flush_all():
    """Execute all deferred JSONDict write operations."""
    logger.debug("Flushing buffer...")
    issues = dict()
    while _BUFFER:
        id, blob = _BUFFER.popitem()
        backend_kwargs, synced_data = _BACKEND_DATA.pop(id)
        if not _BUFFERED_MODE_FORCE_WRITE:
            meta = _FILEMETA.pop(id)
            obj = SyncedCollection.from_base(data=blob, no_sync=True, **backend_kwargs)
            if obj._get_metadata() != meta:
                issues[id] = 'File appears to have been externally modified.'
                continue
        if not synced_data:
            try:
                obj._sync(data=blob)
            except OSError as error:
                logger.error(str(error))
                issues[id] = error
    if issues:
        raise BufferedFileError(issues)


def get_buffer_size():
    """Return the current maximum size of the read/write buffer."""
    return _BUFFER_SIZE


def get_buffer_load():
    """Return the current actual size of the read/write buffer."""
    return sum((sys.getsizeof(x) for x in _BUFFER.values()))


def in_buffered_mode():
    """Return true if in buffered read/write mode."""
    return _BUFFERED_MODE > 0


@contextmanager
def buffer_reads_writes(buffer_size=DEFAULT_BUFFER_SIZE, force_write=False):
    """Enter a global buffer mode for all JSONDict instances.

    All future write operations are written to the buffer, read
    operations are performed from the buffer whenever possible.

    All write operations are deferred until the flush_all() function
    is called, the buffer overflows, or upon exiting the buffer mode.

    This context may be entered multiple times, however the buffer size
    can only be set *once*. Any subsequent specifications of the buffer
    size are ignored.

    Parameters
    ----------

    buffer_size: int
        Specify the maximum size of the read/write buffer. Defaults
        to DEFAULT_BUFFER_SIZE. A negative number indicates to not
        restrict the buffer size.
    force_mode: bool
        If true, overwrites the metadata check.

    Raises
    ------
    BufferException
    """
    global _BUFFERED_MODE
    global _BUFFERED_MODE_FORCE_WRITE
    global _BUFFER_SIZE
    assert _BUFFERED_MODE >= 0

    # Basic type check (to prevent common user error)
    if not isinstance(buffer_size, int) or \
            buffer_size is True or buffer_size is False:    # explicit check against boolean
        raise TypeError("The buffer size must be an integer!")

    # Can't enter force write mode, if already in non-force write mode:
    if _BUFFERED_MODE_FORCE_WRITE is not None and (force_write and not _BUFFERED_MODE_FORCE_WRITE):
        raise BufferException(
            "Unable to enter buffered mode with force write enabled, because "
            "we are already in buffered mode with force write disabled.")

    # Check whether we can adjust the buffer size and warn otherwise:
    if _BUFFER_SIZE is not None and _BUFFER_SIZE != buffer_size:
        raise BufferException("Buffer size already set, unable to change its size!")

    _BUFFER_SIZE = buffer_size
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
                assert not _BACKEND_DATA
                assert not _FILEMETA
                _BUFFER_SIZE = None
                _BUFFERED_MODE_FORCE_WRITE = None


class BufferedSyncedCollection(SyncedCollection):
    """Implement in-memory buffering, which is independent of back-end and data-type."""

    def load(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                if _BUFFERED_MODE > 0:
                    if self._id in _BUFFER:
                        # Load from buffer:
                        blob = _BUFFER[self._id]
                    else:
                        # Load from disk and store in buffer
                        blob = self._load()

                        _store_in_buffer(self._id, blob, self.backend_kwargs,
                                         metadata=self._get_metadata(), synced_data=True)
                else:
                    # Just load from disk
                    blob = self._load()
                # Reset the instance
                with self._suspend_sync():
                    self._update(blob)
            else:
                self._parent.load()

    def sync(self):
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                with self._suspend_sync():
                    data = self.to_base()
                if _BUFFERED_MODE > 0:
                    # Storing in buffer
                    _store_in_buffer(
                        self._id, data, self.backend_kwargs, metadata=self._get_metadata())
                else:
                    # Saving to disk:
                    self._sync(data)
            else:
                self._parent.sync()


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
