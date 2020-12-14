# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the BufferedCollection class.

The BufferedCollection extends the abstract SyncedCollection to support
transparent buffering of the synchronization process. This feature allows
client code to indicate to the collection when it is safe to buffer reads and
writes, which usually means guaranteeing that the synchronization destination
(e.g. an underlying file or database entry) will not be modified by other
processes concurrently with the set of operations within the buffered block.
Judicious use of buffering can dramatically speed up code paths that might
otherwise involve, for instance, heavy I/O. The specific buffering mechanism
must be implemented by each back end since it depends on the nature of the
underlying data format.
"""
import logging
from typing import List, Any

from contextlib import contextmanager

from .errors import BufferedError
from .synced_collection import SyncedCollection

logger = logging.getLogger(__name__)

_BUFFERED_MODE = 0
_BUFFERED_BACKENDS: List[Any] = list()


@contextmanager
def buffer_reads_writes():
    """Enter a global buffer mode for all BufferedCollection instances.

    All future write operations are written to the buffer, read
    operations are performed from the buffer whenever possible.

    All write operations are deferred until the flush_all() function
    is called, the buffer overflows, or upon exiting the buffer mode.

    Raises
    ------
    BufferException
    """
    global _BUFFERED_MODE
    assert _BUFFERED_MODE >= 0

    _BUFFERED_MODE += 1
    try:
        yield
    finally:
        _BUFFERED_MODE -= 1
        if _BUFFERED_MODE == 0:
            _flush_all()


def _flush_all():
    """Execute all deferred write operations.

    Raises
    ------
    BufferedFileError
    """
    global _BUFFERED_BACKENDS
    logger.debug("Flushing buffer...")
    issues = {}
    for backend in _BUFFERED_BACKENDS:
        try:
            # try to sync the data to backend
            issue = backend._flush_buffer()
            issues.update(issue)
        except OSError as error:
            logger.error(str(error))
            issues[backend] = error
    if issues:
        raise BufferedError(issues)


class BufferedCollection(SyncedCollection):
    """Adds buffering capabilities to the SyncedCollection.

    *The default behavior is no buffering.* The class simply defines an
    appropriate interface for buffering behavior so that client code can rely
    on these methods existing, e.g. to be able to do things like
    `with collection.buffered...`. However, in the default case the result of
    this will be a no-op.

    In addition to the methods shown here, we may also require methods to flush
    a specific entry out of the buffer (e.g. _flush(self, item)) which has been
    implemented in the current version of the PR. I've left out such details
    since they're not crucial to getting the right structure here.
    """

    def __init__(self, *args, **kwargs):
        # This attribute _must_ be defined prior to calling the superclass
        # constructors in order to enable subclasses to override setattr and
        # getattr in nontrivial ways. In particular, if setattr and getattr
        # need to access the synced data, they may call sync and load, which
        # depend on this parameter existing and could otherwise end up in an
        # infinite recursion.
        self._buffered = 0
        super().__init__(*args, **kwargs)

    @classmethod
    def __init_subclass__(cls):
        """Register subclasses for the purpose of global buffering.

        Each subclass has its own means of buffering and must be flushed.
        """
        super().__init_subclass__()
        global _BUFFERED_BACKENDS
        _BUFFERED_BACKENDS.append(cls)

    # We would like to be able to override `_sync` and `_load` rather than
    # `sync` and `load` to avoid having to replicate the "parent" logic.
    # However, the underscore methods are the hooks for back end-specific
    # collections to define how synchronization with the back end behaves. What
    # we need to control in this class is when the synchronization to the back
    # end actually occurs and when data is simply written to a buffer, and the
    # only way to unambiguously specific the methods to call is by overriding
    # `sync` and `load`.
    def sync(self):
        """Synchronize data with the backend but buffer if needed.

        This method is identical to the SyncedCollection implementation for
        `sync` except that it determines whether data is actually synchronized
        or instead written to a temporary buffer based on the buffering mode.
        """
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                if self._is_buffered:
                    self._sync_buffer()
                else:
                    self._sync()
            else:
                self._parent.sync()

    def load(self):
        """Load data from the backend but buffer if needed.

        This method is identical to the SyncedCollection implementation for
        `load` except that it determines whether data is actually synchronized
        or instead read from a temporary buffer based on the buffering mode.
        """
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                if self._is_buffered:
                    data = self._load_buffer()
                else:
                    data = self._load()
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent.load()

    def _sync_buffer(self):
        """Store data in buffer.

        There's little benefit to providing a default means of buffering across
        all back ends since the process is heavily dependent on the back end
        data store, so the default behavior is to just sync normally.
        """
        self._sync()

    def _load_buffer(self):
        """Store data in buffer.

        There's little benefit to providing a default means of buffering across
        all back ends since the process is heavily dependent on the back end
        data store, so the default behavior is to just load normally.
        """
        self._load()

    @contextmanager
    def buffered(self):
        """Enter buffered mode."""
        self._buffered += 1
        try:
            yield
        finally:
            self._buffered -= 1
            # TODO: Decide whether it makes the most sense for this check to
            # live here (and anywhere _flush is called) or inside _flush
            # itself. In large part this will depend on how it interacts with
            # global buffering.
            if not self._is_buffered:
                self._flush()

    @property
    def _is_buffered(self):
        """Check if we should write to the buffer or not."""
        global _BUFFERED_MODE
        return self._buffered + _BUFFERED_MODE > 0

    def _flush(self):
        """Flush data associated with this instance from the buffer."""
        pass

    @classmethod
    def _flush_buffer(self):
        """Flush all data in the buffer."""
        pass
