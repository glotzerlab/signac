# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Defines a buffering protocol for SyncedCollection objects.

Depending on the choice of backend, synchronization may be an expensive process.
In that case, it can be helpful to allow many in-memory modifications to occur
before any synchronization is attempted. Since many collections could be pointing
to the same underlying resource, maintaining proper data coherency across different
instances requires careful consideration of how the data is stored. The appropriate
buffering methods can differ for different backends; as a result, the basic
interface simply lays out the API for buffering and leaves implementation
details for specific backends to handle. Judicious use of buffering can
dramatically speed up code paths that might otherwise involve, for instance,
heavy I/O. The specific buffering mechanism must be implemented by each backend
since it depends on the nature of the underlying data format.

All buffered collections expose a local context manager for buffering. In addition,
this module exposes a global context manager :func:`buffer_all` that
indicates to all buffered collections irrespective of data type or backend that
they should enter buffered mode. These context managers may be nested freely, and
buffer flushes will occur when all such managers have been exited.

.. code-block::

    with collection1.buffered:
        with buffer_all:
            collection2['foo'] = 1
            collection1['bar'] = 1
            # collection2 will flush when this context exits.

        # This operation will write straight to the backend.
        collection2['bar'] = 2

        # collection1 will flush when this context exits.
"""

import logging
from inspect import isabstract
from typing import Any, List

from .errors import BufferedError
from .synced_collection import SyncedCollection
from .utils import _CounterFuncContext

logger = logging.getLogger(__name__)


class _GlobalBufferedMode:
    def __enter__(self):
        BufferedCollection._BUFFERED_MODE += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        BufferedCollection._BUFFERED_MODE -= 1
        if BufferedCollection._BUFFERED_MODE == 0:
            BufferedCollection._flush_all_backends()


class BufferedCollection(SyncedCollection):
    """A :class:`SyncedCollection` defining an interface for buffering.

    **The default behavior of this class is not to buffer.** This class simply
    defines an appropriate interface for buffering behavior so that client code
    can rely on these methods existing, e.g. to be able to do things like `with
    collection.buffered...`. This feature allows client code to indicate to the
    collection when it is safe to buffer reads and writes, which usually means
    guaranteeing that the synchronization destination (e.g. an underlying file
    or database entry) will not be modified by other processes concurrently
    with the set of operations within the buffered block. However, in the
    default case the result of this will be a no-op and all data will be
    immediately synchronized with the backend.

    The BufferedCollection overrides the :meth:`SyncedCollection._load` and
    :meth:`SyncedCollection._save` methods to check whether buffering is enabled
    or not. If not, the behavior is identical to the parent class. When in buffered
    mode, however, the BufferedCollection introduces two additional hooks that
    can be overridden by subclasses to control how the collection behaves while buffered:

        - :meth:`~._load_from_buffer`: Loads data while in buffered mode and returns
          it in an object satisfying :meth:`~.is_base_type`. The default behavior
          is to simply call :meth:`~SyncedCollection._load_from_resource`
        - :meth:`~._save_to_buffer`: Stores data while in buffered mode. The default behavior
          is to simply call :meth:`~SyncedCollection._save_to_resource`

    **Thread safety**

    Whether or not buffering is thread safe depends on the buffering method used. In
    general, both the buffering logic and the standard data read/write logic (i.e.
    operations like `__setitem__`) must be thread safe for the resulting collection
    type to be thread safe.

    """

    _BUFFERED_MODE = 0
    _BUFFERED_BACKENDS: List[Any] = []

    def __init__(self, *args, **kwargs):
        # The `_buffered` attribute _must_ be defined prior to calling the
        # superclass constructors in order to enable subclasses to override
        # setattr and getattr in nontrivial ways. In particular, if setattr and
        # getattr need to access the synced data, they may call sync and load,
        # which depend on this parameter existing and could otherwise end up in
        # an infinite recursion.
        self.buffered = _CounterFuncContext(self._flush)
        super().__init__(*args, **kwargs)

    @classmethod
    def __init_subclass__(cls):
        """Register subclasses for the purpose of global buffering.

        Each subclass has its own means of buffering and must be flushed.
        """
        super().__init_subclass__()
        if not isabstract(cls):
            BufferedCollection._BUFFERED_BACKENDS.append(cls)

    buffer_all = _GlobalBufferedMode()
    """Enter a globally buffer context for all BufferedCollection instances.

    All future operations use the buffer whenever possible. Write operations
    are deferred until the context is exited, at which point all buffered
    backends will flush their buffers. Individual backends may flush their
    buffers within this context if the implementation requires it; this context
    manager represents a promise to buffer whenever possible, but does not
    guarantee that no writes will occur under all circumstances.
    """

    @staticmethod
    def _flush_all_backends():
        """Execute all deferred write operations.

        Raises
        ------
        BufferedError
            If there are any issues with flushing any backend.

        """
        logger.debug("Flushing buffer...")
        issues = {}
        for backend in BufferedCollection._BUFFERED_BACKENDS:
            try:
                # try to sync the data to backend
                issue = backend._flush_buffer()
                issues.update(issue)
            except OSError as error:
                logger.error(str(error))
                issues[backend] = error
        if issues:
            raise BufferedError(issues)

    def _save(self):
        """Synchronize data with the backend but buffer if needed.

        This method is identical to the SyncedCollection implementation for
        `sync` except that it determines whether data is actually synchronized
        or instead written to a temporary buffer based on the buffering mode.
        """
        if not self._suspend_sync:
            if self._parent is None:
                if self._is_buffered:
                    self._save_to_buffer()
                else:
                    self._save_to_resource()
            else:
                self._parent._save()

    def _load(self):
        """Load data from the backend but buffer if needed.

        This method is identical to the SyncedCollection implementation for
        `load` except that it determines whether data is actually synchronized
        or instead read from a temporary buffer based on the buffering mode.
        """
        if not self._suspend_sync:
            if self._parent is None:
                if self._is_buffered:
                    data = self._load_from_buffer()
                else:
                    data = self._load_from_resource()
                with self._suspend_sync:
                    self._update(data)
            else:
                self._parent._load()

    def _save_to_buffer(self):
        """Store data in buffer.

        By default, this method simply calls :meth:`~._save_to_resource`. Subclasses
        must implement specific buffering strategies.
        """
        self._save_to_resource()

    def _load_from_buffer(self):
        """Store data in buffer.

        By default, this method simply calls :meth:`~._load_from_resource`. Subclasses
        must implement specific buffering strategies.

        Returns
        -------
        Collection
            An equivalent unsynced collection satisfying :meth:`is_base_type` that
            contains the buffered data. By default, the buffered data is just the
            data in the resource.

        """
        self._load_from_resource()

    @property
    def _is_buffered(self):
        """Check if we should write to the buffer or not."""
        return self.buffered or (BufferedCollection._BUFFERED_MODE > 0)

    def _flush(self):
        """Flush data associated with this instance from the buffer."""
        pass

    @classmethod
    def _flush_buffer(self):
        """Flush all data in this class's buffer."""
        pass


def buffer_all():
    """Enter a globally buffer context for all BufferedCollection instances.

    All future operations use the buffer whenever possible. Write operations
    are deferred until the context is exited, at which point all buffered
    backends will flush their buffers. Individual backends may flush their
    buffers within this context if the implementation requires it; this context
    manager represents a promise to buffer whenever possible, but does not
    guarantee that no writes will occur under all circumstances.

    This method is a trivial alias for :meth:`~.BufferedCollection.buffer_all`
    """
    return BufferedCollection.buffer_all
