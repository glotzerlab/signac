# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Defines a buffering protocol for :class:`~.SyncedCollection` objects.

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

All buffered collections expose a local context manager for buffering. In
addition, each backend exposes a context manager
:meth:`BufferedCollection.buffer_backend` that indicates to all buffered
collections of that backend that they should enter buffered mode. These context
managers may be nested freely, and buffer flushes will occur when all such
managers have been exited.

.. code-block:: python

    with collection1.buffered:
        with type(collection1).buffer_backend:
            collection2['foo'] = 1
            collection1['bar'] = 1
            # collection2 will flush when this context exits.

        # This operation will write straight to the backend.
        collection2['bar'] = 2

        # collection1 will flush when this context exits.
"""

import logging
from inspect import isabstract

from .. import SyncedCollection
from ..utils import _CounterFuncContext

logger = logging.getLogger(__name__)


class BufferedCollection(SyncedCollection):
    """A :class:`~.SyncedCollection` defining an interface for buffering.

    **The default behavior of this class is not to buffer.** This class simply
    defines an appropriate interface for buffering behavior so that client code
    can rely on these methods existing, e.g. to be able to do things like ``with
    collection.buffered...``. This feature allows client code to indicate to the
    collection when it is safe to buffer reads and writes, which usually means
    guaranteeing that the synchronization destination (e.g. an underlying file
    or database entry) will not be modified by other processes concurrently
    with the set of operations within the buffered block. However, in the
    default case the result of this will be a no-op and all data will be
    immediately synchronized with the backend.

    The BufferedCollection overrides the :meth:`~._load` and
    :meth:`~._save` methods to check whether buffering is enabled
    or not. If not, the behavior is identical to the parent class. When in buffered
    mode, however, the BufferedCollection introduces two additional hooks that
    can be overridden by subclasses to control how the collection behaves while buffered:

        - :meth:`~._load_from_buffer`: Loads data while in buffered mode and returns
          it in an object satisfying
          :meth:`~signac.synced_collections.data_types.synced_collection.SyncedCollection.is_base_type`.
          The default behavior is to simply call
          :meth:`~._load_from_resource`.
        - :meth:`~._save_to_buffer`: Stores data while in buffered mode. The default behavior
          is to simply call
          :meth:`~._save_to_resource`.

    **Thread safety**

    Whether or not buffering is thread safe depends on the buffering method used. In
    general, both the buffering logic and the data type operations must be
    thread safe for the resulting collection type to be thread safe.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffered = _CounterFuncContext(self._flush)

    @classmethod
    def __init_subclass__(cls):
        """Register subclasses for the purpose of global buffering.

        Each subclass has its own means of buffering and must be flushed.
        """
        super().__init_subclass__()
        if not isabstract(cls):
            cls._buffer_context = _CounterFuncContext(cls._flush_buffer)

    @classmethod
    def buffer_backend(cls, *args, **kwargs):
        """Enter context to buffer all operations for this backend."""
        return cls._buffer_context

    @classmethod
    def backend_is_buffered(cls):
        """Check if this backend is currently buffered."""
        return bool(cls._buffer_context)

    def _save(self):
        """Synchronize data with the backend but buffer if needed.

        This method is identical to the SyncedCollection implementation for
        `sync` except that it determines whether data is actually synchronized
        or instead written to a temporary buffer based on the buffering mode.
        """
        if not self._suspend_sync:
            if self._root is None:
                if self._is_buffered:
                    self._save_to_buffer()
                else:
                    self._save_to_resource()
            else:
                self._root._save()

    def _load(self):
        """Load data from the backend but buffer if needed.

        This method is identical to the :class:`~.SyncedCollection`
        implementation except that it determines whether data is actually
        synchronized or instead read from a temporary buffer based on the
        buffering mode.
        """
        if not self._suspend_sync:
            if self._root is None:
                if self._is_buffered:
                    data = self._load_from_buffer()
                else:
                    data = self._load_from_resource()
                with self._suspend_sync:
                    self._update(data)
            else:
                self._root._load()

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
            An equivalent unsynced collection satisfying
            :meth:`~signac.synced_collections.data_types.synced_collection.SyncedCollection.is_base_type` that
            contains the buffered data. By default, the buffered data is just the
            data in the resource.

        """  # noqa: E501
        self._load_from_resource()

    @property
    def _is_buffered(self):
        """Check if we should write to the buffer or not."""
        return self.buffered or type(self)._buffer_context

    def _flush(self):
        """Flush data associated with this instance from the buffer."""
        pass

    @classmethod
    def _flush_buffer(self):
        """Flush all data in this class's buffer."""
        pass
