# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""A standardized buffering implementation for file-based backends.

All file-based backends can use a similar buffering protocol. In particular,
integrity checks can be performed by checking for whether the file has been
modified since it was originally loaded into the buffer. However, various
specific components are abstract and must be implemented by child classes.
"""

import errno
import os
import warnings
from abc import abstractmethod
from threading import RLock
from typing import Dict, Tuple, Union

from ..data_types.synced_collection import _LoadAndSave
from ..errors import BufferedError, MetadataError
from ..utils import _CounterFuncContext, _NullContext
from .buffered_collection import BufferedCollection


class _FileBufferedContext(_CounterFuncContext):
    """Extend the usual buffering context to support setting the buffer size.

    This context allows the buffer_backend method to temporarily set the buffer
    size within the scope of this context.
    """

    def __init__(self, cls):
        super().__init__(cls._flush_buffer)
        self._buffer_capacity = None
        self._original_buffer_capacitys = []
        self._cls = cls

    def __call__(self, buffer_capacity=None):
        self._buffer_capacity = buffer_capacity
        return self

    def __enter__(self):
        super().__enter__()
        if self._buffer_capacity is not None:
            self._original_buffer_capacitys.append(self._cls.get_buffer_capacity())
            self._cls.set_buffer_capacity(self._buffer_capacity)
        else:
            self._original_buffer_capacitys.append(None)
        self._buffer_capacity = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        original_buffer_capacity = self._original_buffer_capacitys.pop()
        if original_buffer_capacity is not None:
            self._cls.set_buffer_capacity(original_buffer_capacity)


class _BufferedLoadAndSave(_LoadAndSave):
    """Wrap base loading and saving with an extra thread lock.

    Writes to buffered collections will also modify the buffer, so they must
    acquire the buffer lock in addition to the default behavior.
    """

    def __enter__(self):
        self._collection._buffer_lock.__enter__()
        super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            super().__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._collection._buffer_lock.__exit__(exc_type, exc_val, exc_tb)


class FileBufferedCollection(BufferedCollection):
    """A :class:`~.SyncedCollection` that can buffer file I/O.

    This class provides a standardized buffering protocol for all file-based
    backends. All file-based backends can use the same set of integrity checks
    prior to a buffer flush to ensure that no conflicting modifications are
    made. Specifically, they can check whether the file has been modified on
    disk since it was originally loaded to the buffer. This class provides the
    basic infrastructure for that and defines standard methods that can be used
    by all classes. Subclasses must define the appropriate storage mechanism.

    .. note::
        Important notes for developers:
            - This class should be inherited before any other collections. This
              requirement is due to the extensive use of multiple inheritance.
              Since this class is designed to be combined with other
              :class:`~.SyncedCollection` types without making those types aware
              of buffering behavior, it transparently hooks into the
              initialization process, but this is dependent on its constructor
              being called before those of other classes.
            - All subclasses must define a class level ``_BUFFER_CAPACITY``
              variable that is used to determine the maximum allowable buffer
              size.

    Parameters
    ----------
    filename : str, optional
        The filename of the associated JSON file on disk (Default value = None).

    Warnings
    --------
    Although it can be done safely, in general modifying two different collections
    pointing to the same underlying resource while both are in different buffering
    modes is unsupported and can lead to undefined behavior. This class makes a
    best effort at performing safe modifications, but it is possible to construct
    nested buffered contexts for different objects that can lead to an invalid
    buffer state, or even situations where there is no obvious indicator of what
    is the canonical source of truth. In general, if you need multiple objects
    pointing to the same resource, it is **strongly** recommeneded to work with
    both of them in identical buffering states at all times.

    """

    _LoadSaveType = _BufferedLoadAndSave

    def __init__(self, parent=None, filename=None, *args, **kwargs):
        super().__init__(parent=parent, filename=filename, *args, **kwargs)
        self._filename = filename

    @classmethod
    def __init_subclass__(cls):
        """Prepare subclasses."""
        super().__init_subclass__()
        cls._CURRENT_BUFFER_SIZE = 0

        # This dict is the actual data buffer, mapping filenames to their
        # cached data and metadata.
        cls._buffer: Dict[str, Dict[str, Union[bytes, str, Tuple[int, float]]]] = {}

        # Buffered contexts may be nested, and when leaving a buffered context
        # we only want to flush collections that are no longer buffered. To
        # accomplish this, we maintain a list of buffered collections so that
        # we can perform per-instance flushes that account for their current
        # buffering state.
        cls._buffered_collections: Dict[int, BufferedCollection] = {}

        cls._buffer_context = _FileBufferedContext(cls)

    @classmethod
    def enable_multithreading(cls):
        """Enable safety checks and thread locks required for thread safety.

        This method adds managed buffer-related thread safety in addition to
        what the parent method does.

        """
        super().enable_multithreading()
        cls._BUFFER_LOCK = RLock()

    @classmethod
    def disable_multithreading(cls):
        """Disable all safety checks and thread locks required for thread safety.

        This method adds managed buffer-related thread safety in addition to
        what the parent method does.

        """
        super().disable_multithreading()
        cls._BUFFER_LOCK = _NullContext()

    @property
    def _buffer_lock(self):
        """Acquire the buffer lock."""
        return type(self)._BUFFER_LOCK

    def _get_file_metadata(self):
        """Return metadata of file.

        Returns
        -------
        Tuple[int, float] or None
            The size and last modification time of the associated file. If the
            file does not exist, returns :code:`None`.

        """
        try:
            metadata = os.stat(self._filename)
            return metadata.st_size, metadata.st_mtime_ns
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise
            # A return value of None indicates that the file does not
            # exist. Since any non-``None`` value will return `False` when
            # compared to ``None``, returning ``None`` provides a
            # reasonable value to compare against for metadata-based
            # validation checks.
            return None

    @classmethod
    def get_buffer_capacity(cls):
        """Get the current buffer capacity.

        Returns
        -------
        int
            The amount of data that can be stored before a flush is triggered
            in the appropriate units for a particular buffering implementation.

        """
        return cls._BUFFER_CAPACITY

    @classmethod
    def set_buffer_capacity(cls, new_capacity):
        """Update the buffer capacity.

        Parameters
        ----------
        new_capacity : int
            The new capacity of the buffer in the appropriate units for a particular
            buffering implementation.

        """
        cls._BUFFER_CAPACITY = new_capacity
        with cls._BUFFER_LOCK:
            if new_capacity < cls._CURRENT_BUFFER_SIZE:
                cls._flush_buffer(force=True)

    @classmethod
    def get_current_buffer_size(cls):
        """Get the total amount of data currently stored in the buffer.

        Returns
        -------
        int
            The size of all data contained in the buffer in the appropriate
            units for a particular buffering implementation.

        """
        return cls._CURRENT_BUFFER_SIZE

    def _load_from_buffer(self):
        """Read data from buffer.

        See :meth:`~._initialize_data_in_buffer` for details on the data stored
        in the buffer and the integrity checks performed.

        Returns
        -------
        collections.abc.Collection
            A collection of the same base type as the :class:`~.SyncedCollection` this
            method is called for, corresponding to data loaded from the
            underlying file.

        """
        with self._buffer_lock:
            if self._filename not in type(self)._buffer:
                # The first time this method is called, if nothing is in the buffer
                # for this file then we cannot guarantee that the _data attribute
                # is valid either since the resource could have been modified
                # between when _data was last updated and when this load is being
                # called. As a result, we have to load from the resource here to be
                # safe.
                data = self._load_from_resource()
                with self._thread_lock, self._suspend_sync:
                    self._update(data)
                self._initialize_data_in_buffer()

        # This storage can be safely updated every time on every thread.
        type(self)._buffered_collections[id(self)] = self

    @abstractmethod
    def _initialize_data_in_buffer(self):
        """Create the initial entry for the data in the cache.

        This method should be called the first time that a collection's data is
        accessed in buffered mode. This method stores the encoded data in the
        cache, along with the metadata of the underlying file and any other
        information that may be used for validation later. This information
        depends on the implementation of the buffer in subclasses.
        """
        pass

    @classmethod
    def _flush_buffer(cls, force=False, retain_in_force=False):
        """Flush the data in the file buffer.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False). This
            parameter is used when the buffer is filled to capacity.
        retain_in_force : bool
            If True, when forcing a flush a collection is retained in the buffer.
            This feature is useful if only some subset of the buffer's contents
            are relevant to size restrictions. For intance, since only modified
            items will have to be written back out to disk, a buffer protocol may
            not care to count unmodified collections towards the total.

        Returns
        -------
        dict
            Mapping of filename and errors occured during flushing data.

        Raises
        ------
        BufferedError
            If there are any issues with flushing the data.

        """
        issues = {}

        # We need to use the list of buffered objects rather than directly
        # looping over the local cache so that each collection can
        # independently decide whether or not to flush based on whether it's
        # still buffered (if buffered contexts are nested).
        remaining_collections = {}
        while True:
            # This is the only part that needs to be locked; once items are
            # removed from the buffer they can be safely handled on separate
            # threads.
            with cls._BUFFER_LOCK:
                try:
                    (
                        col_id,
                        collection,
                    ) = cls._buffered_collections.popitem()
                except KeyError:
                    break

            if collection._is_buffered and not force:
                # If force is false, then the only way for the collection to
                # still be buffered is if there are nested buffered contexts.
                # In that case, flush_buffer was called due to the exit of an
                # inner buffered context, and we shouldn't do anything with
                # this object, so we just put it back in the list *and* skip
                # the flush.
                remaining_collections[col_id] = collection
                continue
            elif force and retain_in_force:
                # If force is true, the collection must still be buffered.
                # In that case, the retain_in_force parameter controls whether
                # we we want to put it back in the remaining_collections list
                # after flushing any writes.
                remaining_collections[col_id] = collection

            try:
                collection._flush(force=force)
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        if not issues:
            cls._buffered_collections = remaining_collections
        else:
            raise BufferedError(issues)

    # (issue #727) TODO: The buffer_size argument should be changed to buffer_capacity in
    # signac 2.0 for consistency with the new names in synced collections.
    @classmethod
    def buffer_backend(cls, buffer_size=None, force_write=None, *args, **kwargs):
        """Enter context to buffer all operations for this backend.

        Parameters
        ----------
        buffer_size : int
            The capacity of the buffer to use within this context (resets after
            the context is exited).
        force_write : bool
            This argument does nothing and is only present for compatibility
            with signac 1.x.
        """
        if force_write is not None:
            warnings.warn(
                "The force_write parameter is deprecated and will be removed in "
                "signac 2.0. This functionality is no longer supported.",
                FutureWarning,
            )

        return cls._buffer_context(buffer_size)
