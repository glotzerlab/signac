# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""A standardized buffering implementation for file-based backends.

All file-based backends can use a similar buffering protocol. In particular,
integrity checks can be performed by checking for whether the file has been
modified since it was originally loaded into the buffer.
"""

import errno
import hashlib
import json
import os
from contextlib import contextmanager
from threading import RLock
from typing import Dict, Tuple, Union

from .buffered_collection import BufferedCollection
from .errors import MetadataError
from .synced_collection import _fake_lock
from .utils import SCJSONEncoder


@contextmanager
def _buffer_lock(self):
    """Prepare context for thread-safe operation.

    All operations that can mutate an object should use this context
    manager to ensure thread safety.
    """
    with type(self)._BUFFER_LOCK:
        yield


class FileBufferedCollection(BufferedCollection):
    """A :class:`SyncedCollection` that can buffer file I/O.

    This class provides a standardized buffering protocol for all file-based
    backends.  All file-based backends can use the same set of integrity checks
    prior to a buffer flush to ensure that no conflicting modifications are
    made. Specifically, they can check whether the file has been modified on
    disk since it was originally loaded to the buffer. This class uses a
    single centralized cache for all subclasses, irrespective of backend. This
    choice is so that that users can reliably get and set the buffer capacity
    without worrying about the number of distinct internal data buffers that
    might be present. This setting has no effect on the buffering behavior of
    other :class:`BufferedCollection` types.

    .. note::
        Important note for subclasses: This class should be inherited before
        any other collections. This requirement is due to the extensive use of
        multiple inheritance: since this class is designed to be combined with
        other :class:`SyncedCollection` types without making those types aware
        of buffering behavior, it transparently hooks into the initialization
        process, but this is dependent on its constructor being called before
        those of other classes.

    Parameters
    ----------
    filename: str, optional
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

    # Note for developers: since all subclasses share a single cache, all
    # references to cache-related class variables in the code use the class
    # name explicitly rather than using cls (in classmethods) or self (in
    # methods). This usage avoids any possibility for confusion regarding
    # backend-specific caches.

    _cache: Dict[str, Dict[str, Union[bytes, str, Tuple[int, float]]]] = {}
    _cached_collections: Dict[int, BufferedCollection] = {}
    _BUFFER_CAPACITY = 32 * 2 ** 20  # 32 MB
    _CURRENT_BUFFER_SIZE = 0
    _BUFFER_LOCK = RLock()

    def __init__(self, filename=None, *args, **kwargs):
        super().__init__(filename=filename, *args, **kwargs)
        self._filename = filename

    @classmethod
    def enable_multithreading(cls):
        """Allow multithreaded access to and modification of :class:`SyncedCollection`s.

        Support for multithreaded execution can be disabled by calling
        :meth:`~.disable_multithreading`; calling this method reverses that.

        """
        super().enable_multithreading()
        cls._buffer_lock = _buffer_lock

    @classmethod
    def disable_multithreading(cls):
        """Prevent multithreaded access to and modification of :class:`SyncedCollection`s.

        The mutex locks required to enable multithreading introduce nontrivial performance
        costs, so they can be disabled for classes that support it.

        """
        super().disable_multithreading()
        cls._buffer_lock = _fake_lock

    @staticmethod
    def _hash(blob):
        """Calculate and return the md5 hash value for the file data.

        Parameters
        ----------
        blob : bytes
            Byte literal to be hashed.

        Returns
        -------
        str
            The md5 hash of the input bytes.

        """
        if blob is not None:
            m = hashlib.md5()
            m.update(blob)
            return m.hexdigest()

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

    @staticmethod
    def get_buffer_capacity():
        """Get the current buffer capacity.

        Returns
        -------
        int
            The number of bytes that can be stored before a flush is triggered.

        """
        return FileBufferedCollection._BUFFER_CAPACITY

    @contextmanager
    def _load_and_save(self):
        """Prepare a context manager in which mutating changes can happen.

        Override the parent function's hook to support safely multithreaded
        access to the buffer.
        """
        with self._buffer_lock():
            with super()._load_and_save():
                yield

    @staticmethod
    def set_buffer_capacity(new_capacity):
        """Update the buffer capacity.

        Parameters
        ----------
        new_capacity : int
            The new capacity of the buffer in bytes.

        """
        FileBufferedCollection._BUFFER_CAPACITY = new_capacity
        if new_capacity < FileBufferedCollection._CURRENT_BUFFER_SIZE:
            FileBufferedCollection._flush_buffer(force=True)

    @staticmethod
    def get_current_buffer_size():
        """Get the total amount of data currently stored in the buffer.

        Returns
        -------
        int
            The size of all data contained in the buffer (in bytes).

        Notes
        -----
        The buffer size is defined as the total number of bytes that will be
        written out when the buffer is flushed. This is *not* the same as the total
        size of the buffer, which also contains additional information like the
        hash of the data and the file metadata (which are used for integrity checks).

        """
        return FileBufferedCollection._CURRENT_BUFFER_SIZE

    def _flush(self, force=False):
        """Save buffered changes to the underlying file.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False). This
            parameter is used when the buffer is filled to capacity.

        Raises
        ------
        MetadataError
            If any file is detected to have changed on disk since it was
            originally loaded into the buffer and modified.

        """
        if not self._is_buffered or force:
            try:
                cached_data = FileBufferedCollection._cache[self._filename]
            except KeyError:
                # There are valid reasons for nothing to be in the cache (the
                # object was never actually accessed during global buffering,
                # multiple collections pointing to the same file, etc).
                pass
            else:
                blob = self._encode(self._data)

                # If the contents have not been changed since the initial read,
                # we don't need to rewrite it.
                try:
                    if self._hash(blob) != cached_data["hash"]:
                        # Validate that the file hasn't been changed by
                        # something else.
                        if cached_data["metadata"] != self._get_file_metadata():
                            raise MetadataError(self._filename, cached_data["contents"])
                        self._data = self._decode(cached_data["contents"])
                        self._save_to_resource()
                finally:
                    # Whether or not an error was raised, the cache must be
                    # cleared to ensure a valid final buffer state.
                    del FileBufferedCollection._cache[self._filename]
                    data_size = len(cached_data["contents"])
                    FileBufferedCollection._CURRENT_BUFFER_SIZE -= data_size

    @staticmethod
    def _encode(data):
        """Encode the data into a serializable form.

        This method assumes JSON-serializable data, but subclasses can override
        this hook method to change the encoding behavior as needed.

        Parameters
        ----------
        data : collections.abc.Collection
            Any collection type that can be encoded.

        Returns
        -------
        bytes
            The underlying encoded data.

        """
        return json.dumps(data, cls=SCJSONEncoder).encode()

    @staticmethod
    def _decode(blob):
        """Decode serialized data.

        This method assumes JSON-serializable data, but subclasses can override
        this hook method to change the encoding behavior as needed.

        Parameters
        ----------
        blob : bytes
            Byte literal to be decoded.

        Returns
        -------
        data : collections.abc.Collection
            The decoded data in the appropriate base collection type.

        """
        return json.loads(blob.decode())

    def _save_to_buffer(self):
        """Store data in buffer.

        See :meth:`~._initialize_data_in_cache` for details on the data stored
        in the buffer and the integrity checks performed.
        """
        # Writes to the buffer must always be locked for thread safety.
        with self._buffer_lock():
            if self._filename in FileBufferedCollection._cache:
                # Always track all instances pointing to the same data.
                FileBufferedCollection._cached_collections[id(self)] = self
                blob = self._encode(self._data)
                cached_data = FileBufferedCollection._cache[self._filename]
                buffer_size_change = len(blob) - len(cached_data["contents"])
                FileBufferedCollection._CURRENT_BUFFER_SIZE += buffer_size_change
                cached_data["contents"] = blob
            else:
                # The only methods that could safely call sync without a load are
                # destructive operations like `reset` or `clear` that completely
                # wipe out previously existing data. Therefore, the safest choice
                # for ensuring consistency of the buffer is to modify the stored
                # hash (which is used for the consistency check) with the hash of
                # the current data on disk. _initialize_data_in_cache always uses
                # the current metadata, so the only extra work here is to modify
                # the hash after it's called (since it uses self._to_base() to get
                # the data to initialize the cache with).
                self._initialize_data_in_cache()
                disk_data = self._load_from_resource()
                FileBufferedCollection._cache[self._filename]["hash"] = self._hash(
                    self._encode(disk_data)
                )

            if (
                FileBufferedCollection._CURRENT_BUFFER_SIZE
                > FileBufferedCollection._BUFFER_CAPACITY
            ):
                FileBufferedCollection._flush_buffer(force=True)

    def _load_from_buffer(self):
        """Read data from buffer.

        See :meth:`~._initialize_data_in_cache` for details on the data stored
        in the buffer and the integrity checks performed.

        Returns
        -------
        Collection
            A collection of the same base type as the SyncedCollection this
            method is called for, corresponding to data loaded from the
            underlying file.

        """
        if self._filename in FileBufferedCollection._cache:
            # Always track all instances pointing to the same data.
            FileBufferedCollection._cached_collections[id(self)] = self
        else:
            # The first time this method is called, if nothing is in the buffer
            # for this file then we cannot guarantee that the _data attribute
            # is valid either since the resource could have been modified
            # between when _data was last updated and when this load is being
            # called. As a result, we have to load from the resource here to be
            # safe.
            data = self._load_from_resource()
            with self._thread_lock():
                with self._suspend_sync():
                    self._update(data)
            self._initialize_data_in_cache()

        # Load from buffer
        blob = FileBufferedCollection._cache[self._filename]["contents"]

        if (
            FileBufferedCollection._CURRENT_BUFFER_SIZE
            > FileBufferedCollection._BUFFER_CAPACITY
        ):
            FileBufferedCollection._flush_buffer(force=True)
        return self._decode(blob)

    def _initialize_data_in_cache(self):
        """Create the initial entry for the data in the cache.

        This method should be called the first time that a collection's data is
        accessed in buffered mode. This method stores the encoded data in the
        cache, along with a hash of the data and the metadata of the underlying
        file. The hash is later used for quick checks of whether the data in
        memory has changed since the initial load into the buffer. If so the
        metadata is used to verify that the file on disk has not been modified
        since the data was modified in memory.

        We also maintain a separate set of all collections that are currently
        in buffered mode. This extra storage is necessary because when leaving
        a buffered context we need to make sure to only flush collections that
        are no longer in buffered mode in cases of nested buffering. This
        additional check helps prevent or transparently error on otherwise
        unsafe access patterns.
        """
        blob = self._encode(self._data)
        metadata = self._get_file_metadata()

        FileBufferedCollection._cache[self._filename] = {
            "contents": blob,
            "hash": self._hash(blob),
            "metadata": metadata,
        }
        FileBufferedCollection._CURRENT_BUFFER_SIZE += len(
            FileBufferedCollection._cache[self._filename]["contents"]
        )
        FileBufferedCollection._cached_collections[id(self)] = self

    @classmethod
    def _flush_buffer(cls, force=False):
        """Flush the data in the file buffer.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False). This
            parameter is used when the buffer is filled to capacity.

        Returns
        -------
        dict
            Mapping of filename and errors occured during flushing data.

        Raises
        ------
        BufferedError
            If there are any issues with flushing the data.

        """
        # All subclasses share a single cache rather than having separate
        # caches for each instance, so we can exit early in subclasses.
        if cls != FileBufferedCollection:
            return {}

        issues = {}

        # We need to use the list of buffered objects rather than directly
        # looping over the local cache so that each collection can
        # independently decide whether or not to flush based on whether it's
        # still buffered (if buffered contexts are nested).
        remaining_collections = {}
        while FileBufferedCollection._cached_collections:
            col_id, collection = FileBufferedCollection._cached_collections.popitem()
            if collection._is_buffered and not force:
                remaining_collections[col_id] = collection
                continue
            try:
                collection._flush(force=force)
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        if not issues:
            FileBufferedCollection._cached_collections = remaining_collections
        return issues
