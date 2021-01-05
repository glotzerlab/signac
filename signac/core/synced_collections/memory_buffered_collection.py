# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""A standardized buffering implementation for file-based backends.

This module defines a variant of the buffering protocol in
:py:mod:`file_buffered_collection`. This module exploits the same idea that
all file-based backends can use a similar buffering protocol using integrity
checks based on file modification times. However, that buffering approach
mitigates I/O costs by serializing data into a single buffer that is then
treated as the underlying data store by associated :class:`SyncedCollection` objects.
That approach is expensive because it still requires constant encoding and
decoding of data while checking for changes to the data in the buffer from other
instances pointing to the same underlying files. The approach in this module circumvents
this these performance bottlenecks by directly sharing the underlying data store
in the buffer for all objects, completely removing the need for encoding, decoding,
and updating in place.
"""

import errno
import os
from typing import Dict, Tuple, Union

from .buffered_collection import BufferedCollection
from .errors import MetadataError


class SharedMemoryFileBufferedCollection(BufferedCollection):
    """A :class:`SyncedCollection` that defers all I/O when buffered.

    This class implements a variant of the buffering strategy defined in the
    :class:`signac.core.synced_collections.file_buffered_collection.FileBufferedCollection`.
    Like that class, this class enables buffering for file-based backends by storing
    the last known modification time of the data on disk prior to entering buffered
    mode, then checking whether that has changed when the buffer is flushed.
    The buffering implemented by
    :class:`~.FileBufferedCollection`
    is true buffering in the sense that the entire write operation is
    performed as normal, including serialization, except that data is written to
    a different persistent store than the underlying resource. However, such
    buffering incurs significant overhead associated all the non-I/O tasks
    required prior to writing to the buffer, particularly data serialization.
    This class exists to remove these performance bottlenecks, which can be
    severe, and provide a more performant alternative.

    The buffering method implemented in this class circumvents the
    aforementioned performance bottlenecks by directly sharing the data between
    multiple synced collections. Rather than encoding and decoding data, all
    objects associated with a file share a single underlying in-memory data store,
    allowing any changes to one to be transparently persisted to the others. When
    the first collection associated with a particular file is accessed or
    modified in buffered mode for the first time, its data is stored in a cache.
    When future collections pointing to the same file access the cache, rather
    than synchronizing their data with the contents of the cache, the underlying
    data attribute of the collection is simply repointed at the data in the cache.
    This method exploits the fact that all mutable collection types in Python are
    references, so modifying one such collection results in modifying all of them,
    thereby removing any need for more complicated synchronization protocols.

    This approach has one principal downside relative to the other buffering
    method. Since the data is not always encoded into a byte-string, the exact
    size of the data in the buffer is not exactly known. This method simply counts
    the number of objects in the buffer as a means to decide how much to store before
    the buffer is flushed.

    Parameters
    ----------
    filename : str, optional
        The filename of the associated JSON file on disk (Default value = None).

    .. note::
        Important note for subclasses: This class should be inherited before
        any other collections. This requirement is due to the extensive use of
        multiple inheritance: since this class is designed to be combined with
        other :class:`SyncedCollection` types without making those types aware
        of buffering behavior, it transparently hooks into the initialization
        process, but this is dependent on its constructor being called before
        those of other classes.

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

    _cache: Dict[str, Dict[str, Union[bytes, str, Tuple[int, float, int]]]] = {}
    _cached_collections: Dict[int, BufferedCollection] = {}
    # TODO: Do we really care about the total number of objects stored in the
    # buffer, or do we only care about objects that have been modified (and
    # therefore require a file write when flushing)? For files that have been
    # read but not modified, I don't think that there's any reason not to just
    # let them sit in memory, or at least to give the user an option to allow
    # that in case they know that virtual memory exhaustion won't be an issue.
    _BUFFER_CAPACITY = 1000  # The number of collections to store in the buffer.
    _CURRENT_BUFFER_SIZE = 0

    def __init__(self, filename=None, *args, **kwargs):
        super().__init__(filename=filename, *args, **kwargs)
        self._filename = filename

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
            The number of collections that can be stored before a flush is triggered.

        """
        return SharedMemoryFileBufferedCollection._BUFFER_CAPACITY

    @staticmethod
    def set_buffer_capacity(new_capacity):
        """Update the buffer capacity.

        Parameters
        ----------
        new_capacity : int
            The number of collections that can be fit in the buffer.

        """
        SharedMemoryFileBufferedCollection._BUFFER_CAPACITY = new_capacity
        if new_capacity < SharedMemoryFileBufferedCollection._CURRENT_BUFFER_SIZE:
            SharedMemoryFileBufferedCollection._flush_buffer()

    @staticmethod
    def get_current_buffer_size():
        """Get the total number of collections currently stored in the buffer.

        Returns
        -------
        int
            The number of collections contained in the buffer.

        """
        return SharedMemoryFileBufferedCollection._CURRENT_BUFFER_SIZE

    def _flush(self, force=False):
        """Save buffered changes to the underlying file.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False). This
            parameter is used when the buffer is filled to capacity.
        stored_metadata : tuple[int, float]
            The metadata stored for this particular collection in the cache. It
            will have been removed by _flush_buffer when this method is called, so
            it must be passed as an argument. Under normal circumstances it will be
            equal to the metadata stored in _cache, but if there are multiple objects
            pointing to the same file and one of them modifies it by exiting buffered
            mode first and the other makes _no_ modifications, it is safe to simply
            reload the data.

        Raises
        ------
        MetadataError
            If any file is detected to have changed on disk since it was
            originally loaded into the buffer and modified.

        """
        if not self._is_buffered or force:
            # TODO: If we have two objects pointing to the same filename in the
            # cache and one of them flushes before the other, need to decide
            # how to handle it.
            try:
                _, stored_metadata = self._cached_collections.pop(id(self))
                cached_data = self._cache[self._filename]
            except KeyError:
                # There are valid reasons for nothing to be in the cache (the
                # object was never actually accessed during global buffering,
                # multiple collections pointing to the same file, etc).

                # However, we do have to verify that the current metadata for
                # the file is not newer than the originally stored metadata;
                # otherwise we load the file from disk, because this indicates
                # that something has modified the file since its data was
                # originally stored in the buffer. A typical use case would be
                # multiple collections pointing to the same file where only one
                # of them has changed. The fact that this collection hasn't
                # stored anything to the buffer is why this behavior is valid.
                cur_metadata = self._get_file_metadata()
                if stored_metadata[1] < cur_metadata[1]:
                    self._data = self._load_from_resource()
            else:
                # If the contents have not been changed since the initial read,
                # we don't need to rewrite it.
                try:
                    # Validate that the file hasn't been changed by
                    # something else.
                    if cached_data["modified"]:
                        if cached_data["metadata"] != self._get_file_metadata():
                            raise MetadataError(self._filename, cached_data["contents"])
                        self._data = cached_data["contents"]
                        self._save_to_resource()
                finally:
                    # Whether or not an error was raised, the cache must be
                    # cleared to ensure a valid final buffer state.
                    SharedMemoryFileBufferedCollection._CURRENT_BUFFER_SIZE -= 1
                    del self._cache[self._filename]

    def _load(self):
        """Load data from the backend but buffer if needed.

        Override the base buffered method to skip the _update and to let
        _load_from_buffer happen "in place."
        """
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                if self._is_buffered:
                    self._load_from_buffer()
                else:
                    data = self._load_from_resource()
                    with self._suspend_sync():
                        self._update(data)
            else:
                self._parent._load()

    def _save_to_buffer(self):
        """Store data in buffer.

        See :meth:`~._initialize_data_in_cache` for details on the data stored
        in the buffer and the integrity checks performed.
        """
        if self._filename in self._cache:
            # Need to check if we have multiple collections pointing to the
            # same file, and if so, track it.
            if id(self) not in SharedMemoryFileBufferedCollection._cached_collections:
                SharedMemoryFileBufferedCollection._cached_collections[id(self)] = (
                    self,
                    self._cache[self._filename]["metadata"],
                )
            self._cache[self._filename]["modified"] = True
        else:
            self._initialize_data_in_cache(modified=True)

        if (
            SharedMemoryFileBufferedCollection._CURRENT_BUFFER_SIZE
            > SharedMemoryFileBufferedCollection._BUFFER_CAPACITY
        ):
            SharedMemoryFileBufferedCollection._flush_buffer(force=True)

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
        if self._filename in self._cache:
            # Need to check if we have multiple collections pointing to the
            # same file, and if so, track it.
            if id(self) not in SharedMemoryFileBufferedCollection._cached_collections:
                SharedMemoryFileBufferedCollection._cached_collections[id(self)] = (
                    self,
                    self._cache[self._filename]["metadata"],
                )
        else:
            # TODO: The first time we call _load_from_buffer we might need to call
            # _load_from_resource. Otherwise, if something modified the file in memory
            # since the last time that we performed any save/load operation, we could be
            # putting an out-of-date state into the buffer. This also affects the
            # FileBufferedCollection.
            self._initialize_data_in_cache(modified=False)

        # Set local data to the version in the buffer.
        self._data = self._cache[self._filename]["contents"]

        if (
            SharedMemoryFileBufferedCollection._CURRENT_BUFFER_SIZE
            > SharedMemoryFileBufferedCollection._BUFFER_CAPACITY
        ):
            SharedMemoryFileBufferedCollection._flush_buffer(force=True)

    def _initialize_data_in_cache(self, modified):
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
        metadata = self._get_file_metadata()
        SharedMemoryFileBufferedCollection._cache[self._filename] = {
            "contents": self._data,
            "metadata": metadata,
            "modified": modified,
        }
        SharedMemoryFileBufferedCollection._CURRENT_BUFFER_SIZE += 1
        SharedMemoryFileBufferedCollection._cached_collections[id(self)] = (
            self,
            metadata,
        )

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
        if cls != SharedMemoryFileBufferedCollection:
            return {}

        issues = {}

        # We need to use the list of buffered objects rather than directly
        # looping over the local cache so that each collection can
        # independently decide whether or not to flush based on whether it's
        # still buffered (if buffered contexts are nested).
        remaining_collections = {}
        while SharedMemoryFileBufferedCollection._cached_collections:
            col_id = next(iter(SharedMemoryFileBufferedCollection._cached_collections))
            collection = SharedMemoryFileBufferedCollection._cached_collections[col_id][
                0
            ]

            if collection._is_buffered and not force:
                remaining_collections[
                    col_id
                ] = SharedMemoryFileBufferedCollection._cached_collections.pop(col_id)
                continue
            try:
                collection._flush(force=force)
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        if not issues:
            SharedMemoryFileBufferedCollection._cached_collections = (
                remaining_collections
            )
        return issues
