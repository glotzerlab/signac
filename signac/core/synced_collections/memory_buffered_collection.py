# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Enable in-memory buffering."""

import errno
import os
from typing import Dict, Tuple, Union

from .buffered_collection import BufferedCollection
from .errors import MetadataError


class MemoryBufferedCollection(BufferedCollection):
    """An in-memory buffer."""

    _cache: Dict[str, Dict[str, Union[bytes, str, Tuple[int, float]]]] = {}
    _cached_collections: Dict[int, BufferedCollection] = {}
    _BUFFER_CAPACITY = 32 * 2 ** 20  # 32 MB
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
                # the file is not older than the old metadata; otherwise we
                # load the file from disk, because this indicates that while we
                # haven't stored any buffered data for this file since it was
                # last modified, something else has (typical use case would be
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
                    if cached_data["metadata"] != self._get_file_metadata():
                        raise MetadataError(self._filename, cached_data["contents"])
                    self._data = cached_data["contents"]
                    self._save_to_resource()
                finally:
                    # Whether or not an error was raised, the cache must be
                    # cleared to ensure a valid final buffer state.
                    del self._cache[self._filename]
                    # data_size = len(cached_data["contents"])
                    # MemoryBufferedCollection._CURRENT_BUFFER_SIZE -= data_size

    def _load(self):
        """Load data from the backend but buffer if needed.

        Override the base buffered method to skip the _update and to let
        _load_from_buffer happen "in place".
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
            if id(self) not in MemoryBufferedCollection._cached_collections:
                MemoryBufferedCollection._cached_collections[id(self)] = (
                    self,
                    self._get_file_metadata(),
                )
        else:
            self._initialize_data_in_cache()

        # if (
        #     MemoryBufferedCollection._CURRENT_BUFFER_SIZE
        #     > MemoryBufferedCollection._BUFFER_CAPACITY
        # ):
        #     MemoryBufferedCollection._flush_buffer(force=True)

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
            if id(self) not in MemoryBufferedCollection._cached_collections:
                MemoryBufferedCollection._cached_collections[id(self)] = (
                    self,
                    self._get_file_metadata(),
                )
        else:
            self._initialize_data_in_cache()

        # Set local data to the version in the buffer.
        self._data = self._cache[self._filename]["contents"]

        # if (
        #     MemoryBufferedCollection._CURRENT_BUFFER_SIZE
        #     > MemoryBufferedCollection._BUFFER_CAPACITY
        # ):
        #     MemoryBufferedCollection._flush_buffer(force=True)
        # return self._decode(blob)

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
        metadata = self._get_file_metadata()
        MemoryBufferedCollection._cache[self._filename] = {
            "contents": self._data,
            "metadata": metadata,
        }
        # MemoryBufferedCollection._CURRENT_BUFFER_SIZE += len(
        #     MemoryBufferedCollection._cache[self._filename]["contents"]
        # )
        MemoryBufferedCollection._cached_collections[id(self)] = (self, metadata)

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
        if cls != MemoryBufferedCollection:
            return {}

        issues = {}

        # We need to use the list of buffered objects rather than directly
        # looping over the local cache so that each collection can
        # independently decide whether or not to flush based on whether it's
        # still buffered (if buffered contexts are nested).
        remaining_collections = {}
        while MemoryBufferedCollection._cached_collections:
            col_id = next(iter(MemoryBufferedCollection._cached_collections))
            collection = MemoryBufferedCollection._cached_collections[col_id][0]

            if collection._is_buffered and not force:
                remaining_collections[
                    col_id
                ] = MemoryBufferedCollection._cached_collections.pop(col_id)
                continue
            try:
                collection._flush(force=force)
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        if not issues:
            MemoryBufferedCollection._cached_collections = remaining_collections
        return issues
