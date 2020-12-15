# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the FileBufferedSyncedCollection class.

The FileBufferedSyncedCollection is a concrete implementation of the buffer
protocol established by the BufferedSyncedCollection class. It uses an
in-memory cache to store data when in buffered mode. It is suitable for
use with any file-based back end because it performs integrity checks based on
whether or not the underlying file has been modified while buffering was
activated.
"""

import errno
import hashlib
import json
import os

from .buffered_synced_collection import BufferedCollection
from .caching import get_cache
from .errors import MetadataError


class FileBufferedCollection(BufferedCollection):
    """Implement buffering for SyncedCollections with file-based backends.

    All file-based backends can use the same set of integrity checks prior to a
    buffer flush. This class standardizes that protocol.
    """
    # TODO: Need to track buffer size to force a flush.
    _cache = get_cache()
    _cached_collections = {}

    def __init__(self, filename, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename

    @staticmethod
    def _hash(blob):
        """Calculate and return the md5 hash value for the file data."""
        if blob is not None:
            m = hashlib.md5()
            m.update(blob)
            return m.hexdigest()

    def _get_file_metadata(self):
        """Return metadata of file."""
        try:
            metadata = os.stat(self._filename)
            return metadata.st_size, metadata.st_mtime
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise

    # TODO: Use a better name to differentiate between the flush of the entire
    # buffer (the classmethod) and just flushing the current item from the
    # buffer
    def _flush(self):
        """Save buffered changes to the underlying file."""
        # TODO: Currently this check (_is_buffered) may also be happening
        # everywhere that _flush is called, need to be consistent at some
        # point.
        if not self._is_buffered:
            try:
                cached_data = self._cache[self._filename]
            except KeyError:
                # There are valid reasons for nothing to be in the cache (the
                # object was never actually accessed during global buffering,
                # multiple collections pointing to the same file, etc).
                pass
            else:
                # TODO: Make sure that calling to_base doesn't just lead to
                # calling _load (the non-buffered version) and wiping out the
                # data from the buffer.
                blob = json.dumps(self.to_base()).encode()

                # If the contents have not been changed since the initial read,
                # we don't need to rewrite it.
                if self._hash(blob) != cached_data['contents']:
                    # Validate that the file hasn't been changed by something
                    # else.
                    if cached_data['metadata'] != self._get_file_metadata():
                        raise MetadataError(self._filename)
                    self._data = json.loads(cached_data['contents'])
                    self._sync()
                del self._cache[self._filename]

    def _sync_buffer(self):
        """Store data in buffer.

        We can reasonably provide a default implementation for all file-based
        backends that simply entails storing data to an in-memory cache (which
        could also be a Redis instance, etc).
        """
        if self._filename in self._cache:
            # TODO: Generalize encode/decode so that we can also use non-JSON
            # encodable data. Alternatively, add json format validation to this
            # backend.
            blob = json.dumps(self.to_base()).encode()
            self._cache[self._filename]['contents'] = blob
        else:
            self._initialize_data_in_cache()

        # If multiple collections point to the same data, just checking that
        # the file contents are cached is not a sufficient check.
        if id(self) not in self._cached_collections:
            self._cached_collections[id(self)] = self

    def _load_buffer(self):
        """Read data from buffer.

        We can reasonably provide a default implementation for all file-based
        backends that simply entails reading data from an in-memory cache
        (which could also be a Redis instance, etc).
        """
        if self._filename in self._cache:
            # Load from buffer
            blob = self._cache[self._filename]['contents']

            # If multiple collections point to the same data, just checking
            # that the file contents are cached is not a sufficient check.
            if id(self) not in self._cached_collections:
                self._cached_collections[id(self)] = self
        else:
            blob = self._initialize_data_in_cache()
        return json.loads(blob.decode())

    def _initialize_data_in_cache(self):
        """Create the initial entry for the data in the cache."""
        # TODO: Add this logic to the buffered context manager. For
        # instance-level buffering, we should just load immediately (if
        # data is not in the buffer). For global buffering, this logic here
        # is necessary.
        data = self.to_base()
        blob = json.dumps(data).encode()
        blob_hash = self._hash(blob)

        self._cache[self._filename] = {
            'contents': blob,
            'hash': blob_hash,
            'metadata': self._get_file_metadata(),
        }
        self._cached_collections[id(self)] = self
        return blob

    @classmethod
    def _flush_buffer(cls):
        """Flush the data in the file buffer.

        Returns
        -------
        issues : dict
            Mapping of filename and errors occured during flushing data.
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
        while cls._cached_collections:
            col_id, collection = cls._cached_collections.popitem()
            if collection._is_buffered:
                remaining_collections[col_id] = collection
                continue
            try:
                collection._flush()
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        cls._cached_collections = remaining_collections
        return issues
