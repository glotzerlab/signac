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
import sys

from typing import Dict

from .buffered_synced_collection import BufferedCollection
from .caching import get_cache
from .errors import MetadataError


class FileBufferedCollection(BufferedCollection):
    """Implement buffering for SyncedCollections with file-based backends.

    All file-based backends can use the same set of integrity checks prior to a
    buffer flush. This class standardizes that protocol.
    """
    _cache = get_cache()
    _cached_collections: Dict[int, BufferedCollection] = {}
    _BUFFER_CAPACITY = 32 * 2**20    # 32 MB
    _CURRENT_BUFFER_SIZE = 0

    def __init__(self, filename, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename

    @classmethod
    def get_buffer_capacity(cls):
        """Get the current buffer capacity."""
        return cls._BUFFER_CAPACITY

    @classmethod
    def set_buffer_capacity(cls, new_capacity):
        """Update the buffer capacity."""
        cls._BUFFER_CAPACITY = new_capacity

    @classmethod
    def get_current_buffer_size(cls):
        """Get the total amount of data currently stored in the buffer."""
        return FileBufferedCollection._CURRENT_BUFFER_SIZE

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

    def _flush(self, force=False):
        """Save buffered changes to the underlying file."""
        if not self._is_buffered or force:
            try:
                cached_data = self._cache[self._filename]
            except KeyError:
                # There are valid reasons for nothing to be in the cache (the
                # object was never actually accessed during global buffering,
                # multiple collections pointing to the same file, etc).
                pass
            else:
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
                data_size = sys.getsizeof(cached_data)
                FileBufferedCollection._CURRENT_BUFFER_SIZE -= data_size

    def _encode(self):
        """Encode the data into a serializable form.

        This method assumes JSON-serializable data, but is exposed to allow
        changing this behavior.
        """
        return json.dumps(self.to_base()).encode()

    @staticmethod
    def _decode(blob):
        """Decode serialized data.

        Mirroring _encode, this method assumes JSON serialization.
        """
        return json.loads(blob.decode())

    def _sync_buffer(self):
        """Store data in buffer.

        We can reasonably provide a default implementation for all file-based
        backends that simply entails storing data to an in-memory cache (which
        could also be a Redis instance, etc).
        """
        if self._filename in self._cache:
            blob = self.encode()
            cached_data = self._cache[self._filename]
            buffer_size_change = sys.getsizeof(blob) - sys.getsizeof(
                cached_data['contents'])
            FileBufferedCollection._CURRENT_BUFFER_SIZE += buffer_size_change
            cached_data['contents'] = blob
        else:
            self._initialize_data_in_cache()

        if (FileBufferedCollection._CURRENT_BUFFER_SIZE
                > FileBufferedCollection._BUFFER_CAPACITY):
            FileBufferedCollection._flush_buffer(force=True)
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

        if (FileBufferedCollection._CURRENT_BUFFER_SIZE
                > FileBufferedCollection._BUFFER_CAPACITY):
            FileBufferedCollection._flush_buffer(force=True)
        return self._decode(blob)

    def _initialize_data_in_cache(self):
        """Create the initial entry for the data in the cache."""
        data = self.to_base()
        blob = json.dumps(data).encode()

        self._cache[self._filename] = {
            'contents': blob,
            'hash': self._hash(blob),
            'metadata': self._get_file_metadata(),
        }
        FileBufferedCollection._CURRENT_BUFFER_SIZE += sys.getsizeof(
            self._cache[self._filename])
        self._cached_collections[id(self)] = self
        return blob

    @classmethod
    def _flush_buffer(cls, force=False):
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
            if collection._is_buffered and not force:
                remaining_collections[col_id] = collection
                continue
            try:
                collection._flush(force=force)
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        cls._cached_collections = remaining_collections
        return issues
