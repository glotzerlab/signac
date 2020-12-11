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

    @classmethod
    def __init_subclass__(cls):
        """Add  ``_cache`` attribute to every subclass.

        Each file-based SyncedCollection needs its own local cache.
        """
        super().__init_subclass__()
        cls._cache = get_cache()
        # We store the list of all objects currently storing data in the cache
        # so that we can call the instance-level flush method. The reason to do
        # is to ensure that we can check instance-specific buffering so that
        # there are no inconsistencies caused by nesting global and
        # instance-level buffering.
        cls._cached_collections = set()

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
                self._cached_collections.remove(self)
            except AttributeError as e:
                raise LookupError(
                    "An object attempted to flush from the cache without ever "
                    "being loaded. This likely indicates an internal error, "
                    "please contact the development team."
                ) from e
            cached_contents = self._cache[self._filename]

            # TODO: Make sure that calling to_base doesn't just lead to calling
            # _load (the non-buffered version) and wiping out the data from the
            # buffer.
            blob = json.dumps(self.to_base()).encode()

            # If the contents have not been changed since the initial read, we
            # don't need to rewrite it.
            if self._hash(cached_contents[blob]) != cached_contents['blob']:
                # Validate that the file hasn't been changed by something else.
                if cached_contents['meta'] != self._get_file_metadata():
                    raise MetadataError
                self._data = json.loads(cached_contents['contents'])
                self._sync()
            del self._cache[self._filename]

    def _sync_buffer(self):
        """Store data in buffer.

        We can reasonably provide a default implementation for all file-based
        backends that simply entails storing data to an in-memory cache (which
        could also be a Redis instance, etc).
        """
        # If we haven't already added this object to the cache, this indicates
        # either developer error (or possibly concurrency, which we don't
        # support). TODO Make sure there aren't any other cases.
        assert self._filename in self._cache

        # TODO: Generalize encode/decode so that we can also use non-JSON
        # encodable data.
        blob = json.dumps(self.to_base()).encode()
        self._cache[self._filename]['contents'] = blob

    def _load_buffer(self):
        """Read data from buffer.

        We can reasonably provide a default implementation for all file-based
        backends that simply entails reading data from an in-memory cache
        (which could also be a Redis instance, etc).
        """
        if self._filename in self._cache:
            # Load from buffer
            blob = self._cache[self._filename]['contents']
        else:
            data = self.to_base()
            blob = json.dumps(data).encode()
            blob_hash = self._hash(blob)

            self._cache[self._filename] = {
                'contents': blob,
                'hash': blob_hash,
                'metadata': self._get_file_metadata(self._filename),
            }
            self._cached_collections.add(self)
        return json.loads(blob.decode())
