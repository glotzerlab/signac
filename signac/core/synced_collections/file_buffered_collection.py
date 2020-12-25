# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the FileBufferedSyncedCollection class.

The FileBufferedSyncedCollection is a concrete implementation of the buffer
protocol established by the BufferedSyncedCollection class. It uses an
in-memory cache to store data when in buffered mode. It is suitable for
use with any file-based backend because it performs integrity checks based on
whether or not the underlying file has been modified while buffering was
activated.
"""

import errno
import hashlib
import json
import os
import platform
import sys
from typing import Dict, Tuple, Union

from .buffered_collection import BufferedCollection
from .errors import MetadataError

PYPY = "PyPy" in platform.python_implementation()


class FileBufferedCollection(BufferedCollection):
    """Implement buffering for SyncedCollections with file-based backends.

    All file-based backends can use the same set of integrity checks prior to a
    buffer flush. This class standardizes that protocol. This class also
    centralizes the storage of buffered data, i.e. all subclasses share a
    single cache. This choice is so that that users can reliably get and set
    the buffer capacity without worrying about the number of distinct internal
    data buffers that might be present.

    Note for developers: The FileBufferedCollection should be inherited before
    any other collections so that it can pass the filename argument up the MRO
    of super calls.
    """

    _cache: Dict[str, Dict[str, Union[bytes, str, Tuple[int, float]]]] = {}
    _cached_collections: Dict[int, BufferedCollection] = {}
    _BUFFER_CAPACITY = 32 * 2 ** 20  # 32 MB
    _CURRENT_BUFFER_SIZE = 0

    def __init__(self, filename=None, *args, **kwargs):
        if PYPY:
            raise NotImplementedError("File-based buffering is not supported on PyPy.")
        super().__init__(filename=filename, *args, **kwargs)
        self._filename = filename

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
        """Get the current buffer capacity."""
        return FileBufferedCollection._BUFFER_CAPACITY

    @staticmethod
    def set_buffer_capacity(new_capacity):
        """Update the buffer capacity."""
        FileBufferedCollection._BUFFER_CAPACITY = new_capacity
        if new_capacity < FileBufferedCollection._CURRENT_BUFFER_SIZE:
            FileBufferedCollection._flush_buffer()

    @staticmethod
    def get_current_buffer_size():
        """Get the total amount of data currently stored in the buffer."""
        return FileBufferedCollection._CURRENT_BUFFER_SIZE

    def _flush(self, force=False):
        """Save buffered changes to the underlying file.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False).
        """
        if not self._is_buffered or force:
            try:
                cached_data = self._cache[self._filename]
            except KeyError:
                # There are valid reasons for nothing to be in the cache (the
                # object was never actually accessed during global buffering,
                # multiple collections pointing to the same file, etc).
                pass
            else:
                blob = self._encode(self._to_base())

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
                    del self._cache[self._filename]
                    data_size = sys.getsizeof(cached_data)
                    FileBufferedCollection._CURRENT_BUFFER_SIZE -= data_size

    @staticmethod
    def _encode(data):
        """Encode the data into a serializable form.

        This method assumes JSON-serializable data, but subclasses can override
        this hook method to change the encoding behavior as needed.

        Returns
        -------
        bytes
            The underlying encoded data.
        """
        return json.dumps(data).encode()

    @staticmethod
    def _decode(blob):
        """Decode serialized data.

        This method assumes JSON-serializable data, but subclasses can override
        this hook method to change the encoding behavior as needed.

        Parameters
        ----------
        blob : bytes
            Byte literal to be decoded.
        """
        return json.loads(blob.decode())

    def _sync_buffer(self):
        """Store data in buffer.

        We can reasonably provide a default implementation for all file-based
        backends that simply entails storing data to an in-memory cache (which
        could also be a Redis instance, etc).
        """
        if self._filename in self._cache:
            blob = self._encode(self._to_base())
            cached_data = self._cache[self._filename]
            buffer_size_change = sys.getsizeof(blob) - sys.getsizeof(
                cached_data["contents"]
            )
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
            # the hash after it's called (since it uses self._to_base()) to get
            # the data to initialize the cache with.
            self._initialize_data_in_cache()
            disk_data = self._load_from_resource()
            self._cache[self._filename]["hash"] = self._hash(self._encode(disk_data))

        if (
            FileBufferedCollection._CURRENT_BUFFER_SIZE
            > FileBufferedCollection._BUFFER_CAPACITY
        ):
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

        Returns
        -------
        Collection
            A collection of the same base type as the SyncedCollection this
            method is called for, corresponding to data loaded from the
            underlying file.
        """
        if self._filename in self._cache:
            # If multiple collections point to the same data, just checking
            # that the file contents are cached is not a sufficient check.
            if id(self) not in self._cached_collections:
                self._cached_collections[id(self)] = self
        else:
            self._initialize_data_in_cache()

        # Load from buffer
        blob = self._cache[self._filename]["contents"]

        if (
            FileBufferedCollection._CURRENT_BUFFER_SIZE
            > FileBufferedCollection._BUFFER_CAPACITY
        ):
            FileBufferedCollection._flush_buffer(force=True)
        return self._decode(blob)

    def _initialize_data_in_cache(self):
        """Create the initial entry for the data in the cache.

        This method always populates the cache with the encoded contents of the
        provided data, the hash of said data, and the current metadata of the
        file on disk.
        """
        blob = self._encode(self._to_base())
        metadata = self._get_file_metadata()

        self._cache[self._filename] = {
            "contents": blob,
            "hash": self._hash(blob),
            "metadata": metadata,
        }
        FileBufferedCollection._CURRENT_BUFFER_SIZE += sys.getsizeof(
            self._cache[self._filename]
        )
        self._cached_collections[id(self)] = self

    @classmethod
    def _flush_buffer(cls, force=False):
        """Flush the data in the file buffer.

        Returns
        -------
        dict
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
        while FileBufferedCollection._cached_collections:
            col_id, collection = FileBufferedCollection._cached_collections.popitem()
            if collection._is_buffered and not force:
                remaining_collections[col_id] = collection
                continue
            try:
                collection._flush(force=force)
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        FileBufferedCollection._cached_collections = remaining_collections
        return issues
