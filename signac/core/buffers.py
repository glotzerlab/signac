# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Buffers for SyncedCollection API."""

import os
import hashlib
import logging
import errno
from abc import abstractmethod

from .synced_collection import _in_buffered_mode
from .synced_collection import _get_buffer_force_mode
from .caching import get_cache
from .synced_collection import BufferException

logger = logging.getLogger(__name__)


class MetadataError(BufferException):
    """Raised when metadata check fails."""

    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return f'{self.filename} appears to have been externally modified.'


def _hash(blob):
    """Calculate and return the md5 hash value for the file data."""
    if blob is not None:
        m = hashlib.md5()
        m.update(blob)
        return m.hexdigest()


class FileBuffer:
    """Implement Buffer for file based backends.

    It provides ``_store_to_buffer`` and ``_flush_buffer`` methods to write the data
    to the buffer and flush all the data respectively.
    """

    _cache = get_cache()

    @staticmethod
    def _get_filemetadata(filename):
        """Return metadata of file"""
        try:
            metadata = os.stat(filename)
            return metadata.st_size, metadata.st_mtime
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise

    def _store_to_buffer(self, filename, blob, store_hash=False):
        """Store the data to the file buffer.

        This method stores the serialized data and metadata of the file to the buffer.
        It also stores the hash of the data if the ``store_hash`` flag is set.
        Hash is stored when the data is being loaded from the file and stored in buffer.
        It is used to skip the extra synchronization (by comparing the hashes).
        """
        if store_hash:
            self._cache['HASH::' + filename] = _hash(blob)
        if filename not in self._cache:
            # storing metadata
            if not (_in_buffered_mode() and _get_buffer_force_mode()):
                self._cache['METADATA::' + filename] = self._get_filemetadata(filename)
            # adding filename to list
            self._cache.setdefault('filenames', [])
            self._cache['filenames'].append(filename)
        self._cache[filename] = blob

    @staticmethod
    @abstractmethod
    def _write_to_file(filename, blob):
        """Write data to file."""

    @classmethod
    def _flush(cls, filename):
        """Write the data from buffer to the file. Return error if any."""
        blob = cls._cache.pop(filename)

        if not _get_buffer_force_mode():
            meta = cls._cache.pop('METADATA::' + filename)

        # if hash is stored and hash of data is same then we skip the sync
        hash_key = 'HASH::' + filename
        if not (hash_key in cls._cache and _hash(blob) == cls._cache.pop(hash_key)):
            # compare the metadata
            if not _get_buffer_force_mode():
                if meta and cls._get_filemetadata(filename) != meta:
                    raise MetadataError(filename)
            # Sync the data to underlying backend
            try:
                cls._write_to_file(filename, blob)
            except OSError as error:
                logger.error(str(error))
                raise error

    @classmethod
    def _flush_buffer(cls):
        """Flush the data in the file buffer.

        Returns
        -------
        issues: dict
            Mapping of filename and errors occured during flushing data.
        """
        issues = {}

        # files stored in buffer
        filenames = cls._cache.pop('filenames') if 'filenames' in cls._cache else []

        for filename in filenames:
            try:
                cls._flush(filename)
            except (OSError, MetadataError) as err:
                issues[filename] = err
        return issues

    def flush(self):
        """Save buffered changes to the underlying file."""
        if not _in_buffered_mode():
            # remove the filename from the list of files to flush
            self._cache['filenames'].remove(self._filename)
            # flush the data
            self._flush(self._filename)
