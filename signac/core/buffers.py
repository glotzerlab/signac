# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Buffers for SyncedCollection API."""

import os
import json
import hashlib
import logging
import errno
from abc import abstractmethod

from .synced_collection import _in_buffered_mode
from .synced_collection import _get_buffer_force_mode
from .synced_collection import _register_buffered_backend
from .caching import get_cache
from .synced_collection import BufferedError

logger = logging.getLogger(__name__)


def _hash(blob):
    """Calculate and return the md5 hash value for the file data."""
    if blob is not None:
        m = hashlib.md5()
        m.update(blob)
        return m.hexdigest()


class FileBuffer:

    _cache =  get_cache()

    @staticmethod
    def _get_filemetadata(filename):
        """Return metadata of JSON-file"""
        try:
            metadata = os.stat(filename)
            return metadata.st_size, metadata.st_mtime
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise

    def _store_to_buffer(self, filename, blob, store_hash=False):
        if store_hash:
            self._cache['HASHE::' + filename] = _hash(blob)
        if filename not in self._cache:
            if not (_in_buffered_mode() and _get_buffer_force_mode()):
                # storing serialized metadata for redis-server 
                self._cache['METADATA::' + filename] = json.dumps(self._get_filemetadata(filename))
            # adding filename to list 
            if isinstance(self._cache, dict):
                if 'filenames' not in self._cache:
                    self._cache['filenames'] = []
                self._cache['filenames'].append(filename)
            else:
                self._cache.lpush('filenames', filename)
        self._cache[filename] = blob

    @classmethod
    def _pop_from_cache(cls, key, decode=False):
        # Redis client does not have `pop`.
        val = cls._cache[key]
        del cls._cache[key]
        return val.decode() if decode and isinstance(val, bytes) else val

    @staticmethod
    @abstractmethod
    def _write_to_file(filename, blob):
        """Write data to file."""

    @classmethod
    def _flush(cls, filename):
        """Write the data from buffer to the file. Return error if any."""
        blob = cls._pop_from_cache(filename) 

        if not _get_buffer_force_mode():
            meta = json.loads(cls._pop_from_cache('METADATA::' + filename))

        # if hash stored and hash of data is same then we skip the sync
        hash_key = 'HASHE::' + filename
        if not (hash_key in cls._cache and _hash(blob) == cls._pop_from_cache(hash_key,
                                                                              decode=True)):
            # compare the metadata
            if (not _get_buffer_force_mode()) and meta is not None:
                if cls._get_filemetadata(filename) != tuple(meta):
                    return 'File appears to have been externally modified.'
            # Sync the data to underlying backend
            try:
                cls._write_to_file(filename, blob)
            except OSError as error:
                # if sync fails return error
                logger.error(str(error))
                return error


    @classmethod
    def _flush_buffer(cls):
        """Flush the data in JSON-buffer.

        Returns
        -------
        issues: dict
            Mapping of filename and errors occured during flushing data.
        """
        issues = dict()

        # files stored in buffer
        if isinstance(cls._cache, dict):
            filenames = cls._cache.pop('filenames')
        else:
            filenames = []
            while cls._cache.llen('filenames'):
                filenames.append(cls._cache.lpop('filenames').decode())

        while filenames:
            filename = filenames.pop()
            err = cls._flush(filename)
            if err is not None:
                issues[filename] = err
        return issues

    def flush(self):
        """Save buffered changes to the underlying file."""
        if not _in_buffered_mode():
            # remove the filename from the list of files to flush
            if isinstance(self._cache, dict):
                self._cache['filenames'].remove(self._filename)
            else:
                self._cache.lrem('filenames', 1, self._filename)
            # flush the data
            err = self._flush(self._filename)
            if err is not None:
                raise BufferedError({self._filename: err})
