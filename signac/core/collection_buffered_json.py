# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements Buffered-JSON-backend."""
import os
import json
import errno
import uuid
import logging

from .jsoncollection import JSONCollection
from .buffered_collection import BufferedSyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList
from .caching import get_cache
from .buffered_collection import get_buffer_force_mode

logger = logging.getLogger(__name__)

_JSON_CACHE = None
_JSON_META = dict()


def get_json_cache():
    global _JSON_CACHE
    if _JSON_CACHE is None:
        _JSON_CACHE = get_cache()
    return _JSON_CACHE


def _store_metadata(filename, metadata=None):
    """Store the data to the buffer"""
    if (not get_buffer_force_mode()) and (filename not in _JSON_META):
        _JSON_META[filename] = metadata


class BufferedJSONCollection(BufferedSyncedCollection, JSONCollection):

    _cache = get_json_cache()

    @staticmethod
    def _get_metadata(filename):
        """Return metadata of JSON-file"""
        try:
            metadata = os.stat(filename)
            return metadata.st_size, metadata.st_mtime
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise

    def _write_to_buffer(self, data=None):
        """Write filename to buffer."""
        data = self.to_base() if data is None else data

        # Using cache to store the data and
        # storing filename and metadata in buffer
        self._cache[self._filename] = json.dumps(data).encode()
        metadata = self._get_metadata(self._filename)
        _store_in_buffer(self._filename, metadata)

    def _read_from_buffer(self):
        try:
            return json.loads(self._cache[self._filename])
        except KeyError:
            return None

    @classmethod
    def _flush_buffer(cls):
        """Flush the data in JSON-buffer.

        Returns
        -------
        issues: dict
            Mapping of filename and errors occured during flushing data.
        """
        issues = dict()

        while _JSON_BUFFER_FILENAMES:
            filename = _JSON_BUFFER_FILENAMES.pop()

            if not get_buffer_force_mode():
                # compare the metadata
                meta = _JSON_META.pop(filename)
                if cls._get_metadata(filename) != meta:
                    issues[filename] = 'File appears to have been externally modified.'
                    cls._cache[filename] = json.dumps(None)  # redis does not support None
                    continue

            # Sync the data to underlying backend
            try:
                blob = cls._cache[filename]
                dirname, fn = os.path.split(filename)
                fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(uid=uuid.uuid4(), fn=fn))
                with open(fn_tmp, 'wb') as tmpfile:
                    tmpfile.write(blob)
                os.replace(fn_tmp, filename)
            except OSError as error:
                # if sync fails add filename to issues
                # and remove data from cache
                logger.error(str(error))
                cls._cache[filename] = json.dumps(None)  # redis does not support None
                issues[filename] = error
        return issues
