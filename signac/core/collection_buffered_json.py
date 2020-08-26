# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements Buffered-JSON-backend."""
import os
import json
import errno
import uuid
import logging
import hashlib

from .synced_collection import SyncedCollection
from .buffered_collection import BufferedSyncedCollection
from .jsoncollection import JSONCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList
from .caching import get_cache
from .buffered_collection import get_buffer_force_mode

logger = logging.getLogger(__name__)

_JSON_CACHE = None
_JSON_META = dict()
_JSON_HASHES = dict()


def get_json_cache():
    """Return reference to JSON-Cache """
    global _JSON_CACHE
    if _JSON_CACHE is None:
        _JSON_CACHE = get_cache()
    return _JSON_CACHE


def _hash(blob):
    """Calculate and return the md5 hash value for the file data."""
    if blob is not None:
        m = hashlib.md5()
        m.update(blob)
        return m.hexdigest()


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

    def _write_to_buffer(self, data=None, synced_data=True):
        """Write filename to buffer."""
        data = self.to_base() if data is None else data
        # Using cache to store the data
        blob = json.dumps(data).encode()
        self._cache[self._filename] = blob
        # storing metadata and hash
        if self._filename not in _JSON_META:
            _JSON_META[self._filename] = None if get_buffer_force_mode() else \
                                              self._get_metadata(self._filename)
        if synced_data:
            _JSON_HASHES[self._filename] = _hash(blob)

    def _read_from_buffer(self):
        """Read the data from the buffer."""
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

        while _JSON_META:
            filename, meta = _JSON_META.popitem()

            if not get_buffer_force_mode():
                # compare the metadata
                if cls._get_metadata(filename) != meta:
                    issues[filename] = 'File appears to have been externally modified.'
                    continue

            blob = cls._cache[filename]
            del cls._cache[filename]  # Redis client does not have `pop`.

            # if hash match then data is same in flie and buffer
            if _hash(blob) != _JSON_HASHES.pop(filename):
                # Sync the data to underlying backend
                try:
                    dirname, fn = os.path.split(filename)
                    fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(uid=uuid.uuid4(), fn=fn))
                    with open(fn_tmp, 'wb') as tmpfile:
                        tmpfile.write(blob)
                    os.replace(fn_tmp, filename)
                except OSError as error:
                    # if sync fails add filename to issues
                    logger.error(str(error))
                    issues[filename] = error
        return issues


class BufferedJSONDict(BufferedJSONCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent JSON file.

    The JSONDict inherits from :class:`~core.synced_collection.SyncedCollection`
    and :class:`~core.syncedattrdict.SyncedAttrDict`.This class also supports the
    "buffered" mode where all the reads and writes are deferred.

    .. code-block:: python

        doc = JSONDict('data.json', write_concern=True)
        doc['foo'] = "bar"
        assert doc.foo == doc['foo'] == "bar"
        assert 'foo' in doc
        del doc['foo']

    .. code-block:: python

        >>> doc['foo'] = dict(bar=True)
        >>> doc
        {'foo': {'bar': True}}
        >>> doc.foo.bar = False
        {'foo': {'bar': False}}


    .. warning::

        While the JSONDict object behaves like a dictionary, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying file, copying (even deep
        copying) a JSONDict instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new JSONDict
        instance: `new_dict = JSONDict(old_dict.to_base())`.

    Parameters
    ----------
    filename: str, optional
        The filename of the associated JSON file on disk (Default value = None).
    write_concern: bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = None).
    data: mapping, optional
        The intial data pass to JSONDict. Defaults to `list()`
    parent: object, optional
        A parent instance of JSONDict or None (Default value = None).
    """


class BufferedJSONList(JSONCollection, SyncedList):
    """A non-string sequence interface to a persistent JSON file.

    The JSONList inherits from :class:`~core.collection_api.SyncedCollection`
    and :class:`~core.syncedlist.SyncedList`. This class also supports the
    "buffered" mode where all the reads and writes are deferred.

    .. code-block:: python
        synced_list = JSONList('data.json', write_concern=True)
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    .. warning::
        While the JSONList object behaves like a list, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying file, copying (even deep
        copying) a JSONList instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new JSONList
        instance: `new_list = JSONList(old_list.to_base())`.

    Parameters
    ----------
    filename: str, optional
        The filename of the associated JSON file on disk (Default value = None).
    write_concern: bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = None).
    data: non-str Sequence, optional
        The intial data pass to JSONList
    parent: object, optional
        A parent instance of JSONList or None (Default value = None).
    """


SyncedCollection.register(BufferedJSONCollection, BufferedJSONDict, BufferedJSONList)
