# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements JSON-backend.

This implements the JSON-backend for SyncedCollection API by
implementing sync and load methods.
"""
import os
import json
import errno
import uuid
import logging

from .synced_collection import SyncedCollection
from .buffered_collection import BufferedSyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList
from .caching import get_cache
from .buffered_collection import get_buffer_force_mode

logger = logging.getLogger(__name__)

_JSON_CACHE = None
_JSON_BUFFER_FILENAMES = set()
_JSON_META = dict()


def get_json_cache():
    global _JSON_CACHE
    if _JSON_CACHE is None:
        _JSON_CACHE = get_cache()
    return _JSON_CACHE


def _store_in_buffer(filename, metadata=None):
    """Store the data to the buffer"""
    _JSON_BUFFER_FILENAMES.add(filename)
    if (not get_buffer_force_mode()) and (filename not in _JSON_META):
        _JSON_META[filename] = metadata


class JSONCollection(BufferedSyncedCollection):
    """Implement sync and load using a JSON back end."""

    backend = __name__  # type: ignore
    _cache = get_json_cache()

    def __init__(self, filename=None, data=None, write_concern=False, **kwargs):
        kwargs['data'] = data
        self._filename = None if filename is None else os.path.realpath(filename)
        self._write_concern = write_concern
        super().__init__(**kwargs)
        self._is_cached = True
        if (filename is None) == (self._parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        if data is not None:
            self.sync()

    def _load_from_file(self):
        """Return Serialized data loaded from file."""
        try:
            with open(self._filename, 'rb') as file:
                return file.read()
        except IOError as error:
            if error.errno == errno.ENOENT:
                return json.dumps(None).encode()

    def _load(self):
        """Load the data from a JSON-file."""
        # Reading from cache
        try:
            data = json.loads(self._cache[self._filename])
        except KeyError:
            data = None
        # if no data in cache or cache contain None then load from file
        if data is None:
            blob = self._load_from_file()
            self._cache[self._filename] = blob
            return json.loads(blob)
        else:
            return data

    def _sync(self, data=None):
        """Write the data to JSON-file."""
        if data is None:
            data = self.to_base()
        # Serialize data:
        blob = json.dumps(data).encode()
        # When write_concern flag is set, we write the data into dummy file and then
        # replace that file with original file.
        if self._write_concern:
            dirname, filename = os.path.split(self._filename)
            fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
                uid=uuid.uuid4(), fn=filename))
            with open(fn_tmp, 'wb') as tmpfile:
                tmpfile.write(blob)
            os.replace(fn_tmp, self._filename)
        else:
            with open(self._filename, 'wb') as file:
                file.write(blob)
        # Writing to cache
        self._cache[self._filename] = blob

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

    # Cache invalidation

    def refresh_cache(self):
        """Load the data from backend and update the cache."""
        if self._parent is None:
            blob = self._load_from_file()
            # Writing to cache
            self._cache[self._filename] = blob
        else:
            self._parent.refresh_cache()


class JSONDict(JSONCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent JSON file.

    The JSONDict inherits from :class:`~core.collection_api.SyncedCollection`
    and :class:`~core.syncedattrdict.SyncedAttrDict`.

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
    data: mapping, optional
        The intial data pass to JSONDict. Defaults to `list()`
    parent: object, optional
        A parent instance of JSONDict or None (Default value = None).
    write_concern: bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = None).
    """

    pass


class JSONList(JSONCollection, SyncedList):
    """A non-string sequence interface to a persistent JSON file.

    The JSONDict inherits from :class:`~core.collection_api.SyncedCollection`
    and :class:`~core.syncedlist.SyncedList`.

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
    filename: str
        The filename of the associated JSON file on disk (Default value = None).
    data: non-str Sequence
        The intial data pass to JSONDict
    parent: object
        A parent instance of JSONDict or None (Default value = None).
    write_concern: bool
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = None).
    """

    pass


SyncedCollection.register(JSONCollection, JSONDict, JSONList)
