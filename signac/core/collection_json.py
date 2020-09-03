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
import hashlib
import logging

from .synced_collection import SyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList
from .synced_collection import _in_buffered_mode
from .synced_collection import _get_buffer_force_mode
from .synced_collection import _register_buffered_backend
from .caching import get_cache
from .synced_collection import BufferedError


logger = logging.getLogger(__name__)

_JSON_CACHE = get_cache()
_JSON_META = dict()
_JSON_HASHES = dict()


def _get_metadata(filename):
    """Return metadata of JSON-file"""
    try:
        metadata = os.stat(filename)
        return metadata.st_size, metadata.st_mtime
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise


def _hash(blob):
    """Calculate and return the md5 hash value for the file data."""
    if blob is not None:
        m = hashlib.md5()
        m.update(blob)
        return m.hexdigest()


def _store_to_buffer(filename, blob, store_hash=False):
    _JSON_CACHE[filename] = blob
    if store_hash:
        _JSON_HASHES[filename] = _hash(blob)
    if filename not in _JSON_META:
        _JSON_META[filename] = None if _in_buffered_mode() and _get_buffer_force_mode() \
                                else _get_metadata(filename)


class JSONCollection(SyncedCollection):
    """Implement sync and load using a JSON back end."""

    backend = __name__  # type: ignore

    def __init__(self, filename=None, write_concern=False, **kwargs):
        self._filename = None if filename is None else os.path.realpath(filename)
        self._write_concern = write_concern
        kwargs['name'] = filename
        super().__init__(**kwargs)
        self._supports_buffering = True

    def _load_from_file(self):
        """Load the data from a JSON file."""
        try:
            with open(self._filename, 'rb') as file:
                return file.read()
        except IOError as error:
            if error.errno == errno.ENOENT:
                return json.dumps(None).encode()  # Redis requires data to be string or bytes.

    def _load(self):
        """Load the data from buffer or JSON file."""
        if _in_buffered_mode() or self._buffered:
            if self._filename in _JSON_CACHE:
                # Load from buffer
                blob = _JSON_CACHE[self._filename]
            else:
                # Load from file and store in buffer
                blob = self._load_from_file()
                _store_to_buffer(self._filename, blob, store_hash=True)
        else:
            # Load from file
            blob = self._load_from_file()
        return json.loads(blob)

    @staticmethod
    def _write_to_file(filename, blob, write_concern=False):
        """Write the data to JSON file."""
        # When write_concern flag is set, we write the data into dummy file and
        # then replace that file with original file.
        if write_concern:
            dirname, fn = os.path.split(filename)
            fn_tmp = os.path.join(dirname, f'._{uuid.uuid4()}_{fn}')
            with open(fn_tmp, 'wb') as tmpfile:
                tmpfile.write(blob)
            os.replace(fn_tmp, filename)
        else:
            with open(filename, 'wb') as file:
                file.write(blob)

    def _sync(self):
        """Write the data to file or buffer."""
        data = self.to_base()
        # Serialize data
        blob = json.dumps(data).encode()

        if _in_buffered_mode() or self._buffered > 0:
            # write in buffer
            _store_to_buffer(self._filename, blob)
        else:
            # write to file
            self._write_to_file(self._filename, blob, self._write_concern)

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

            blob = _JSON_CACHE[filename]
            del _JSON_CACHE[filename]  # Redis client does not have `pop`.

            if not _get_buffer_force_mode():
                # compare the metadata
                if _get_metadata(filename) != meta:
                    issues[filename] = 'File appears to have been externally modified.'
                    continue

            # if hash match then data is same in flie and buffer
            if _hash(blob) != _JSON_HASHES.pop(filename):
                # Sync the data to underlying backend
                try:
                    cls._write_to_file(filename, blob, write_concern=True)
                except OSError as error:
                    # if sync fails add filename to issues
                    logger.error(str(error))
                    issues[filename] = error
        return issues

    def flush(self):
        """Save buffered changes to the underlying file."""
        if not _in_buffered_mode():
            if _get_metadata(self._filename) != _JSON_META.pop(self._filename):
                raise BufferedError({
                    self._filename: 'File appears to have been externally modified.'})
            blob = _JSON_CACHE[self._filename]
            del _JSON_CACHE[self._filename]
            self._write_to_file(self._filename, blob, self._write_concern)


_register_buffered_backend(JSONCollection)


class JSONDict(JSONCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent JSON file.

    The JSONDict inherits from :class:`~core.synced_collection.SyncedCollection`
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
    write_concern: bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = None).
    data: mapping, optional
        The intial data pass to JSONDict. Defaults to `list()`
    parent: object, optional
        A parent instance of JSONDict or None (Default value = None).
    """


class JSONList(JSONCollection, SyncedList):
    """A non-string sequence interface to a persistent JSON file.

    The JSONList inherits from :class:`~core.synced_collection.SyncedCollection`
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


SyncedCollection.register(JSONDict, JSONList)
