# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a JSON :class:`~.SyncedCollection` backend."""

import errno
import json
import os
import uuid
import warnings
from typing import Tuple

from .. import SyncedAttrDict, SyncedCollection, SyncedList
from ..buffers.memory_buffered_collection import SharedMemoryFileBufferedCollection
from ..buffers.serialized_file_buffered_collection import (
    SerializedFileBufferedCollection,
)
from ..utils import SyncedCollectionJSONEncoder
from ..validators import json_format_validator


# TODO: This method should be removed in signac 2.0.
def _convert_key_to_str(data):
    """Recursively convert non-string keys to strings in dicts.

    This method supports :class:`collections.abc.Sequence` or
    :class:`collections.abc.Mapping` types as inputs, and recursively
    searches for any entries in :class:`collections.abc.Mapping` types where
    the key is not a string. This functionality is added for backwards
    compatibility with legacy behavior in signac, which allowed integer keys
    for dicts. These inputs were silently converted to string keys and stored
    since JSON does not support integer keys. This behavior is deprecated and
    will become an error in signac 2.0.

    Note for developers: this method is designed for use as a validator in the
    synced collections framework, but due to the backwards compatibility requirement
    it violates the general behavior of validators by modifying the data in place.
    This behavior can be removed in signac 2.0 once non-str keys become an error.
    """
    if isinstance(data, dict):

        def _str_key(key):
            if not isinstance(key, str):
                warnings.warn(
                    f"Use of {type(key).__name__} as key is deprecated "
                    "and will be removed in version 2.0",
                    DeprecationWarning,
                )
                key = str(key)
            return key

        # Get a list of keys a priori to support modification in place.
        for key in list(data):
            _convert_key_to_str(data[key])
            data[_str_key(key)] = data.pop(key)
    elif isinstance(data, list):
        for i, value in enumerate(data):
            _convert_key_to_str(value)


class JSONCollection(SyncedCollection):
    r"""A :class:`~.SyncedCollection` that synchronizes with a JSON file.

    This collection implements synchronization by reading and writing the associated
    JSON file in its entirety for every read/write operation. This backend is a good
    choice for maximum accessibility and transparency since all data is immediately
    accessible in the form of a text file with no additional tooling, but is
    likely a poor choice for high performance applications.

    **Thread safety**

    The :class:`JSONCollection` is thread-safe. To make these collections safe, the
    ``write_concern`` flag is ignored in multithreaded execution, and the
    write is **always** performed via a write to temporary file followed by a
    replacement of the original file. The file replacement operation uses
    :func:`os.replace`, which is guaranteed to be atomic by the Python standard.

    Parameters
    ----------
    filename : str
        The filename of the associated JSON file on disk.
    write_concern : bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = False).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    """

    _backend = __name__  # type: ignore
    _supports_threading = True

    def __init__(self, filename=None, write_concern=False, *args, **kwargs):
        self._write_concern = write_concern
        self._filename = filename
        super().__init__(*args, **kwargs)

    def _load_from_resource(self):
        """Load the data from a JSON file.

        Returns
        -------
        Collection or None
            An equivalent unsynced collection satisfying :meth:`is_base_type` that
            contains the data in the JSON file. Will return None if the file does
            not exist.

        """
        try:
            with open(self._filename, "rb") as file:
                blob = file.read()
                return json.loads(blob)
        except OSError as error:
            if error.errno == errno.ENOENT:
                return None
            else:
                raise

    def _save_to_resource(self):
        """Write the data to JSON file."""
        # Serialize data
        blob = json.dumps(self, cls=SyncedCollectionJSONEncoder).encode()
        # When write_concern flag is set, we write the data into dummy file and then
        # replace that file with original file. We also enable this mode
        # irrespective of the write_concern flag if we're running in
        # multithreaded mode.
        if self._write_concern or type(self)._threading_support_is_active:
            dirname, filename = os.path.split(self._filename)
            fn_tmp = os.path.join(dirname, f"._{uuid.uuid4()}_{filename}")
            with open(fn_tmp, "wb") as tmpfile:
                tmpfile.write(blob)
            os.replace(fn_tmp, self._filename)
        else:
            with open(self._filename, "wb") as file:
                file.write(blob)

    @property
    def filename(self):
        """str: The name of the associated JSON file on disk."""
        return self._filename

    @property
    def _lock_id(self):
        return self._filename


# The _convert_key_to_str validator will be removed in signac 2.0.
JSONCollection.add_validator(json_format_validator, _convert_key_to_str)


class BufferedJSONCollection(SerializedFileBufferedCollection, JSONCollection):
    """A :class:`JSONCollection` that supports I/O buffering.

    This class implements the buffer protocol defined by
    :class:`~.BufferedCollection`. The concrete implementation of buffering
    behavior is defined by the :class:`~.SerializedFileBufferedCollection`.
    """

    _backend = __name__ + ".buffered"  # type: ignore


class MemoryBufferedJSONCollection(SharedMemoryFileBufferedCollection, JSONCollection):
    """A :class:`JSONCollection` that supports I/O buffering.

    This class implements the buffer protocol defined by :class:`~.BufferedCollection`.
    The concrete implementation of buffering behavior is defined by the
    :class:`~.SharedMemoryFileBufferedCollection`.
    """

    _backend = __name__ + ".memory_buffered"  # type: ignore


class JSONDict(JSONCollection, SyncedAttrDict):
    r"""A dict-like data structure that synchronizes with a persistent JSON file.

    Examples
    --------
    >>> doc = JSONDict('data.json', write_concern=True)
    >>> doc['foo'] = "bar"
    >>> assert doc.foo == doc['foo'] == "bar"
    >>> assert 'foo' in doc
    >>> del doc['foo']
    >>> doc['foo'] = dict(bar=True)
    >>> doc
    {'foo': {'bar': True}}
    >>> doc.foo.bar = False
    >>> doc
    {'foo': {'bar': False}}

    Parameters
    ----------
    filename : str, optional
        The filename of the associated JSON file on disk (Default value = None).
    write_concern : bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = False).
    data : :class:`collections.abc.Mapping`, optional
        The initial data passed to :class:`JSONDict`. If ``None``, defaults to
        ``{}`` (Default value = None).
    parent : JSONCollection, optional
        A parent instance of :class:`JSONCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`JSONDict` object behaves like a :class:`dict`, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying file, copying (even deep copying) a :class:`JSONDict`
    instance may exhibit unexpected behavior. If a true copy is required, you
    should use the call operator to get a dictionary representation, and if
    necessary construct a new :class:`JSONDict` instance.

    """

    _PROTECTED_KEYS: Tuple[str, ...] = ("_filename",)

    def __init__(
        self,
        filename=None,
        write_concern=False,
        data=None,
        parent=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )


class JSONList(JSONCollection, SyncedList):
    r"""A list-like data structure that synchronizes with a persistent JSON file.

    Only non-string sequences are supported by this class.

    Examples
    --------
    >>> synced_list = JSONList('data.json', write_concern=True)
    >>> synced_list.append("bar")
    >>> assert synced_list[0] == "bar"
    >>> assert len(synced_list) == 1
    >>> del synced_list[0]

    Parameters
    ----------
    filename : str, optional
        The filename of the associated JSON file on disk (Default value = None).
    write_concern : bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = None).
    data : non-str :class:`collections.abc.Sequence`, optional
        The initial data passed to :class:`JSONList `. If ``None``, defaults to
        ``[]`` (Default value = None).
    parent : JSONCollection, optional
        A parent instance of :class:`JSONCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`JSONList` object behaves like a :class:`list`, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying file, copying (even deep copying) a :class:`JSONList`
    instance may exhibit unexpected behavior. If a true copy is required, you
    should use the call operator to get a dictionary representation, and if
    necessary construct a new :class:`JSONList` instance.

    """

    def __init__(
        self,
        filename=None,
        write_concern=False,
        data=None,
        parent=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )


class BufferedJSONDict(BufferedJSONCollection, SyncedAttrDict):
    """A buffered :class:`JSONDict`."""

    _PROTECTED_KEYS: Tuple[str, ...] = (
        "_filename",
        "_buffered",
        "_is_buffered",
    )

    def __init__(
        self,
        filename=None,
        write_concern=False,
        data=None,
        parent=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )


class BufferedJSONList(BufferedJSONCollection, SyncedList):
    """A buffered :class:`JSONList`."""

    def __init__(
        self,
        filename=None,
        write_concern=False,
        data=None,
        parent=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )


class MemoryBufferedJSONDict(MemoryBufferedJSONCollection, SyncedAttrDict):
    """A buffered :class:`JSONDict`."""

    _PROTECTED_KEYS: Tuple[str, ...] = SyncedAttrDict._PROTECTED_KEYS + (
        "_filename",
        "_buffered",
        "_is_buffered",
    )

    def __init__(
        self,
        filename=None,
        write_concern=False,
        data=None,
        parent=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )


class MemoryBufferedJSONList(MemoryBufferedJSONCollection, SyncedList):
    """A buffered :class:`JSONList`."""

    def __init__(
        self,
        filename=None,
        write_concern=False,
        data=None,
        parent=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )
