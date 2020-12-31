# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a JSON SyncedCollection backend."""

import errno
import json
import os
import uuid
import warnings

# from .file_buffered_collection import FileBufferedCollection
from .memory_buffered_collection import MemoryBufferedCollection
from .synced_attr_dict import SyncedAttrDict
from .synced_collection import SyncedCollection
from .synced_list import SyncedList
from .utils import SCJSONEncoder
from .validators import json_format_validator


# TODO: This method should be removed in signac 2.0.
def _convert_key_to_str(data):
    """Recursively convert non-string keys to strings in dicts.

    This method supports :py:class:`collections.abc.Sequence` or
    :py:class:`collections.abc.Mapping` types as inputs, and recursively
    searches for any entries in :py:class:`collections.abc.Mapping` types where
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
    """A :class:`SyncedCollection` that synchronizes with a JSON file.

    This collection implements synchronization by reading and writing the associated
    JSON file in its entirety for every read/write operation. This backend is a good
    choice for maximum accessibility and transparency since all data is immediately
    accessible in the form of a text file with no additional tooling, but is
    likely a poor choice for high performance applications.

    Parameters
    ----------
    filename: str
        The filename of the associated JSON file on disk.
    write_concern: bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = False).

    """

    _backend = __name__  # type: ignore

    def __init__(
        self, filename=None, write_concern=False, parent=None, *args, **kwargs
    ):
        self._write_concern = write_concern
        self._filename = filename
        super().__init__(parent=parent, *args, **kwargs)

    def _load_from_resource(self):
        """Load the data from a JSON file.

        Returns
        -------
        Collection
            An equivalent unsynced collection satisfying :meth:`is_base_type` that
            contains the data in the JSON file.

        """
        try:
            with open(self._filename, "rb") as file:
                blob = file.read()
                return json.loads(blob)
        except OSError as error:
            if error.errno == errno.ENOENT:
                return None

    def _save_to_resource(self):
        """Write the data to JSON file."""
        # Serialize data
        blob = json.dumps(self, cls=SCJSONEncoder).encode()
        # When write_concern flag is set, we write the data into dummy file and then
        # replace that file with original file.
        if self._write_concern:
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
        """str: The name of the file this collection is synchronized with."""
        return self._filename


JSONCollection.add_validator(json_format_validator, _convert_key_to_str)


class BufferedJSONCollection(MemoryBufferedCollection, JSONCollection):
    """A :class:`JSONCollection` that supports I/O buffering.

    This class implements the buffer protocol defined by :class:`BufferedCollection`.
    The concrete implementation of buffering behavior is defined by the
    :class:`FileBufferedCollection`.
    """

    _backend = __name__ + ".buffered"  # type: ignore

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class JSONDict(JSONCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent JSON file.

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

    Parameters
    ----------
    filename: str, optional
        The filename of the associated JSON file on disk (Default value = None).
    write_concern: bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = False).
    data: :py:class:`collections.abc.Mapping`, optional
        The intial data pass to JSONDict (Default value = {}).
    parent: JSONCollection, optional
        A parent instance of JSONCollection or None (Default value = None).

    Warnings
    --------

    While the JSONDict object behaves like a dictionary, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying file, copying (even deep copying) a JSONDict
    instance may exhibit unexpected behavior. If a true copy is required, you
    should use the call operator to get a dictionary representation, and if
    necessary construct a new JSONDict instance: ``new_dict =
    JSONDict(old_dict())``.

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
        self._validate_constructor_args({"filename": filename}, data, parent)
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )


class JSONList(JSONCollection, SyncedList):
    """A non-string sequence interface to a persistent JSON file.

    .. code-block:: python

        synced_list = JSONList('data.json', write_concern=True)
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    Parameters
    ----------
    filename: str, optional
        The filename of the associated JSON file on disk (Default value = None).
    write_concern: bool, optional
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file (Default value = None).
    data: non-str :py:class:`collections.abc.Sequence`, optional
        The intial data pass to JSONList (Default value = []).
    parent: JSONCollection, optional
        A parent instance of JSONCollection or None (Default value = None).

    Warnings
    --------

    While the JSONList object behaves like a list, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying file, copying (even deep copying) a JSONList
    instance may exhibit unexpected behavior. If a true copy is required, you
    should use the call operator to get a dictionary representation, and if
    necessary construct a new JSONList instance:
    ``new_list = JSONList(old_list())``.

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
        self._validate_constructor_args({"filename": filename}, data, parent)
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

    _PROTECTED_KEYS = SyncedAttrDict._PROTECTED_KEYS + (
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
        self._validate_constructor_args({"filename": filename}, data, parent)
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
        self._validate_constructor_args({"filename": filename}, data, parent)
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )
