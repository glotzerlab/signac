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
import warnings

from .synced_collection import SyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList
from collections.abc import Mapping
from collections.abc import Sequence
from .synced_collection import NUMPY
from ..errors import KeyTypeError

if NUMPY:
    import numpy


class JSONFormatValidator:
    """Implement the validation for :class:`JSONCollection`."""

    @classmethod
    def validate(cls, data):
        """Emit a warning or raise an exception if data is invalid.

        Parameters
        ----------
        data:
            Data to validate.

        Returns
        -------
        data

        Raises
        ------
        KeyTypeError:
            If keys of mapping have unsupported data type.
        TypeError
            If data type is not supported.
        """

        if isinstance(data, (str, int, float, bool, bytes, type(None))):
            return data
        elif isinstance(data, Mapping):
            ret = {}
            for key, value in data.items():
                if isinstance(key, (int, bool, type(None))):
                    warnings.warn(
                        "Use of {} as key is deprecated and will be removed in version 2.0"
                        .format(type(key)), DeprecationWarning)
                    new_key = str(key)
                elif isinstance(key, str):
                    new_key = key
                else:
                    raise KeyTypeError(
                        "Keys must be str, int, bool or None, not {}".format(type(key).__name__))
                new_value = cls.validate(value)
                ret[new_key] = new_value
            return ret
        elif isinstance(data, Sequence):
            for i in range(len(data)):
                data[i] = cls.validate(data[i])
            return data
        elif NUMPY:
            if isinstance(data, numpy.ndarray):
                data = data.tolist()
                return cls.validate(data)
            if isinstance(data, numpy.number):
                return data.item()
        raise TypeError("Object of {} is not JSON-serializable".format(type(data)))


class JSONCollection(SyncedCollection):
    """Implement sync and load using a JSON back end."""

    backend = __name__  # type: ignore

    def __init__(self, filename=None, write_concern=False, **kwargs):
        self._filename = os.path.realpath(filename) if filename is not None else None
        self._write_concern = write_concern
        super().__init__(**kwargs)
        self._validators.append(JSONFormatValidator)
        if (filename is None) == (self._parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")

    def _load(self):
        """Load the data from a JSON-file."""
        try:
            with open(self._filename, 'rb') as file:
                blob = file.read()
                return json.loads(blob)
        except IOError as error:
            if error.errno == errno.ENOENT:
                return None

    def _sync(self):
        """Write the data to json file."""
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


class JSONDict(SyncedAttrDict, JSONCollection):
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


class JSONList(SyncedList, JSONCollection):
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


SyncedCollection.register(JSONDict, JSONList)
