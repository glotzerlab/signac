# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a JSON :class:`~.SyncedCollection` backend."""

import errno
import json
import os
import sys
import uuid
from collections.abc import Mapping, Sequence
from typing import Callable, FrozenSet
from typing import Sequence as Sequence_t

from .. import SyncedCollection, SyncedDict, SyncedList
from ..buffers.memory_buffered_collection import SharedMemoryFileBufferedCollection
from ..buffers.serialized_file_buffered_collection import (
    SerializedFileBufferedCollection,
)
from ..data_types.attr_dict import AttrDict
from ..errors import InvalidKeyError, KeyTypeError
from ..numpy_utils import (
    _is_atleast_1d_numpy_array,
    _is_complex,
    _is_numpy_scalar,
    _numpy_cache_blocklist,
)
from ..utils import AbstractTypeResolver, SyncedCollectionJSONEncoder
from ..validators import json_format_validator, no_dot_in_key, require_string_key

ON_WINDOWS = sys.platform.startswith("win32") or sys.platform.startswith("cygwin")


"""
There are many classes defined in this file. Most of the definitions are
trivial since logic is largely inherited, but the large number of classes
and the extensive docstrings can be intimidating and make the source
difficult to parse. Section headers like these are used to organize the
code to reduce this barrier.
"""

_json_attr_dict_validator_type_resolver = AbstractTypeResolver(
    {
        # We identify >0d numpy arrays as sequences for validation purposes.
        "SEQUENCE": lambda obj: (isinstance(obj, Sequence) and not isinstance(obj, str))
        or _is_atleast_1d_numpy_array(obj),
        "NUMPY": lambda obj: _is_numpy_scalar(obj),
        "BASE": lambda obj: isinstance(obj, (str, int, float, bool, type(None))),
        "MAPPING": lambda obj: isinstance(obj, Mapping),
    },
    cache_blocklist=_numpy_cache_blocklist,
)


def json_attr_dict_validator(data):
    """Validate data for JSONAttrDict.

    This validator combines the logic from the following validators into one to
    make validation more efficient:

    This validator combines the following logic:
        - JSON format validation
        - Ensuring no dots are present in string keys

    Parameters
    ----------
    data
        Data to validate.

    Raises
    ------
    KeyTypeError
        If key data type is not supported.
    TypeError
        If the data type of ``data`` is not supported.

    """
    switch_type = _json_attr_dict_validator_type_resolver.get_type(data)

    if switch_type == "BASE":
        return
    elif switch_type == "MAPPING":
        # Explicitly call `list(keys)` to get a fixed list of keys to avoid
        # running into issues with iterating over a DictKeys view while
        # modifying the dict at the same time. Inside the loop, we:
        #   1) validate the key, converting to string if necessary
        #   2) pop and validate the value
        #   3) reassign the value to the (possibly converted) key
        for key in list(data):
            json_attr_dict_validator(data[key])
            if isinstance(key, str):
                if "." in key:
                    raise InvalidKeyError(
                        f"Mapping keys may not contain dots ('.'): {key}."
                    )
            else:
                raise KeyTypeError(
                    f"Mapping keys must be str, not {type(key).__name__}."
                )
    elif switch_type == "SEQUENCE":
        for value in data:
            json_attr_dict_validator(value)
    elif switch_type == "NUMPY":
        if _is_numpy_scalar(data.item()):
            raise TypeError("NumPy extended precision types are not JSON serializable.")
        elif _is_complex(data):
            raise TypeError("Complex numbers are not JSON serializable.")
    else:
        raise TypeError(
            f"Object of type {type(data).__name__} is not JSON serializable."
        )


"""
Here we define the main JSONCollection class that encapsulates most of the
logic for reading from and writing to JSON files. The remaining classes in
this file inherit from these classes to add features like buffering or
attribute-based dictionary access, each with a different backend name for
correct resolution of nested SyncedCollection types.
"""


class JSONCollection(SyncedCollection):
    r"""A :class:`~.SyncedCollection` that synchronizes with a JSON file.

    This collection implements synchronization by reading and writing the associated
    JSON file in its entirety for every read/write operation. This backend is a good
    choice for maximum accessibility and transparency since all data is immediately
    accessible in the form of a text file with no additional tooling, but is
    likely a poor choice for high performance applications.

    **Thread safety**

    The :class:`JSONCollection` is thread-safe on Unix-like systems (not
    Windows, see the Warnings section). To make these collections safe, the
    ``write_concern`` flag is ignored in multithreaded execution, and the write
    is **always** performed via a write to temporary file followed by a
    replacement of the original file. The file replacement operation uses
    :func:`os.replace`, which is guaranteed to be atomic by the Python
    standard.

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

    Warnings
    --------
    This class is _not_ thread safe on Windows. It relies on ``os.replace`` for
    atomic file writes, and that method can fail in multithreaded situations if
    open handles exist to the destination file within the same process on a
    different thread. See https://bugs.python.org/issue46003 for more
    information.
    """

    _backend = __name__  # type: ignore
    _supports_threading = not ON_WINDOWS
    _validators: Sequence_t[Callable] = (require_string_key, json_format_validator)

    def __init__(self, filename=None, write_concern=False, *args, **kwargs):
        # The `_filename` attribute _must_ be defined prior to calling the
        # superclass constructors because the filename defines the `_lock_id`
        # used to uniquely identify thread locks for this collection.
        self._filename = filename
        super().__init__(*args, **kwargs)
        self._write_concern = write_concern

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
        """str: Get or set the name of the associated JSON file on disk."""
        return self._filename

    @filename.setter
    def filename(self, value):
        # When setting the filename we must also remap the locks.
        with self._thread_lock:
            if type(self)._threading_support_is_active:
                old_lock_id = self._lock_id

            self._filename = value

            if type(self)._threading_support_is_active:
                type(self)._locks[self._lock_id] = type(self)._locks.pop(old_lock_id)

    @property
    def _lock_id(self):
        return self._filename


# These are the common protected keys used by all JSONDict types.
_JSONDICT_PROTECTED_KEYS = frozenset(
    (
        # These are all protected keys that are inherited from data type classes.
        "_data",
        "_name",
        "_suspend_sync_",
        "_load",
        "_sync",
        "_root",
        "_validators",
        "_all_validators",
        "_load_and_save",
        "_suspend_sync",
        "_supports_threading",
        "_LoadSaveType",
        "registry",
        # These keys are specific to the JSON backend.
        "_filename",
        "filename",
        "_write_concern",
    )
)


class JSONDict(JSONCollection, SyncedDict):
    r"""A dict-like data structure that synchronizes with a persistent JSON file.

    Examples
    --------
    >>> doc = JSONDict('data.json', write_concern=True)
    >>> doc['foo'] = "bar"
    >>> assert doc['foo'] == "bar"
    >>> assert 'foo' in doc
    >>> del doc['foo']
    >>> doc['foo'] = dict(bar=True)
    >>> doc
    {'foo': {'bar': True}}

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

    _PROTECTED_KEYS: FrozenSet[str] = _JSONDICT_PROTECTED_KEYS

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


"""
Here we define the BufferedJSONCollection class and its data type
subclasses, which augment the JSONCollection with a serialized in-memory
buffer for improved performance.
"""


class BufferedJSONCollection(SerializedFileBufferedCollection, JSONCollection):
    """A :class:`JSONCollection` that supports I/O buffering.

    This class implements the buffer protocol defined by
    :class:`~.BufferedCollection`. The concrete implementation of buffering
    behavior is defined by the :class:`~.SerializedFileBufferedCollection`.
    """

    _backend = __name__ + ".buffered"  # type: ignore


# These are the keys common to buffer backends.
_BUFFERED_PROTECTED_KEYS = frozenset(
    (
        "buffered",
        "_is_buffered",
        "_buffer_lock",
        "_buffer_context",
        "_buffered_collections",
    )
)


class BufferedJSONDict(BufferedJSONCollection, SyncedDict):
    """A buffered :class:`JSONDict`."""

    _PROTECTED_KEYS: FrozenSet[str] = (
        _JSONDICT_PROTECTED_KEYS | _BUFFERED_PROTECTED_KEYS
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


"""
Here we define the MemoryBufferedJSONCollection class and its data type
subclasses, which augment the JSONCollection with a serialized in-memory
buffer for improved performance.
"""


class MemoryBufferedJSONCollection(SharedMemoryFileBufferedCollection, JSONCollection):
    """A :class:`JSONCollection` that supports I/O buffering.

    This class implements the buffer protocol defined by :class:`~.BufferedCollection`.
    The concrete implementation of buffering behavior is defined by the
    :class:`~.SharedMemoryFileBufferedCollection`.
    """

    _backend = __name__ + ".memory_buffered"  # type: ignore


class MemoryBufferedJSONDict(MemoryBufferedJSONCollection, SyncedDict):
    """A buffered :class:`JSONDict`."""

    _PROTECTED_KEYS: FrozenSet[str] = (
        _JSONDICT_PROTECTED_KEYS | _BUFFERED_PROTECTED_KEYS
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


"""
Here we define various extensions of the above classes that add
attribute-based access to dictionaries. Although list behavior is not
modified in any way by these, they still require separate classes with the
right backend so that nested classes are created appropriately.
"""


class JSONAttrDict(JSONDict, AttrDict):
    r"""A dict-like data structure that synchronizes with a persistent JSON file.

    Unlike :class:`JSONDict`, this class also supports attribute-based access to
    dictionary contents, e.g. ``doc.foo == doc['foo']``.

    Examples
    --------
    >>> doc = JSONAttrDict('data.json', write_concern=True)
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
        The initial data passed to :class:`JSONAttrDict`. If ``None``, defaults to
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
    While the :class:`JSONAttrDict` object behaves like a :class:`dict`, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying file, copying (even deep copying) a :class:`JSONAttrDict`
    instance may exhibit unexpected behavior. If a true copy is required, you
    should use the call operator to get a dictionary representation, and if
    necessary construct a new :class:`JSONAttrDict` instance.

    """

    _backend = __name__ + ".attr"  # type: ignore
    # Define the validators in case subclasses want to inherit the correct
    # behavior, but define _all_validators for performance of this class.
    _validators = (no_dot_in_key,)
    _all_validators = (json_attr_dict_validator,)


class JSONAttrList(JSONList):
    """A :class:`JSONList` whose dict-like children will be of type :class:`JSONAttrDict`."""

    _backend = __name__ + ".attr"  # type: ignore


class BufferedJSONAttrDict(BufferedJSONDict, AttrDict):
    """A buffered :class:`JSONAttrDict`."""

    _backend = __name__ + ".buffered_attr"  # type: ignore
    # Define the validators in case subclasses want to inherit the correct
    # behavior, but define _all_validators for performance of this class.
    _validators = (no_dot_in_key,)
    _all_validators = (json_attr_dict_validator,)


class BufferedJSONAttrList(BufferedJSONList):
    """A :class:`BufferedJSONList` whose dict-like children will be of type :class:`BufferedJSONAttrDict`."""  # noqa: E501

    _backend = __name__ + ".buffered_attr"  # type: ignore


class MemoryBufferedJSONAttrDict(MemoryBufferedJSONDict, AttrDict):
    """A buffered :class:`JSONAttrDict`."""

    _backend = __name__ + ".memory_buffered_attr"  # type: ignore
    # Define the validators in case subclasses want to inherit the correct
    # behavior, but define _all_validators for performance of this class.
    _validators = (no_dot_in_key,)
    _all_validators = (json_attr_dict_validator,)


class MemoryBufferedJSONAttrList(MemoryBufferedJSONList):
    """A :class:`MemoryBufferedJSONList` whose dict-like children will be of type :class:`MemoryBufferedJSONAttrDict`."""  # noqa: E501

    _backend = __name__ + ".memory_buffered_attr"  # type: ignore
