# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Dict implementation with backend JSON file."""
import errno
import hashlib
import logging
import os
import sys
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from copy import copy
from tempfile import mkstemp

from . import json
from .attrdict import SyncedAttrDict
from .errors import Error

logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE = 32 * 2 ** 20  # 32 MB

_BUFFERED_MODE = 0
_BUFFERED_MODE_FORCE_WRITE = None
_BUFFER_SIZE = None
_BUFFER_LOAD = 0
_JSONDICT_BUFFER = {}
_JSONDICT_HASHES = {}
_JSONDICT_META = {}


class BufferException(Error):
    """An exception occurred in buffered mode."""

    pass


class BufferedFileError(BufferException):
    """Raised when an error occurred while flushing one or more buffered files.

    .. attribute:: files

        A dictionary of files that caused issues during the flush operation,
        mapped to a possible reason for the issue or None in case that it
        cannot be determined.
    """

    def __init__(self, files):
        self.files = files

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.files)


def _hash(blob):
    """Calculate and return the md5 hash value for the file data."""
    if blob is not None:
        m = hashlib.md5()
        m.update(blob)
        return m.hexdigest()


def _get_file_metadata(filename):
    try:
        return os.path.getsize(filename), os.path.getmtime(filename)
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise


def _store_in_buffer(filename, blob, store_hash=False):
    global _BUFFER_LOAD
    assert _BUFFERED_MODE > 0
    blob_size = sys.getsizeof(blob)
    if _BUFFER_SIZE > 0:
        if blob_size > _BUFFER_SIZE:
            # Cannot store blobs larger than the buffer size
            return False
        elif blob_size + _BUFFER_LOAD > _BUFFER_SIZE:
            logger.debug("Buffer overflow, flushing...")
            flush_all()

    _JSONDICT_BUFFER[filename] = blob
    _BUFFER_LOAD += blob_size
    if store_hash:
        if not _BUFFERED_MODE_FORCE_WRITE:
            _JSONDICT_META[filename] = _get_file_metadata(filename)
        _JSONDICT_HASHES[filename] = _hash(blob)

    return True


def flush_all():
    """Execute all deferred JSONDict write operations."""
    global _BUFFER_LOAD
    logger.debug("Flushing buffer...")
    issues = {}
    while _JSONDICT_BUFFER:
        filename, blob = _JSONDICT_BUFFER.popitem()
        if not _BUFFERED_MODE_FORCE_WRITE:
            meta = _JSONDICT_META.pop(filename)
        if _hash(blob) != _JSONDICT_HASHES.pop(filename):
            try:
                if not _BUFFERED_MODE_FORCE_WRITE:
                    if _get_file_metadata(filename) != meta:
                        issues[
                            filename
                        ] = "File appears to have been externally modified."
                        continue
                try:
                    fd_tmp, fn_tmp = mkstemp(
                        dir=os.path.dirname(filename), suffix=".json"
                    )
                    with os.fdopen(fd_tmp, "wb") as file:
                        file.write(blob)
                except OSError:
                    os.remove(fn_tmp)
                    raise
                else:
                    os.replace(fn_tmp, filename)
            except OSError as error:
                logger.error(str(error))
                issues[filename] = error
    if issues:
        raise BufferedFileError(issues)
    _BUFFER_LOAD = 0


def get_buffer_size():
    """Return the current maximum size of the read/write buffer."""
    return _BUFFER_SIZE


def get_buffer_load():
    """Return the current actual size of the read/write buffer."""
    return _BUFFER_LOAD


def in_buffered_mode():
    """Return true if in buffered read/write mode."""
    return _BUFFERED_MODE > 0


@contextmanager
def buffer_reads_writes(buffer_size=DEFAULT_BUFFER_SIZE, force_write=False):
    """Enter a global buffer mode for all JSONDict instances.

    All future write operations are written to the buffer, read
    operations are performed from the buffer whenever possible.

    All write operations are deferred until the flush_all() function
    is called, the buffer overflows, or upon exiting the buffer mode.

    This context may be entered multiple times, however the buffer size
    can only be set *once*. Any subsequent specifications of the buffer
    size are ignored.

    :param buffer_size:
        Specify the maximum size of the read/write buffer. Defaults
        to DEFAULT_BUFFER_SIZE. A negative number indicates to not
        restrict the buffer size.
    :type buffer_size:
        int
    """
    global _BUFFERED_MODE
    global _BUFFERED_MODE_FORCE_WRITE
    global _BUFFER_SIZE
    global _BUFFER_LOAD
    assert _BUFFERED_MODE >= 0

    # Basic type check (to prevent common user error)
    if (
        not isinstance(buffer_size, int) or buffer_size is True or buffer_size is False
    ):  # explicit check against boolean
        raise TypeError("The buffer size must be an integer!")

    # Can't enter force write mode, if already in non-force write mode:
    if _BUFFERED_MODE_FORCE_WRITE is not None and (
        force_write and not _BUFFERED_MODE_FORCE_WRITE
    ):
        raise BufferException(
            "Unable to enter buffered mode with force write enabled, because "
            "we are already in buffered mode with force write disabled."
        )

    # Check whether we can adjust the buffer size and warn otherwise:
    if _BUFFER_SIZE is not None and _BUFFER_SIZE != buffer_size:
        raise BufferException("Buffer size already set, unable to change its size!")

    _BUFFER_SIZE = buffer_size
    _BUFFERED_MODE_FORCE_WRITE = force_write

    _BUFFERED_MODE += 1
    try:
        yield
    finally:
        _BUFFERED_MODE -= 1
        if _BUFFERED_MODE == 0:
            try:
                flush_all()
            finally:
                assert not _JSONDICT_BUFFER
                assert not _JSONDICT_HASHES
                assert not _JSONDICT_META
                _BUFFER_SIZE = None
                _BUFFER_LOAD = 0
                _BUFFERED_MODE_FORCE_WRITE = None


class JSONDict(SyncedAttrDict):
    """A dict-like mapping interface to a persistent JSON file.

    The JSONDict is a :class:`~collections.abc.MutableMapping` and therefore
    behaves similarly to a :class:`dict`, but all data is stored persistently
    in the associated JSON file on disk.

    .. code-block:: python

        doc = JSONDict('data.json', write_concern=True)
        doc['foo'] = "bar"
        assert doc.foo == doc['foo'] == "bar"
        assert 'foo' in doc
        del doc['foo']

    This class allows access to values through key indexing or attributes
    named by keys, including nested keys:

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
        true copy is required, you should use the ``_as_dict()`` method to get
        a dictionary representation, and if necessary construct a new JSONDict
        instance: ``new_dict = JSONDict(old_dict._as_dict())``.

    :param filename:
        The filename of the associated JSON file on disk.
    :param write_concern:
        Ensure file consistency by writing changes back to a temporary file
        first, before replacing the original file. Default is False.
    :param parent:
        A parent instance of JSONDict or None.
    """

    def __init__(self, filename=None, write_concern=False, parent=None):
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both."
            )
        self._filename = None if filename is None else os.path.realpath(filename)
        self._write_concern = write_concern
        super().__init__(parent=parent)

    def _load_from_disk(self):
        try:
            with open(self._filename, "rb") as file:
                return file.read()
        except OSError as error:
            if error.errno == errno.ENOENT:
                return None

    def _load(self):
        assert self._filename is not None

        if _BUFFERED_MODE > 0:
            if self._filename in _JSONDICT_BUFFER:
                # Load from buffer:
                blob = _JSONDICT_BUFFER[self._filename]
            else:
                # Load from disk and store in buffer
                blob = self._load_from_disk()
                _store_in_buffer(self._filename, blob, store_hash=True)
        else:
            # Just load from disk
            blob = self._load_from_disk()

        return {} if blob is None else json.loads(blob.decode())

    def _save(self, data=None):
        assert self._filename is not None

        if data is None:
            data = self._as_dict()

        # Serialize data:
        blob = json.dumps(data).encode()

        if _BUFFERED_MODE > 0:
            _store_in_buffer(self._filename, blob)
        else:  # Saving to disk:
            if self._write_concern:
                dirname, filename = os.path.split(self._filename)
                fn_tmp = os.path.join(dirname, f"._{uuid.uuid4()}_{filename}")
                with open(fn_tmp, "wb") as tmpfile:
                    tmpfile.write(blob)
                os.replace(fn_tmp, self._filename)
            else:
                with open(self._filename, "wb") as file:
                    file.write(blob)

    def reset(self, data):
        """Replace the document contents with data."""
        if isinstance(data, Mapping):
            with self._suspend_sync():
                backup = copy(self._data)
                try:
                    self._data = {
                        self._validate_key(k): self._dfs_convert(v)
                        for k, v in data.items()
                    }
                    self._save()
                except BaseException:  # rollback
                    self._data = backup
                    raise
        else:
            raise ValueError("The document must be a mapping.")

    @contextmanager
    def buffered(self):
        """Context manager for buffering read and write operations.

        This context manager activates the "buffered" mode, which
        means that all read operations are cached, and all write operations
        are deferred until the buffered mode is deactivated.
        """
        buffered_dict = BufferedSyncedAttrDict(self, parent=self)
        yield buffered_dict
        buffered_dict.flush()


class BufferedSyncedAttrDict(SyncedAttrDict):
    """Buffered :class:`~.SyncedAttrDict`.

    Saves all changes in memory but does not write them to disk until :meth:`~.flush` is called.
    """

    def load(self):  # noqa: D102
        pass

    def save(self):  # noqa: D102
        pass

    def flush(self):
        """Save buffered changes to the underlying file."""
        self._parent._save(self())
