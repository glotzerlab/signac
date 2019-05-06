# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Dict implementation with backend JSON file."
import os
import sys
import errno
import uuid
import hashlib
import logging
from tempfile import mkstemp
from contextlib import contextmanager

from .errors import Error
from . import json
from .attrdict import SyncedAttrDict
from ..common import six


logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE = 32 * 2**20    # 32 MB

_BUFFERED_MODE = 0
_BUFFERED_MODE_FORCE_WRITE = None
_BUFFER_SIZE = None
_JSONDICT_BUFFER = dict()
_JSONDICT_HASHES = dict()
_JSONDICT_META = dict()


class BufferException(Error):
    "An exception occured in buffered mode."
    pass


class BufferedFileError(BufferException):
    """Raised when an error occured while flushing one or more buffered files.

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
    "Calculate and return the md5 hash value for the file data."
    if blob is not None:
        m = hashlib.md5()
        m.update(blob)
        return m.hexdigest()


def _get_filemetadata(filename):
    try:
        return os.path.getsize(filename), os.path.getmtime(filename)
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise


def _store_in_buffer(filename, blob, store_hash=False):
    assert _BUFFERED_MODE > 0
    blob_size = sys.getsizeof(blob)
    buffer_load = get_buffer_load()
    if _BUFFER_SIZE > 0:
        if blob_size > _BUFFER_SIZE:
            return False
        elif blob_size + buffer_load > _BUFFER_SIZE:
            logger.debug("Buffer overflow, flushing...")
            flush_all()

    _JSONDICT_BUFFER[filename] = blob
    if store_hash:
        if not _BUFFERED_MODE_FORCE_WRITE:
            _JSONDICT_META[filename] = _get_filemetadata(filename)
        _JSONDICT_HASHES[filename] = _hash(blob)
    return True


def flush_all():
    "Execute all deferred JSONDict write operations."
    logger.debug("Flushing buffer...")
    issues = dict()
    while _JSONDICT_BUFFER:
        filename, blob = _JSONDICT_BUFFER.popitem()
        if not _BUFFERED_MODE_FORCE_WRITE:
            meta = _JSONDICT_META.pop(filename)
        if _hash(blob) != _JSONDICT_HASHES.pop(filename):
            try:
                if not _BUFFERED_MODE_FORCE_WRITE:
                    if _get_filemetadata(filename) != meta:
                        issues[filename] = 'File appears to have been externally modified.'
                        continue
                try:
                    fd_tmp, fn_tmp = mkstemp(dir=os.path.dirname(filename), suffix='.json')
                    with os.fdopen(fd_tmp, 'wb') as file:
                        file.write(blob)
                except OSError:
                    os.remove(fn_tmp)
                    raise
                else:
                    if six.PY2:
                        os.rename(fn_tmp, filename)
                    else:
                        os.replace(fn_tmp, filename)
            except OSError as error:
                logger.error(str(error))
                issues[filename] = error
    if issues:
        raise BufferedFileError(issues)


def get_buffer_size():
    "Returns the current maximum size of the read/write buffer."
    return _BUFFER_SIZE


def get_buffer_load():
    "Returns the current actual size of the read/write buffer."
    return sum((sys.getsizeof(x) for x in _JSONDICT_BUFFER.values()))


def in_buffered_mode():
    "Return true if in buffered read/write mode."
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
    assert _BUFFERED_MODE >= 0

    # Basic type check (to prevent common user error)
    if not isinstance(buffer_size, six.integer_types) or \
            buffer_size is True or buffer_size is False:    # explicit check against boolean
        raise TypeError("The buffer size must be an integer!")

    # Can't enter force write mode, if already in non-force write mode:
    if _BUFFERED_MODE_FORCE_WRITE is not None and (force_write and not _BUFFERED_MODE_FORCE_WRITE):
        raise BufferException(
            "Unable to enter buffered mode with force write enabled, because "
            "we are already in buffered mode with force write disabled.")

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
                _BUFFERED_MODE_FORCE_WRITE = None


class JSONDict(SyncedAttrDict):
    """A dict-like mapping interface to a persistent JSON file.

    The JSONDict is a :class:`~collections.abc.MutableMapping` and therefore
    behaves similar to a :class:`dict`, but all data is stored persistently in
    the associated JSON file on disk.

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
        true copy is required, you should use the `_as_dict` method to get a
        dictionary representation, and if necessary construct a new JSONDict
        instance: `new_dict = JSONDict(old_dict._as_dict())`.

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
                "parent or filename must be None, but not both.")
        self._filename = None if filename is None else os.path.realpath(filename)
        self._write_concern = write_concern
        super(JSONDict, self).__init__(parent=parent)

    def _load_from_disk(self):
        try:
            with open(self._filename, 'rb') as file:
                return file.read()
        except IOError as error:
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

        return dict() if blob is None else json.loads(blob.decode())

    def _save(self, data=None):
        assert self._filename is not None

        if data is None:
            data = self._as_dict()

        # Serialize data:
        blob = json.dumps(data).encode()

        if _BUFFERED_MODE > 0:
            _store_in_buffer(self._filename, blob)
        else:   # Saving to disk:
            if self._write_concern:
                dirname, filename = os.path.split(self._filename)
                fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
                    uid=uuid.uuid4(), fn=filename))
                with open(fn_tmp, 'wb') as tmpfile:
                    tmpfile.write(blob)
                if six.PY2:
                    os.rename(fn_tmp, self._filename)
                else:
                    os.replace(fn_tmp, self._filename)
            else:
                with open(self._filename, 'wb') as file:
                    file.write(blob)

    @contextmanager
    def buffered(self):
        buffered_dict = BufferedSyncedAttrDict(self, parent=self)
        yield buffered_dict
        buffered_dict.flush()


class BufferedSyncedAttrDict(SyncedAttrDict):

    def load(self):
        pass

    def save(self):
        pass

    def flush(self):
        self._parent._save(self())
