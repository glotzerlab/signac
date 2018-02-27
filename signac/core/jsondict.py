# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Dict implementation with backend JSON file."
import os
import sys
import errno
import uuid
import logging
from contextlib import contextmanager

from .json import json
from .attrdict import SyncedAttrDict
from ..common import six


logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE = 32 * 2**20    # 32 MB

_BUFFER_MODE = 0
_BUFFER_SIZE = None
_JSONDICT_BUFFER = dict()


def flush_all():
    "Execute all deferred JSONDict write operations."
    logger.debug("Flushing buffer...")
    while _JSONDICT_BUFFER:
        filename, blob = _JSONDICT_BUFFER.popitem()
        with open(filename, 'wb') as file:
            file.write(blob)


def get_buffer_size():
    "Returns the current actual size of the read/write buffer."
    return sum((sys.getsizeof(x) for x in _JSONDICT_BUFFER.values()))


def in_buffer_mode():
    "Return true if in buffered read/write mode."
    return _BUFFER_MODE > 0


@contextmanager
def buffer_reads_writes(buffer_size=DEFAULT_BUFFER_SIZE):
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
    global _BUFFER_MODE
    global _BUFFER_SIZE
    assert _BUFFER_MODE >= 0

    if _BUFFER_SIZE is not None and _BUFFER_SIZE != buffer_size:
        logger.warn("Buffer size already set, ignoring new value.")
    else:
        _BUFFER_SIZE = buffer_size

    _BUFFER_MODE += 1
    try:
        yield
    finally:
        _BUFFER_MODE -= 1
        if _BUFFER_MODE == 0:
            _BUFFER_SIZE = None
            flush_all()


class JSONDict(SyncedAttrDict):

    def __init__(self, parent=None, filename=None, write_concern=False):
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        self._filename = None if filename is None else os.path.realpath(filename)
        self._write_concern = write_concern
        super(JSONDict, self).__init__(parent=parent)

    def _load(self):
        assert self._filename is not None

        if _BUFFER_MODE > 0 and self._filename in _JSONDICT_BUFFER:
            # Load from buffer:
            return json.loads(_JSONDICT_BUFFER[self._filename].decode())
        else:   # Load from disk:
            try:
                with open(self._filename, 'rb') as file:
                    return json.loads(file.read().decode())
            except IOError as error:
                if error.errno == errno.ENOENT:
                    return dict()

    def _save(self, data=None):
        assert self._filename is not None

        if data is None:
            data = self._as_dict()

        # Serialize data:
        blob = json.dumps(data).encode()

        if _BUFFER_MODE > 0 and (_BUFFER_SIZE < 0 or sys.getsizeof(blob) <= _BUFFER_SIZE):
            # Saving in buffer:
            if _BUFFER_SIZE > 0 and sys.getsizeof(blob) + get_buffer_size() > _BUFFER_SIZE:
                logger.debug("Buffer overflow, flushing...")
                flush_all()
            _JSONDICT_BUFFER[self._filename] = blob
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

    def __repr__(self):
        return repr(self())

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
