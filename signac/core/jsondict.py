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
    "Returns the current read/write buffer size."
    return sum((sys.getsizeof(x) for x in _JSONDICT_BUFFER.values()))


def in_buffer_mode():
    "Return true if in buffered read/write mode."
    return _BUFFER_MODE > 0


@contextmanager
def buffer_reads_writes(buffer_size=DEFAULT_BUFFER_SIZE):
    """Enter a global buffer mode for all JSONDict instances.

    All future read operations will be suspended after the first
    read operation while in buffer mode.

    All write operations are deferred until after leaving the buffer mode.
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

    def __hash__(self):
        pass

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

    def _save(self):
        assert self._filename is not None

        # Serialize data:
        blob = json.dumps(self._as_dict()).encode()

        if _BUFFER_MODE > 0 and sys.getsizeof(blob) <= _BUFFER_SIZE:
            # Saving in buffer:
            if sys.getsizeof(blob) + get_buffer_size() > _BUFFER_SIZE:
                logger.debug("Buffer size exceeds limit, flushing...")
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
        self._parent.update(self)
