# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Dict implementation with backend JSON file."
import os
import errno
import uuid
from contextlib import contextmanager
from weakref import WeakValueDictionary

from .json import json
from .attrdict import SyncedAttrDict
from ..common import six


_BUFFER_READS_WRITES = 0
_SUSPENDED_READS = WeakValueDictionary()
_DEFERRED_WRITES = WeakValueDictionary()


def clear_suspended_reads():
    "Clear the set of dictionaries, that are suspended for read operations."
    _SUSPENDED_READS.clear()


def flush_all():
    "Execute all deferred JSONDict write operations."
    while _DEFERRED_WRITES:
        _id, deferred = _DEFERRED_WRITES.popitem()
        deferred.save()


@contextmanager
def buffer_reads_writes():
    """Enter a global buffer mode for all JSONDict instances.

    All future read operations will be suspended after the first
    read operation while in buffer mode.

    All write operations are deferred until after leaving the buffer mode.
    """
    global _BUFFER_READS_WRITES
    assert _BUFFER_READS_WRITES >= 0

    _BUFFER_READS_WRITES += 1
    yield
    _BUFFER_READS_WRITES -= 1
    if _BUFFER_READS_WRITES == 0:
        clear_suspended_reads()
        flush_all()


class JSONDict(SyncedAttrDict):

    def __init__(self, parent=None, filename=None, write_concern=False):
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        self._filename = filename
        self._write_concern = write_concern
        super(JSONDict, self).__init__(parent=parent)

    def __hash__(self):
        pass

    def _load(self):
        assert self._filename is not None

        # Check deferrence
        if _BUFFER_READS_WRITES > 0:
            if id(self) in _SUSPENDED_READS:
                return
            else:
                _SUSPENDED_READS[id(self)] = self

        try:
            with open(self._filename, 'rb') as file:
                return json.loads(file.read().decode())
        except IOError as error:
            if error.errno == errno.ENOENT:
                return dict()

    def _save(self):
        assert self._filename is not None

        # Check deferrence
        if _BUFFER_READS_WRITES > 0:
            _DEFERRED_WRITES[id(self)] = self
            return

        # Serialize data:
        blob = json.dumps(self._as_dict()).encode()

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
