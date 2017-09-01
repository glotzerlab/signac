# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Dict implementation with backend json file."
import os
import errno
import logging
import uuid
from contextlib import contextmanager

from .json import json
from ..common import six

if six.PY2:
    from UserDict import UserDict as UD
    from collections import Mapping
else:
    from collections import UserDict
    from collections.abc import Mapping


logger = logging.getLogger(__name__)

if six.PY2:
    class UserDict(UD, object):  # noqa
        pass


def _convert_to_dict(m):
    "Convert (nested) values of AttrDict to dict."
    ret = dict()
    if isinstance(m, _SyncedDict):
        for k in m:
            ret[k] = _convert_to_dict(m[k])
    elif isinstance(m, Mapping):
        for k, v in m.items():
            ret[k] = _convert_to_dict(v)
    elif isinstance(m, list):
        return [_convert_to_dict(x) for x in m]
    else:
        return m
    return ret


def _convert_to_synced_dict(m, parent):
    "Convert (nested) values of AttrDict to dict."
    ret = _SyncedDict(parent)
    if isinstance(m, Mapping):
        for k in m:
            ret[k] = _convert_to_synced_dict(m[k], ret)
    elif isinstance(m, list):
        return [_convert_to_synced_dict(i) for i in m]
    else:
        return m
    return ret


def _update_synced_dict(orig, new):
    assert isinstance(orig, Mapping)
    assert isinstance(new, Mapping)
    for k in new:
        if k in orig and isinstance(orig[k], Mapping) and isinstance(new[k], Mapping):
            _update_synced_dict(orig[k], new[k])
        else:
            orig[k] = _convert_to_synced_dict(new[k], orig)


class _SyncedDict(UserDict):

    def __init__(self, parent=None, initialdata=None):
        self._parent = None
        super(_SyncedDict, self).__init__(initialdata)
        self._parent = parent

    @contextmanager
    def _suspend_sync(self):
        parent = self._parent
        self._parent = None
        yield
        self._parent = parent

    def load(self):
        if self._parent is not None:
            self._parent.load()

    def save(self):
        if self._parent is not None:
            self._parent.save()

    def __setitem__(self, key, value):
        self.load()
        with self._suspend_sync():
            if isinstance(value, Mapping):
                value = _SyncedDict(self, value)
            self.data[key] = value
        self.save()
        return value

    def __getitem__(self, key):
        self.load()
        return self.data[key]

    def get(self, key, default=None):
        self.load()
        return self.data.get(key, default)

    def __delitem__(self, key):
        self.load()
        del self.data[key]
        self.save()

    def clear(self):
        with self._suspend_sync():
            self.data.clear()
        self.save()

    def update(self, mapping):
        self.load()
        for key in mapping:
            self.data[key] = mapping[key]
        self.save()

    def __len__(self):
        self.load()
        return len(self.data)

    def __containts__(self, key):
        self.load()
        return key in self.data

    def __iter__(self):
        self.load()
        for d in self.data:
            yield d

    def __str__(self):
        self.load()
        return super(_SyncedDict, self).__str__()

    def __repr__(self):
        self.load()
        return super(_SyncedDict, self).__repr__()


class JSonDict(UserDict):

    def __init__(self, filename, synchronized=False, write_concern=False):
        self._filename = filename
        self._write_concern = write_concern
        self._synchronized = synchronized
        if synchronized:
            self.data = _SyncedDict(self)
        else:
            self.data = _SyncedDict(None)
        self.load()

    def load(self):
        try:
            logger.debug("Loading from file '{}'.".format(self._filename))
            with open(self._filename, 'rb') as file:
                data = json.loads(file.read().decode())
                with self.data._suspend_sync():
                    _update_synced_dict(self.data, data)
        except ValueError:
            logger.critical(
                "Document file '{}' seems to be corrupted! Unable "
                "to load document.".format(self._filename))
            raise
        except IOError as error:
            if not error.errno == errno.ENOENT:
                raise
            pass

    def _dump(self):
        return json.dumps(_convert_to_dict(self.data))

    def _save(self):
        logger.debug("Storing to '{}'.".format(self._filename))
        with open(self._filename, 'wb') as file:
            file.write(self._dump().encode())

    def _save_with_concern(self):
        logger.debug(
            "Storing with write concern to '{}'.".format(self._filename))
        dirname, filename = os.path.split(self._filename)
        fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
            uid=uuid.uuid4(), fn=filename))
        with open(fn_tmp, 'wb') as tmpfile:
            tmpfile.write(self._dump().encode())
        if six.PY2:
            os.rename(fn_tmp, self._filename)
        else:
            os.replace(fn_tmp, self._filename)

    def save(self):
        with self.data._suspend_sync():
            if self._write_concern:
                return self._save_with_concern()
            else:
                return self._save()

    def __len__(self):
        return len(self.data)

    def __contains__(self, key):
        return key in self.data

    def __iter__(self):
        for d in self.data:
            yield d

    def __str__(self):
        return super(JSonDict, self).__str__()

    def __repr__(self):
        return super(JSonDict, self).__repr__()

    def clear(self):
        self.data.clear()
