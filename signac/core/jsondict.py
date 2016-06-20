# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Dict implementation with backend json file."
import os
import errno
import logging
import uuid

from ..common import six

if six.PY2:
    from UserDict import UserDict as UD
else:
    from collections import UserDict

try:
    import bson.json_util as json
except ImportError:
    import json

logger = logging.getLogger(__name__)

if not six.PY3:
    class UserDict(UD, object):  # noqa
        pass


class JSonDict(UserDict):

    def __init__(self, filename, synchronized=False, write_concern=False):
        self.data = dict()
        self._filename = filename
        self._synchronized = synchronized
        self._write_concern = write_concern
        if self._synchronized:
            self.load()

    def __setitem__(self, key, value):
        if self._synchronized:
            self.load()
            self.data[key] = value
            self.save()
        else:
            self.data[key] = value
        return value

    def __getitem__(self, key):
        if self._synchronized:
            self.load()
        return self.data[key]

    def get(self, key, default=None):
        if self._synchronized:
            self.load()
        return self.data.get(key, default)

    def __delitem__(self, key):
        if self._synchronized:
            self.load()
            del self.data[key]
            self.save()
        else:
            del self.data[key]

    def clear(self):
        self.data.clear()
        if self._synchronized:
            self.save()

    def update(self, mapping):
        if self._synchronized:
            self.load()
            for key in mapping:
                self.data[key] = mapping[key]
            self.save()
        else:
            for key in mapping:
                self.data[key] = mapping[key]

    def load(self):
        try:
            logger.debug("Loading from file '{}'.".format(self._filename))
            with open(self._filename, 'rb') as file:
                self.data.clear()
                self.data.update(json.loads(file.read().decode()))
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
        return json.dumps(self.data)

    def _save(self):
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
        if self._write_concern:
            return self._save_with_concern()
        else:
            return self._save()

    def __len__(self):
        if self._synchronized:
            self.load()
        return len(self.data)

    def __contains__(self, key):
        if self._synchronized:
            self.load()
        return key in self.data

    def __iter__(self):
        if self._synchronized:
            self.load()
        for d in self.data:
            yield d

    def __str__(self):
        if self._synchronized:
            self.load()
        return super(JSonDict, self).__str__()

    def __repr__(self):
        if self._synchronized:
            self.load()
        return super(JSonDict, self).__repr__()
