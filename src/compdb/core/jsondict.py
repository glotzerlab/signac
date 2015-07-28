"""Dict implementation with backend json file.

Slightly adapted version of jsondict.
https://pypi.python.org/pypi/jsondict/1.2

I could not use the original, because it is not multi-processing
safe and uses a 3rd library 'asjson', instead of bson and had
some other weaknesses in implementation.
"""

import os
import tempfile
import collections
import logging

import bson.json_util as json

logger = logging.getLogger(__name__)

class JSonDict(collections.UserDict):

    def __init__(self, filename, synchronized=False, write_concern=False):
        self.data = dict()
        self._filename = filename
        self._synchronized = synchronized
        self._write_concern = write_concern

    def __setitem__(self, key, value):
        self.data[key] = value
        if self._synchronized:
            self.save()

    def __getitem__(self, key):
        if self._synchronized:
            self.load()
        return self.data[key]

    def get(self, key, default=None):
        if self._synchronized:
            self.load()
        return self.data.get(key, default)

    def __delitem__(self, key):
        del self.data[key]
        if self._synchronized:
            self.save()

    def clear(self):
        self.data.clear()
        if self._synchronized:
            self.save()

    def update(self, mapping):
        for key in mapping:
            self.data[key] = mapping[key]
        if self._synchronized:
            self.save()

    def load(self):
        try:
            logger.debug("Loading from file '{}'.".format(self._filename))
            with open(self._filename, 'rb') as file:
                self.data.update(json.loads(file.read().decode()))
        except FileNotFoundError:
            pass

    def _dump(self):
        return json.dumps(self.data)

    def _save(self):
        with open(self._filename, 'wb') as file:
            file.write(self._dump().encode())

    def _save_with_concern(self):
        logger.debug("Storing with write concern to '{}'.".format(self._filename))
        dirname, filename = os.path.split(self._filename)
        fn_tmp = os.path.join(dirname, '.' + filename)
        with open(fn_tmp, 'wb') as tmpfile:
            tmpfile.write(self._dump().encode())
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
        yield from self.data

    def __str__(self):
        if self._synchronized:
            self.load()
        return super(JSonDict, self).__str__()

    def __repr__(self):
        if self._synchronized:
            self.load()
        return super(JSonDict, self).__repr__()
