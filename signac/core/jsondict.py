"""Dict implementation with backend json file.

Slightly adapted version of jsondict.
https://pypi.python.org/pypi/jsondict/1.2

I could not use the original, because it is not multi-processing
safe and uses a 3rd library 'asjson', instead of bson and had
some other weaknesses in implementation.
"""

import os
import collections
import logging
import uuid

try:
    import bson.json_util as json
except ImportError:
    import json

logger = logging.getLogger(__name__)


class JSonDict(collections.UserDict):

    def __init__(self, filename, synchronized=False, write_concern=False):
        self.data = dict()
        self._filename = filename
        self._synchronized = synchronized
        self._write_concern = write_concern

    def __setitem__(self, key, value):
        if self._synchronized:
            self.load()
            self.data[key] = value
            self.save()
        else:
            self.data[key] = value

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
        except FileNotFoundError:
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
