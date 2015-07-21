"""Dict implementation with backend json file.

Slightly adapted version of jsondict.
https://pypi.python.org/pypi/jsondict/1.2

I could not use the original, because it is not multi-processing
safe and uses a 3rd library 'asjson', instead of bson and had
some other weaknesses in implementation.
"""

import os
import tempfile

import bson.json_util as json

class JSonDict(object):

    def __init__(self, filename, synchronized=False, write_concern = False):
        self._dict = dict()
        self._synchronized = synchronized
        self._filename = filename
        self._write_concern = write_concern

    def __setitem__(self, key, value):
        self._dict[key] = value
        if self._synchronized:
            self.save()

    def __getitem__(self, key):
        if self._synchronized:
            self.load()
        return self._dict[key]

    def get(self, key, default=None):
        if self._synchronized:
            self.load()
        return self._dict.get(key, default)

    def __delitem__(self, key):
        del self._dict[key]
        if self._synchronized:
            self.save()

    def clear(self):
        self._dict.clear()
        if self._synchronized:
            self.save()

    def update(self, *args, **kwargs):
        self._dict.update(*args, **kwargs)
        if self._synchronized:
            self.save()

    def load(self):
        try:
            with open(self._filename, 'rb') as file:
                self._dict.update(json.loads(file.read().decode()))
        except FileNotFoundError:
            pass

    def _dump(self):
        return json.dumps(self._dict)

    def _save(self):
        with open(self._filename, 'wb') as file:
            file.write(self._dump().encode())

    def _save_with_concern(self):
        dirname, filename = os.path.split(self._filename)
        fn_tmp = os.path.join(dirname, '.' + filename)
        with open(fn_tmp, 'xb') as tmpfile:
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
        return len(self._dict)
