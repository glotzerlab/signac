# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import json
import errno
import uuid

from .collection_api import SyncedCollection
from .syncedattrdict import SyncedAttrDict
from .syncedlist import SyncedList


class JSONCollection(SyncedCollection):
    """Implement sync and load using a JSON back end."""

    backend = 'JSON'  # type: ignore

    def __init__(self, filename=None, parent=None, write_concern=False, **kwargs):
        self._parent = parent
        self._filename = os.path.realpath(filename) if filename is not None else None
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        self._write_concern = False
        super().__init__(**kwargs)

    def _load(self):
        "Loads the data from json file"
        try:
            with open(self._filename, 'rb') as file:
                blob = file.read()
                return json.loads(blob.decode())
        except IOError as error:
            if error.errno == errno.ENOENT:
                return None

    def _sync(self):
        "Write the data to json file"
        data = self.to_base()
        # Serialize data:
        blob = json.dumps(data).encode()

        if self._write_concern:
            dirname, filename = os.path.split(self._filename)
            fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
                uid=uuid.uuid4(), fn=filename))
            with open(fn_tmp, 'wb') as tmpfile:
                tmpfile.write(blob)
            os.replace(fn_tmp, self._filename)
        else:
            with open(self._filename, 'wb') as file:
                file.write(blob)


class JSONDict(JSONCollection, SyncedAttrDict):
    pass


class JSONList(JSONCollection, SyncedList):
    pass
