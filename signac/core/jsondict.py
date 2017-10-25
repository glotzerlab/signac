# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Dict implementation with backend JSON file."
import os
import errno
import uuid

from .json import json
from .attrdict import SyncedAttrDict
from ..common import six


class JSONDict(SyncedAttrDict):

    def __init__(self, parent=None, filename=None, write_concern=False):
        if (filename is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or filename must be None, but not both.")
        self._filename = filename
        self._write_concern = write_concern
        super(JSONDict, self).__init__(parent=parent)

    def _load(self):
        assert self._filename is not None
        try:
            with open(self._filename, 'rb') as file:
                return json.loads(file.read().decode())
        except IOError as error:
            if error.errno == errno.ENOENT:
                return dict()

    def _save(self):
        assert self._filename is not None
        if self._write_concern:
            dirname, filename = os.path.split(self._filename)
            fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
                uid=uuid.uuid4(), fn=filename))
            with open(fn_tmp, 'wb') as tmpfile:
                tmpfile.write(json.dumps(self._as_dict()).encode())
            if six.PY2:
                os.rename(fn_tmp, self._filename)
            else:
                os.replace(fn_tmp, self._filename)
        else:
            with open(self._filename, 'wb') as file:
                file.write(json.dumps(self._as_dict()).encode())

    def __repr__(self):
        return repr(self())
