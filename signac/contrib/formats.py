# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging

logger = logging.getLogger(__name__)


class BasicFormat(object):
    pass


class FileFormat(BasicFormat):

    def __init__(self, file_object):
        self._file_object = file_object

    @property
    def data(self):
        return self.read()

    def read(self, size=-1):
        return self._file_object.read(size)

    def seek(self, offset):
        return self._file_object.seek(offset)

    def tell(self):
        return self._file_object.tell()

    def __iter__(self):
        return iter(self._file_object)

    def close(self):
        return self._file_object.close()


class TextFile(FileFormat):
    pass
