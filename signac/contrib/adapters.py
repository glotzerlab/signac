# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from . import conversion
from .formats import FileFormat

conversion.make_adapter(int, float)
conversion.make_adapter(float, int)
conversion.make_adapter(int, str)
conversion.make_adapter(str, int)
conversion.make_adapter(float, str)
conversion.make_adapter(str, float)
conversion.make_adapter(int, bool)
conversion.make_adapter(float, bool)


class FileToBytesAdapter(conversion.Adapter):
    expects = FileFormat
    returns = bytes

    def convert(self, x):
        return x._data


class BytesToStrAdapter(conversion.Adapter):
    expects = bytes
    returns = str

    def convert(self, x):
        return x.decode()


class StrToBytesAdapter(conversion.Adapter):
    expects = str
    returns = bytes

    def convert(self, x):
        return x.encode()
