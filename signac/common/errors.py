# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from . import six
from ..core.errors import Error


class ConfigError(Error, RuntimeError):
    pass


class AuthenticationError(Error, RuntimeError):

    def __str__(self):
        if len(self.args) > 0:
            return "Failed to authenticate with host '{}'.".format(
                self.args[0])
        else:
            return "Failed to authenticate with host."


class ExportError(Error, RuntimeError):
    pass


if six.PY2:
    class FileNotFoundError(Error, IOError):
        pass
else:
    class FileNotFoundError(Error, FileNotFoundError):
        pass


class FetchError(FileNotFoundError):
    pass
