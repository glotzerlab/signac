# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Errors raised by signac.common classes."""
from ..core.errors import Error


class ConfigError(Error, RuntimeError):
    """Error with parsing or reading a configuration file."""

    pass


# this class is only used by deprecated features
class AuthenticationError(Error, RuntimeError):
    """Authentication error."""

    def __str__(self):
        if len(self.args) > 0:
            return "Failed to authenticate with host '{}'.".format(self.args[0])
        else:
            return "Failed to authenticate with host."


# this class is only used by deprecated features
class ExportError(Error, RuntimeError):
    """Error exporting documents to a mirror."""

    pass


# this class is only used by deprecated features
class FetchError(FileNotFoundError):
    """Error in fetching data."""

    pass
