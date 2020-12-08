# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.


class Error(Exception):
    pass


class BufferException(Error):
    """An exception occured in buffered mode."""


class BufferedError(BufferException):
    """Raised when an error occured while flushing one or more buffered files.

    Attribute
    ---------
    files:
        A dictionary of names that caused issues during the flush operation,
        mapped to a possible reason for the issue or None in case that it
        cannot be determined.
    """

    def __init__(self, files):
        self.files = files

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.files)


class MetadataError(BufferException):
    """Raised when metadata check fails."""

    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return f'{self.filename} appears to have been externally modified.'
