# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Errors raised by signac."""

# The subpackage error modules (e.g. signac.core.errors) are used to bundle
# exceptions that are relevant beyond a single module. This top-level errors
# module is used to expose user-facing exception classes.

from .core.errors import Error

from .core.jsondict import BufferException
from .core.jsondict import BufferedFileError

from .common.errors import ConfigError
from .common.errors import AuthenticationError
from .common.errors import ExportError
from .common.errors import FetchError

from .contrib.errors import DestinationExistsError
from .contrib.errors import JobsCorruptedError
from .contrib.errors import IncompatibleSchemaVersion


class SyncConflict(Error, RuntimeError):
    """Raised when a synchronization operation fails."""
    pass


class FileSyncConflict(SyncConflict):
    """Raised when a synchronization operation fails due to a file conflict."""
    def __init__(self, filename):
        self.filename = filename
        "The filename of the file that caused the conflict."

    def __str__(self):
        return "The file with filename '{}' caused a conflict.".format(self.filename)


class DocumentSyncConflict(SyncConflict):
    """Raised when a synchronization operation fails due to a document conflict."""
    def __init__(self, keys):
        self.keys = keys
        "The keys that caused the conflict."

    def __str__(self):
        return "The following keys caused a conflict: {}".format(', '.join(self.keys))


class SchemaSyncConflict(SyncConflict):
    """Raised when a synchronization operation fails due to schema differences."""
    def __init__(self, schema_src, schema_dst):
        self.schema_src = schema_src
        self.schema_dst = schema_dst

    def __str__(self):
        return "The synchronization failed, because of a schema conflict."


class InvalidKeyError(ValueError):
    """Raised when a user uses a non-conforming key."""


class KeyTypeError(TypeError):
    """Raised when a user uses a key of invalid type."""


__all__ = [
    'Error',
    'BufferException',
    'BufferedFileError',
    'ConfigError',
    'AuthenticationError',
    'ExportError',
    'FetchError',
    'DestinationExistsError',
    'JobsCorruptedError',
    'IncompatibleSchemaVersion',
    'SyncConflict',
    'FileSyncConflict',
    'DocumentSyncConflict',
    'SchemaSyncConflict',
    'InvalidKeyError',
]
