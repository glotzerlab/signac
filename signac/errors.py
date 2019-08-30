# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

from signac.db import __version__
from deprecation import deprecated

from .core.errors import Error

from .core.jsondict import BufferException
from .core.jsondict import BufferedFileError

from .common.errors import ConfigError
from .common.errors import AuthenticationError
from .common.errors import ExportError
from .common.errors import FileNotFoundError
from .common.errors import FetchError

from .contrib.errors import DestinationExistsError
from .contrib.errors import JobsCorruptedError


@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The database package is deprecated.")
class SyncConflict(Error, RuntimeError):
    "Raised when a synchronization operation fails."
    pass


@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The database package is deprecated.")
class FileSyncConflict(SyncConflict):
    "Raised when a synchronization operation fails due to a file conflict."
    def __init__(self, filename):
        self.filename = filename
        "The filename of the file that caused the conflict."

    def __str__(self):
        return "The file with filename '{}' caused a conflict.".format(self.filename)


@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The database package is deprecated.")
class DocumentSyncConflict(SyncConflict):
    "Raised when a synchronization operation fails due to a document conflict."
    def __init__(self, keys):
        self.keys = keys
        "The keys that caused the conflict."

    def __str__(self):
        return "The following keys caused a conflict: {}".format(', '.join(self.keys))


@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The database package is deprecated.")
class SchemaSyncConflict(SyncConflict):
    "Raised when a synchronization operation fails due to schema differences."
    def __init__(self, schema_src, schema_dst):
        self.schema_src = schema_src
        self.schema_dst = schema_dst

    def __str__(self):
        return "The synchronization failed, because of a schema conflict."


@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The database package is deprecated.")
class InvalidKeyError(ValueError):
    """Raised when a user uses a non-conforming key."""


@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The database package is deprecated.")
class KeyTypeError(TypeError):
    """Raised when a user uses a key of invalid type."""


__all__ = [
    'Error',
    'BufferException',
    'BufferedFileError',
    'ConfigError',
    'AuthenticationError',
    'ExportError',
    'FileNotFoundError',
    'FetchError',
    'DestinationExistsError',
    'JobsCorruptedError',
    'SyncConflict',
    'FileSyncConflict',
    'DocumentSyncConflict',
    'SchemaSyncConflict',
    'InvalidKeyError',
]
