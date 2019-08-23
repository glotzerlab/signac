# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from .common import six


class Error(Exception):
    pass


class ConfigError(Error, RuntimeError):
    pass


class PermissionsError(ConfigError):
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


class BufferException(Error):
    "An exception occured in buffered mode."
    pass


class BufferedFileError(BufferException):
    """Raised when an error occured while flushing one or more buffered files.

    .. attribute:: files

        A dictionary of files that caused issues during the flush operation,
        mapped to a possible reason for the issue or None in case that it
        cannot be determined.
    """

    def __init__(self, files):
        self.files = files

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.files)


class WorkspaceError(Error, OSError):
    "Raised when there is an issue to create or access the workspace."

    def __init__(self, error):
        self.error = error
        "The underlying error causing this issue."

    def __str__(self):
        return self.error


class DestinationExistsError(Error, RuntimeError):
    "The destination for a move or copy operation already exists."

    def __init__(self, destination):
        self.destination = destination
        "The destination object causing the error."


class JobsCorruptedError(Error, RuntimeError):
    "The state point manifest file of one or more jobs cannot be openend or is corrupted."

    def __init__(self, job_ids):
        self.job_ids = job_ids
        "The job id(s) of the corrupted job(s)."


class StatepointParsingError(Error, RuntimeError):
    "Indicates an error that occurred while tyring to identify a state point."
    pass


class SyncConflict(Error, RuntimeError):
    "Raised when a synchronization operation fails."
    pass


class FileSyncConflict(SyncConflict):
    "Raised when a synchronization operation fails due to a file conflict."

    def __init__(self, filename):
        self.filename = filename
        "The filename of the file that caused the conflict."

    def __str__(self):
        return "The file with filename '{}' caused a conflict.".format(self.filename)


class DocumentSyncConflict(SyncConflict):
    "Raised when a synchronization operation fails due to a document conflict."

    def __init__(self, keys):
        self.keys = keys
        "The keys that caused the conflict."

    def __str__(self):
        return "The following keys caused a conflict: {}".format(', '.join(self.keys))


class SchemaSyncConflict(SyncConflict):
    "Raised when a synchronization operation fails due to schema differences."

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
