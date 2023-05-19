# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Errors raised by signac."""

from synced_collections.errors import InvalidKeyError, KeyTypeError


class Error(Exception):
    """Base class used for signac Errors."""

    pass


class ConfigError(Error, RuntimeError):
    """Error with parsing or reading a configuration file."""

    pass


class H5StoreClosedError(Error, RuntimeError):
    """Raised when trying to access a closed HDF5 file."""


class H5StoreAlreadyOpenError(Error, OSError):
    """Indicates that the underlying HDF5 file is already open."""


class SyncConflict(Error, RuntimeError):
    """Raised when a synchronization operation fails."""

    pass


class FileSyncConflict(SyncConflict):
    """Raised when a synchronization operation fails due to a file conflict."""

    def __init__(self, filename):
        self.filename = filename
        "The filename of the file that caused the conflict."

    def __str__(self):
        return f"The file with filename '{self.filename}' caused a conflict."


class DocumentSyncConflict(SyncConflict):
    """Raised when a synchronization operation fails due to a document conflict."""

    def __init__(self, keys):
        self.keys = keys
        "The keys that caused the conflict."

    def __str__(self):
        return "The following keys caused a conflict: {}".format(", ".join(self.keys))


class SchemaSyncConflict(SyncConflict):
    """Raised when a synchronization operation fails due to schema differences."""

    def __init__(self, schema_src, schema_dst):
        self.schema_src = schema_src
        self.schema_dst = schema_dst

    def __str__(self):
        return "The synchronization failed, because of a schema conflict."


class WorkspaceError(Error, OSError):
    """Raised when there is an issue creating or accessing the workspace.

    Parameters
    ----------
    error :
        The underlying error causing this issue.

    """

    def __init__(self, error):
        self.error = error

    def __str__(self):
        return self.error


class DestinationExistsError(Error, RuntimeError):
    """The destination for a move or copy operation already exists.

    Parameters
    ----------
    destination : str
        The destination causing the error.

    """

    def __init__(self, destination):
        self.destination = destination


class JobsCorruptedError(Error, RuntimeError):
    """The state point file of one or more jobs cannot be opened or is corrupted.

    Parameters
    ----------
    job_ids :
        The job id(s) of the corrupted job(s).

    """

    def __init__(self, job_ids):
        self.job_ids = job_ids


class StatepointParsingError(Error, RuntimeError):
    """Indicates an error that occurred while trying to identify a state point."""

    pass


class IncompatibleSchemaVersion(Error):
    """The project's schema version is incompatible with this version of signac."""

    pass


__all__ = [
    "ConfigError",
    "DestinationExistsError",
    "DocumentSyncConflict",
    "Error",
    "FileSyncConflict",
    "H5StoreAlreadyOpenError",
    "H5StoreClosedError",
    "IncompatibleSchemaVersion",
    "InvalidKeyError",
    "JobsCorruptedError",
    "KeyTypeError",
    "SchemaSyncConflict",
    "StatepointParsingError",
    "SyncConflict",
    "WorkspaceError",
]
