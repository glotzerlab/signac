# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Errors raised by signac.contrib classes."""

from ..core.errors import Error


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
    destination :
        The destination object causing the error.

    """

    def __init__(self, destination):
        self.destination = destination


class JobsCorruptedError(Error, RuntimeError):
    """The state point manifest file of one or more jobs cannot be opened or is corrupted.

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
