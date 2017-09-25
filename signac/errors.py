# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

from .core.errors import Error

from .common.errors import ConfigError
from .common.errors import AuthenticationError
from .common.errors import ExportError
from .common.errors import FileNotFoundError
from .common.errors import FetchError

from .contrib.errors import DestinationExistsError


class MergeConflict(Error, RuntimeError):
    "Raised when the merge of two jobs fails."
    pass


class FileMergeConflict(MergeConflict):
    "Raised when a merge fails due to a file conflict."
    def __init__(self, filename):
        self.filename = filename
        "The filename of the file that caused the conflict."


class DocumentMergeConflict(MergeConflict):
    "Raised when a merge fails due to a document conflict."
    def __init__(self, keys):
        self.keys = keys
        "The keys that caused the conflict."


class SchemaMergeConflict(MergeConflict):
    "Raised when the schema of two projects to be merged differs."
    def __init__(self, schema_src, schema_dst):
        self.schema_src = schema_src
        self.schema_dst = schema_dst


__all__ = [
    'Error',
    'ConfigError',
    'AuthenticationError',
    'ExportError',
    'FileNotFoundError',
    'FetchError',
    'DestinationExistsError',
    'MergeConflict',
    'FileMergeConflict',
    'DocumentMergeConflict',
    'SchemaMergeConflict',
]
