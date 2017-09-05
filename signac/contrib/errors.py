# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from ..core.errors import Error


class DestinationExistsError(Error, RuntimeError):
    "The destination for a move or copy operation already exists."
    def __init__(self, destination):
        self.destination = destination
        "The destination object causing the error."


class JobsCorruptedError(Error, RuntimeError):
    "The state point manifest file of one or more jobs cannot be openend or is corrupted."
    def __init_(self, job_ids):
        self.job_ids = job_ids
        "The job id(s) of the corrupted job(s)."


class MergeConflict(Error, RuntimeError):
    "Raised when the merging of two jobs fails."
    def __init__(self, filename):
        self.filename = filename
        "The filename of the file that could not be merged."


class MergeSchemaConflict(Error, RuntimeError):
    "Raised when the schema of two projects to be merged differs."
    def __init__(self, schema_src, schema_dst):
        self.schema_src = schema_src
        self.schema_dst = schema_dst
