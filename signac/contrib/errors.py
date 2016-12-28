# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from ..core.errors import Error


class DestinationExistsError(Error, RuntimeError):
    pass


class MergeConflict(Error, RuntimeError):
    "Signals a merge conflict when trying to merge a job."
    def __init__(self, keys, filenames):
        self.keys = keys
        "All keys within the job document causing a conflict."
        self.filnames = filenames
        "The filenames of all files causing a conflict."
