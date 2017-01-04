# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from ..core.errors import Error


class DestinationExistsError(Error, RuntimeError):
    "The destination for a move or copy operation already exists."
    def __init__(self, destination):
        self.destination = destination
        "The destination object causing the error."
