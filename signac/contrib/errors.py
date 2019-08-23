# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from ..errors import JobsCorruptedError, StatepointParsingError
from ..errors import WorkspaceError, DestinationExistsError
import warnings
with warnings.catch_warnings():
    warnings.simplefilter('always')
    warnings.warn("Module will be removed in version VERSION",
                  DeprecationWarning)
