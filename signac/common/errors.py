# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from ..errors import FetchError
from ..errors import AuthenticationError, FileNotFoundError
from ..errors import ConfigError, ExportError
import warnings
with warnings.catch_warnings():
    warnings.simplefilter('always')
    warnings.warn("Module will be removed in version VERSION",
                  DeprecationWarning)
