# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Errors raised by signac.common classes."""
from ..core.errors import Error


class ConfigError(Error, RuntimeError):
    """Error with parsing or reading a configuration file."""

    pass
