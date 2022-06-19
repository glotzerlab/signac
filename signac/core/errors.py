# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Errors raised by signac.core classes."""


class Error(Exception):
    """Base class used for signac Errors."""

    pass


class H5StoreClosedError(Error, RuntimeError):
    """Raised when trying to access a closed HDF5 file."""


class H5StoreAlreadyOpenError(Error, OSError):
    """Indicates that the underlying HDF5 file is already open."""
