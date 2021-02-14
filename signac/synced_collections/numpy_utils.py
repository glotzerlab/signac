# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define utilities for handling NumPy arrays.

Various parts of the synced collections framework require conversion of NumPy
data types. Broadly speaking, there are two main reasons for this:
    1. :class:`numpy.ndarray` objects must be converted into :class:`SyncedList`
       objects. This requirement is true for all synced collections irrespective
       of backend because all collections nested within a :class:`SyncedCollection`
       must also be instances of :class:`SyncedCollection` to ensure that synchronization
       always occurs on modification.
    2. NumPy scalar types must be converted to raw Python types. This requirement
       may not be true for all backends, and should be handled at that level.

This module provides facilities for both.
"""

# TODO: Switch evaluation orders in all methods to early-exit when _not_ a
# numpy type (the more common case).
# TODO: Rewrite conditionals in most efficient manner.
# TODO: Add warning on conversion.

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


def _convert_numpy_scalar(data):
    """Convert a numpy scalar to a raw scalar.

    If data already a raw scalar, this function is a no-op.
    """
    if NUMPY:
        if isinstance(data, (numpy.number, numpy.bool_)) or (
            isinstance(data, numpy.ndarray) and data.shape == ()
        ):
            return data.item()
    return data


def _convert_numpy(data):
    """Convert a numpy data types to the corresponding base data types.

    0-d numpy arrays and numpy scalars are converted to their corresponding
    primitive types, while other numpy arrays are converted to lists. If data
    not a numpy data type, this function is a no-op.
    """
    if NUMPY:
        if isinstance(data, numpy.ndarray):
            # tolist will return a scalar for 0d arrays, so there's no need to
            # special-case that check.
            return data.tolist()
        elif isinstance(data, (numpy.number, numpy.bool_)):
            return data.item()
    return data


def _is_numpy_type(data, allow_zero_d=False, allow_scalar=False):
    """Check if an object is a numpy type.

    Parameters
    ----------
    allow_zero_d : bool
        If True, 0d numpy arrays will return True (Default value: False).
    allow_scalar : bool
        If True, numpy scalars will return True (Default value: False).
    """
    return (
        NUMPY
        and (isinstance(data, numpy.ndarray) and (data.shape != () or allow_zero_d))
        or (allow_scalar and isinstance(data, (numpy.number, numpy.bool_)))
    )
