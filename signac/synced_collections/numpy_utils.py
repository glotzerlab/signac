# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define utilities for handling NumPy arrays."""

import warnings

try:
    import numpy

    NUMPY = True
    _numpy_cache_blocklist = (numpy.ndarray,)
except ImportError:
    NUMPY = False
    _numpy_cache_blocklist = None  # type: ignore


class NumpyConversionWarning(UserWarning):
    """Warning raised when NumPy data is converted."""


NUMPY_CONVERSION_WARNING = (
    "Any numpy types provided will be transparently converted to the "
    "closest base Python equivalents."
)


def _convert_numpy(data):
    """Convert numpy data types to the corresponding base data types.

    0d numpy arrays and numpy scalars are converted to their corresponding
    primitive types, while other numpy arrays are converted to lists. If ``data``
    is not a numpy data type, this function is a no-op.
    """
    # Initially performing one isinstance check is faster since most of the
    # time the inputs are not numpy arrays, preventing additional function
    # calls inside the if statement.
    if NUMPY and isinstance(data, (numpy.ndarray, numpy.number, numpy.bool_)):
        if isinstance(data, numpy.ndarray):
            # tolist will return a scalar for 0d arrays, so there's no need to
            # special-case that check. 1-element 1d arrays should remain
            # arrays, i.e. np.array([1]) should become [1], not 1.
            warnings.warn(NUMPY_CONVERSION_WARNING, NumpyConversionWarning)
            return data.tolist()
        else:
            warnings.warn(NUMPY_CONVERSION_WARNING, NumpyConversionWarning)
            return data.item()
    return data


def _is_atleast_1d_numpy_array(data):
    """Check if an object is a nonscalar numpy array.

    The synced collections framework must differentiate 0d numpy arrays from
    other arrays because they are mapped to scalars while >0d arrays are mapped
    to (synced) lists.

    Returns
    -------
    bool
        Whether or not the input is a numpy array with at least 1 dimension.
    """
    return NUMPY and isinstance(data, numpy.ndarray) and data.ndim > 0


def _is_numpy_scalar(data):
    """Check if an object is a numpy scalar.

    This function is designed for use in situations where _convert_numpy has
    already been applied (if necessary), so 0d arrays are not considered
    scalars here.

    Returns
    -------
    bool
        Whether or not the input is a numpy scalar type.
    """
    return NUMPY and (
        (isinstance(data, (numpy.number, numpy.bool_)))
        or (isinstance(data, numpy.ndarray) and data.ndim == 0)
    )


def _is_complex(data):
    """Check if an object is complex.

    This function works for both numpy raw Python data types.

    Returns
    -------
    bool
        Whether or not the input is a complex number.
    """
    return (NUMPY and numpy.iscomplex(data).any()) or (isinstance(data, complex))
