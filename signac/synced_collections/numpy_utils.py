# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define utilities for handling NumPy arrays."""

import warnings

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


class NumpyConversionWarning(UserWarning):
    """Warning raised when NumPy data is converted."""


NUMPY_CONVERSION_WARNING = (
    "Any numpy types provided will be transparently converted to the "
    "closest base Python equivalents."
)


def _convert_numpy(data):
    """Convert a numpy data types to the corresponding base data types.

    0d numpy arrays and numpy scalars are converted to their corresponding
    primitive types, while other numpy arrays are converted to lists. If data
    not a numpy data type, this function is a no-op.
    """
    if NUMPY:
        if isinstance(data, numpy.ndarray):
            # tolist will return a scalar for 0d arrays, so there's no need to
            # special-case that check. 1-element 1d arrays should remain
            # arrays, i.e. np.array([1])->[1], not 1.
            warnings.warn(NUMPY_CONVERSION_WARNING, NumpyConversionWarning)
            return data.tolist()
        elif isinstance(data, (numpy.number, numpy.bool_)):
            warnings.warn(NUMPY_CONVERSION_WARNING, NumpyConversionWarning)
            return data.item()
    return data


def _is_atleast_1d_numpy_array(data):
    """Check if an object is a nonscalar numpy array.

    The need to ignore 0d numpy arrays is in typical in the synced
    collections framework, since >0d arrays are mapped to (synced) lists
    while 0d arrays are mapped to scalars.

    Returns
    -------
    bool
        Whether or not the input is a numpy array with at least 1 dimension.
    """
    return NUMPY and isinstance(data, numpy.ndarray) and data.ndim > 0


def _is_numpy_scalar(data, allow_zero_d_array=False):
    """Check if an object is a numpy scalar.

    In certain cases a 0d numpy array is an acceptable surrogate for a scalar,
    in which case the optional parameter can be set to True.

    Parameters
    ----------
    allow_zero_d_array : bool
        If True, 0d numpy arrays will return True (Default value: False).

    Returns
    -------
    bool
        Whether or not the input is a numpy scalar type.
    """
    return NUMPY and (
        isinstance(data, (numpy.number, numpy.bool_))
        or (allow_zero_d_array and isinstance(data, numpy.ndarray) and data.ndim == 0)
    )
