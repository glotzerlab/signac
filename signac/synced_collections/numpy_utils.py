# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define utilities for handling NumPy arrays."""

# TODO: Add warning on conversion.
try:
    import numpy

    def _convert_numpy(data):
        """Convert a numpy data types to the corresponding base data types.

        0d numpy arrays and numpy scalars are converted to their corresponding
        primitive types, while other numpy arrays are converted to lists. If data
        not a numpy data type, this function is a no-op.
        """
        if isinstance(data, numpy.ndarray):
            # tolist will return a scalar for 0d arrays, so there's no need to
            # special-case that check. 1-element 1d arrays should remain
            # arrays, i.e. np.array([1])->[1], not 1.
            return data.tolist()
        elif isinstance(data, (numpy.number, numpy.bool_)):
            return data.item()
        return data

    def _is_nonscalar_numpy_array(data):
        """Check if an object is a nonscalar numpy array.

        0d numpy arrays are not considered numpy arrays by this function. This
        behavior is in line with typical requirements in the synced collections
        framework, since >0d arrays are mapped to (synced) lists while 0d
        arrays are mapped to scalars.

        Returns
        -------
        bool
            Whether or not the input is a numpy array.
        """
        return isinstance(data, numpy.ndarray) and data.shape != ()

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
        return isinstance(data, (numpy.number, numpy.bool_)) or (
            allow_zero_d_array and isinstance(data, numpy.ndarray) and data.shape != ()
        )


except ImportError:

    def _convert_numpy(data):
        """Trivial implementation if numpy is not present."""
        return data

    def _is_nonscalar_numpy_array(data):
        """Trivial implementation if numpy is not present."""
        return False

    def _is_numpy_scalar(data, allow_zero_d_array=False):
        """Trivial implementation if numpy is not present."""
        return False
