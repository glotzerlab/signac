# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Utility functions."""

import os.path


def _safe_relpath(path):
    """Attempt to make a relative path, or return the original path.

    This is useful for logging and representing objects, where an absolute path
    may be very long.
    """
    try:
        return os.path.relpath(path)
    except ValueError:
        # Windows cannot find relative paths across drives, so show the
        # original path instead.
        return path
