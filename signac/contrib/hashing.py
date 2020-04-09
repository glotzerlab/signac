# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Hashing functions."""

import hashlib
# we need to use the standard module here to ensure
# exact consistent formatting
import json


def calc_id(spec):
    """Calculate and return a hash value for the given spec.

    Parameters
    ----------
    spec :
        Data to hash.

    Returns
    -------
    str
        Encoded hash in hexadecimal format.

    """
    blob = json.dumps(spec, sort_keys=True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()
