# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Hashing functions."""

import hashlib
import json

# We must use the standard library json for exact consistency in formatting


def calc_id(spec):
    """Calculate and return a hash value for the given spec.

    The hash is computed as an MD5 checksum of the input data. The input data
    is first encoded as JSON, with dictionary keys sorted to ensure the hash
    is reproducible.

    Parameters
    ----------
    spec : dict
        A JSON-encodable mapping.

    Returns
    -------
    str
        Encoded hash in hexadecimal format.

    """
    blob = json.dumps(spec, sort_keys=True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()
