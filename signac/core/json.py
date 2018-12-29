# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import absolute_import
import logging

logger = logging.getLogger(__name__)
msg = "Using '{}' package for JSON encoding/decoding."

try:
    import rapidjson as json
    logger.debug(msg.format('rapidjson'))
except ImportError:
    import json
    from json import JSONEncoder

    logger.debug(msg.format('json'))

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False


class CustomJSONEncoder(JSONEncoder):
    """Attempt to JSON-encode objects beyond the default supported types.

    This encoder will attempt to obtain a JSON-serializable representation of
    an object that is otherwise not serializable, by calling the object's
    `_as_dict()` method.
    """
    def default(self, o):
        if NUMPY:
            if isinstance(o, numpy.number):
                return numpy.asscalar(o)
            elif isinstance(o, numpy.ndarray):
                return o.tolist()
        try:
            return o._as_dict()
        except AttributeError:
            # Call the super method, which probably raise a TypeError.
            return super(CustomJSONEncoder, self).default(o)


__all__ = ['json', 'CustomJSONEncoder']
