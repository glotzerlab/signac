# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import absolute_import
import logging

logger = logging.getLogger(__name__)
msg = "Using '{}' package for JSON encoding/decoding."

try:
    import numpy
    NUMPY = True
except ImportError:
    NUMPY = False

try:
    import rapidjson as json
    from rapidjson import Encoder

    class JSONEncoder(Encoder):
        encode = Encoder.__call__

    logger.debug(msg.format('rapidjson'))
except ImportError:
    import json
    from json import JSONEncoder

    logger.debug(msg.format('json'))


_has_default = hasattr(JSONEncoder, 'default')


class CustomJSONEncoder(JSONEncoder):
    """Attempt to JSON-encode objects beyond the default supported types.

    This encoder will attempt to obtain a JSON-serializable representation of
    an object that is otherwise not serializable, by calling the object's
    `_as_dict()` method.
    """
    def default(self, o):
        if NUMPY:
            if isinstance(o, numpy.number):
                return o.item()
            elif isinstance(o, numpy.ndarray):
                return o.tolist()
        try:
            return o._as_dict()
        except AttributeError:
            if _has_default:
                # Call the super method, which probably raise a TypeError.
                return super(CustomJSONEncoder, self).default(o)
            else:
                raise TypeError(
                    "Unable to serialize object '{}'.".format(o))


def loads(s):
    return json.loads(s)


def dumps(o, sort_keys=False, indent=None):
    return CustomJSONEncoder(sort_keys=sort_keys, indent=indent).encode(o)


__all__ = ['loads', 'dumps']
