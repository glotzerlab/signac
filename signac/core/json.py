# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging
from json import load, loads, JSONEncoder
from json.decoder import JSONDecodeError

logger = logging.getLogger(__name__)

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
                return o.item()
            elif isinstance(o, numpy.ndarray):
                return o.tolist()
        try:
            return o._as_dict()
        except AttributeError:
            # Call the super method, which raises a TypeError if it cannot
            # encode the object.
            return super(CustomJSONEncoder, self).default(o)


def dumps(o, sort_keys=False, indent=None):
    return CustomJSONEncoder(sort_keys=sort_keys, indent=indent).encode(o)


__all__ = ['loads', 'load', 'dumps', 'JSONDecodeError']
