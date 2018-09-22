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


class CustomJSONEncoder(JSONEncoder):
    """Attempt to JSON-encode objects beyond the default supported types.

    This encoder will attempt to obtain a JSON-serializable representation of
    a otherwise not serializable object, by calling a potentially implemented
    `_as_dict()` method.
    """
    def default(self, o):
        try:
            return o._to_url()
        except AttributeError:
            try:
                return o._to_json()
            except AttributeError:
                # Call the super method, which probably raise a TypeError.
                return super(CustomJSONEncoder, self).default(o)


__all__ = ['json', 'CustomJSONEncoder']
