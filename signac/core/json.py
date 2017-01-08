# Copyright (c) 2017 The Regents of the University of Michigan
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
    logger.debug(msg.format('json'))


__all__ = ['json']
