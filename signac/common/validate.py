# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Validate config schema."""

import logging

from .configobj.validate import Validator

logger = logging.getLogger(__name__)


def get_validator():  # noqa: D103
    return Validator()


# TODO: Rename to something internal and uppercase e.g. _CFG.
cfg = """
schema_version = string(default='1')
"""
