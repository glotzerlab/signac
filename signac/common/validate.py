# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Validate config schema."""

import logging

from .configobj.validate import Validator

logger = logging.getLogger(__name__)


def get_validator():  # noqa: D103
    return Validator()


cfg = """
workspace_dir = string(default='workspace')
schema_version = string(default='1')
"""
