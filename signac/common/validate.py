# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Validate config schema."""

import logging

from .configobj.validate import Validator, VdtValueError

logger = logging.getLogger(__name__)


def mongodb_uri(value, *args, **kwargs):
    """Return a MongoDB URI."""
    if isinstance(value, list):
        value = ",".join(value)
    if not value.startswith("mongodb://"):
        value = "mongodb://" + value
    try:
        import pymongo
    except ImportError:
        logger.debug("Install pymongo to validate database configurations!")
    else:
        try:
            pymongo.uri_parser.parse_uri(value)
        except pymongo.errors.InvalidURI:
            raise VdtValueError(value)
    return value


def password(value, *args, **kwargs):  # noqa: D103
    return value


def get_validator():  # noqa: D103
    return Validator({"mongodb_uri": mongodb_uri, "password": password})


cfg = """
project = string()
workspace_dir = string(default='workspace')
schema_version = string(default='1')

[General]
default_host = string(default=None)

[hosts]
[[__many__]]
url = mongodb_uri(default='localhost')
auth_mechanism = option('none', 'SCRAM-SHA-1', default='none')
username = string()
password = password()
db_auth = string(default='admin')
[[[password_config]]]
salt = string()
rounds = integer()
"""
