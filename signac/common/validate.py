# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging

from .configobj.validate import Validator
from .configobj.validate import VdtValueError


logger = logging.getLogger(__name__)


def version(value, *args, **kwargs):
    try:
        if isinstance(value, str):
            return tuple((int(v) for v in value.split(',')))
        else:
            return tuple((int(v) for v in value))
    except Exception:
        raise VdtValueError(value)


def mongodb_uri(value, *args, **kwargs):
    if isinstance(value, list):
        value = ','.join(value)
    if not value.startswith('mongodb://'):
        value = 'mongodb://' + value
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


def password(value, *args, **kwargs):
    return value


def get_validator():
    return Validator({
        'version': version,
        'mongodb_uri': mongodb_uri,
        'password': password,
    })


cfg = """
workspace_dir = string(default='workspace')
project = string()
signac_version = version(default='0,1,0')

[General]
default_host = string()

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
