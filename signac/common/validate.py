# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the MIT License.
import warnings

from .configobj.validate import Validator
from .configobj.validate import VdtValueError


def version(value, *args, **kwargs):
    try:
        if isinstance(value, str):
            return tuple((int(v) for v in value.split(',')))
        else:
            return tuple((int(v) for v in value))
    except Exception:
        raise VdtValueError(value)


def mongodb_uri(value, *args, **kwargs):
    try:
        import pymongo
    except ImportError:
        warnings.warn("Install pymongo to validate database configurations.")
    else:
        uris = value if isinstance(value, list) else value.split(',')
        for uri in uris:
            try:
                if not uri.startswith('mongodb://'):
                    uri = 'mongodb://' + uri
                pymongo.uri_parser.parse_uri(uri)
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
author_name = string(default=None)
author_email = string(default=None)
workspace_dir = string(default='workspace')
project = string(default=None)
signac_version = version(default='0,1,0')

database_host = string(default=None)
database_auth_mechanism = option('none', 'SCRAM-SHA-1', 'SSL-x509', 'SSL', default='none')
database_ssl_ca_certs = string(default=None)
database_ssl_certfile = string(default=None)
database_ssl_keyfile = string(default=None)
database_username = string(default=None)
database_password = string(default=None)
database_connect_timeout_ms = integer(default=5000)

[General]
default_host = string()
[Author]
name = string(default=None)
email = string(default=None)

[signacdb]
database = string(default='signacdb')

[hosts]
[[__many__]]
url = mongodb_uri(default='localhost')
auth_mechanism = option('none', 'SCRAM-SHA-1', 'SSL-x509', 'SSL', default='none')
ssl_ca_certs = string(default=None)
ssl_certfile = string(default=None)
ssl_keyfile = string(default=None)
username = string
password = password()
connect_timeout_ms = integer(default=5000)
db_auth = string(default='admin')
[[[password_config]]]
salt = string
rounds = integer
"""
