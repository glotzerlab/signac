from .configobj.validate import Validator
from .configobj.validate import VdtTypeError


def version(value, *args, **kwargs):
    try:
        if isinstance(value, str):
            return tuple((int(v) for v in value.split(',')))
        else:
            return tuple((int(v) for v in value))
    except Exception as error:
        print(error)
        raise VdtTypeError(value)

fdict = {
    'version': version
    }

def validator():
    return Validator(fdict)


cfg="""
author_name = string
author_email = string
filestorage_dir = string
workspace_dir = string
noforking = bool(default=False)
project = string
signac_version = version(default='0,1,0')

database_host = string()
database_auth_mechanism = option('none', 'SCRAM-SHA-1', 'SSL-x509', 'SSL', default='none')
database_ssl_ca_certs = string
database_ssl_certfile = string
database_ssl_keyfile = string
database_username = string
database_password = string
database_connect_timeout_ms = integer(default=5000)

[General]
default_host = string()
[Author]
name = string
email = string

[signacdb]
database = string(default='signacdb')

[hosts]
[[__many__]]
url = string(default='localhost')
auth_mechanism = option('none', 'SCRAM-SHA-1', 'SSL-x509', 'SSL', default='none')
ssl_ca_certs = string
ssl_certfile = string
ssl_keyfile = string
username = string
password = string
connect_timeout_ms = integer(default=5000)
"""
