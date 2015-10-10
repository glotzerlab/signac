import logging
import warnings
import os

from . import SSL_SUPPORT

logger = logging.getLogger(__name__)

from ..common.connection import SUPPORTED_AUTH_MECHANISMS, SSL_CERT_REQS


def load_config(* args, **kwargs):
    from ..common.config import load_config
    warnings.warn(DeprecationWarning, "Use common library.")
    return load_config(* args, **kwargs)


def read_config_file(* args, **kwargs):
    from ..common.config import read_config_file
    warnings.warn(DeprecationWarning, "Use common library.")
    return read_config_file(* args, **kwargs)

ENVIRONMENT_VARIABLES = {
    'author_name':              'SIGNAC_AUTHOR_NAME',
    'author_email':              'SIGNAC_AUTHOR_EMAIL',
    'project':                   'SIGNAC_PROJECT',
    'project_dir':              'SIGNAC_PROJECT_DIR',
    'filestorage_dir':           'SIGNAC_FILESTORAGE_DIR',
    'workspace_dir':             'SIGNAC_WORKING_DIR',
    'database_host':             'SIGNAC_DATABASE_HOST',
    'develop':                   'SIGNAC_DEVELOP',
    'connect_timeout_ms':        'SIGNAC_CONNECT_TIMEOUT',
    'signacdb_host':             'SIGNAC_DB_HOST',
    'database_auth_mechanism':   'SIGNAC_DATABASE_AUTH_MECHANISM',
    'signac_version':            'SIGNAC_VERSION',
}

REQUIRED_KEYS = [
    'author_name', 'author_email', 'project',
    'project_dir',  'filestorage_dir', 'workspace_dir',
]

DEFAULTS = {
    'database_host':            'localhost',
    'database_auth_mechanism':  'none',
    'database_meta':            'signac',
    'database_signacdb':       'signacdb',
    'connect_timeout_ms':       5000,
    'noforking':                False,
}

CHOICES = dict()

if SSL_SUPPORT:
    CHOICES.update({
        'database_auth_mechanism': SUPPORTED_AUTH_MECHANISMS,
        'database_ssl_cert_reqs': SSL_CERT_REQS.keys(),
    })

# File and dir names are interpreted relative to the working directory and
# stored as absolute path.
DIRS = ['workspace_dir', 'project_dir', 'filestorage_dir', 'global_fs_dir']
FILES = ['database_ssl_keyfile', 'database_ssl_certfile',
         'database_ssl_ca_certs', 'database_ssl_cakeypemfile']

LEGAL_ARGS = REQUIRED_KEYS\
    + list(ENVIRONMENT_VARIABLES.keys())\
    + list(DEFAULTS.keys())\
    + list(CHOICES.keys())\
    + DIRS + FILES\
    + [
        'develop', 'signacdb_host', 'compmatdb_host',
        'database_username', 'database_password',
        'signacdb_admin',
    ]
LEGAL_ARGS = list(set(LEGAL_ARGS))


class IllegalKeyError(ValueError):
    pass


class IllegalArgumentError(ValueError):
    pass


class PermissionsError(RuntimeError):
    pass


def read_environment():
    logger.debug("Reading environment variables.")
    args = dict()
    for key, var in ENVIRONMENT_VARIABLES.items():
        try:
            args[key] = os.environ[var]
            logger.debug("{}='{}'".format(key, args[key]))
        except KeyError:
            pass
    return args


def verify(args, strict=False):
    warnings.warn("No verification.")
    return
    for key in args.keys():
        if not key in LEGAL_ARGS:
            msg = "Config key '{}' not recognized. Possible version conflict."
            logger.warning(msg.format(key))
            if strict:
                raise IllegalKeyError(msg.format(key))
            else:
                warnings.warn(msg.format(key), UserWarning)

    dirs = [dir for dir in DIRS if dir in args]
    for dir_key in dirs:
        if not os.path.isabs(args[dir_key]):
            msg = "Directory specified for '{}': '{}' is not an absolute path."
            logger.warning(msg.format(dir_key, args[dir_key]))
