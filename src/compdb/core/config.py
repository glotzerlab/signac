import logging
logger = logging.getLogger(__name__)

import os
import json as serializer

DEFAULT_FILENAME = 'compdb.rc'
CONFIG_FILENAMES = ['compdb.rc',]
HOME = os.path.expanduser('~')
CONFIG_PATH = [HOME]
CWD = os.getcwd()

ENVIRONMENT_VARIABLES = {
    'author_name' :              'COMPDB_AUTHOR_NAME',
    'author_email':              'COMPDB_AUTHOR_EMAIL',
    'project':                   'COMPDB_PROJECT',
    'project_dir' :              'COMPDB_PROJECT_DIR',
    'filestorage_dir':           'COMPDB_FILESTORAGE_DIR',
    'workspace_dir':             'COMPDB_WORKING_DIR',
    'database_host':             'COMPDB_DATABASE_HOST',
    'develop':                   'COMPDB_DEVELOP',
    'connect_timeout_ms':        'COMPDB_CONNECT_TIMEOUT',
    'compmatdb_host':            'COMPDB_COMPMATDB_HOST',
    'database_auth_mechanism':   'COMPDB_DATABASE_AUTH_MECHANISM',
    'compdb_version':            'COMPDB_VERSION',
}

REQUIRED_KEYS = [
    'author_name', 'author_email', 'project',
    'project_dir',  'filestorage_dir', 'workspace_dir',
    ]

DEFAULTS = {
    'database_host':            'localhost',
    'database_auth_mechanism':  'none',
    'database_meta':            'compdb',
    'database_compmatdb':       'compmatdb',
    'connect_timeout_ms':       5000,
    'noforking':                False,
}

from .dbclient_connector import SUPPORTED_AUTH_MECHANISMS, SSL_CERT_REQS
CHOICES = {
    'database_auth_mechanism': SUPPORTED_AUTH_MECHANISMS,
    'database_ssl_cert_reqs': SSL_CERT_REQS.keys(),
}

# File and dir names are interpreted relative to the working directory and stored as absolute path.
DIRS = ['workspace_dir', 'project_dir', 'filestorage_dir', 'global_fs_dir']
FILES = ['database_ssl_keyfile', 'database_ssl_certfile', 'database_ssl_ca_certs', 'database_ssl_cakeypemfile']

LEGAL_ARGS = REQUIRED_KEYS\
    + list(ENVIRONMENT_VARIABLES.keys())\
    + list(DEFAULTS.keys())\
    + list(CHOICES.keys())\
    + DIRS + FILES\
    + [
    'develop', 'compmatdb_host',
    'database_username', 'database_password',
    ]
LEGAL_ARGS = list(set(LEGAL_ARGS))

class IllegalKeyError(ValueError):
    pass

class IllegalArgumentError(ValueError):
    pass

def is_legal_key(key):
    return key in LEGAL_ARGS

def process_set(key, value):
    from os.path import abspath, expanduser
    if not is_legal_key(key):
        raise IllegalKeyError(key)
    if key in DIRS or key in FILES:
        return abspath(expanduser(value))
    if key in CHOICES:
        if not value in CHOICES[key]:
            raise IllegalArgumentError(key, value, CHOICES[key])
    if key.endswith('password'):
        import base64
        return base64.standard_b64encode(value.encode()).decode()
    if key.endswith('version'):
        if isinstance(value, tuple):
            return '.'.join((str(v) for v in value))
    return value

def process_get(key, value):
    if value is None:
        return None
    if key.endswith('password'):
        import base64
        return base64.standard_b64decode(value.encode()).decode()
    if key.endswith('version'):
        if isinstance(value, str):
            return tuple((int(v) for v in value.split('.')))
    return value

class Config(object):   

    def __init__(self, args = None):
        from copy import copy
        self._args = {}
        if args is not None:
            self.update(args)

    def __str__(self):
        return str(self._args)

    def read(self, filename = DEFAULT_FILENAME):
        is_root = False
        try:
            args = read_config_file(filename)
            logger.debug("Read: {}".format(args))
            is_root = 'project' in args
            self._args.update(args)
        except ValueError as error:
            msg = "Failed to read config file '{}'."
            raise RuntimeError(msg.format(filename))
        else:
            return is_root

    def _read_files(self):
        from os.path import dirname
        root_directory = None
        args_chain = []
        try:
            for fn in search_tree():
                args = read_config_file(fn)
                args_chain.append(args)
                is_root = 'project' in args
                if is_root:
                    root_directory = dirname(fn)
                    break
            for fn in search_standard_dirs():
                args = read_config_file(fn)
                args_chain.append(args)
        except Exception as error:
            msg = "Error while reading config file '{}': {}."
            logger.error(msg.format(fn, error))
        for args in reversed(args_chain):
            self._args.update(args)
        if root_directory is not None:
            logger.debug("Found root: {}".format(root_directory))
            self['project_dir'] = root_directory

    def update(self, args):
        for key, value in args.items():
            self[key] = value

    def load(self):
        logger.debug('Reading config...')
        self._read_files()
        self._args.update(read_environment())
        logger.debug('Verifying config...')
        self.verify()
        logger.debug('OK')

    def verify(self, strict = False):
        verify(self._args, strict = strict)

    def write(self, filename = DEFAULT_FILENAME, indent = 2, keys = None):
        import tempfile, shutil
        if keys is None:
            args = self._args
        else:
            args = {k: self._args[k] for k in keys if k in self._args}
        blob = serializer.dumps(args, indent=indent, sort_keys=True)
        with tempfile.NamedTemporaryFile() as file:
            file.write((blob + '\n').encode())
            file.flush()
            shutil.copy(file.name, filename)

    def _dump(self, indent = 2, keys = None):
        if keys is None:
            args = self._args
        else:
            args = {k: self._args[k] for k in keys if k in self._args}
        return serializer.dumps(args, indent = indent, sort_keys = True)

    def dump(self, indent = 2, keys = None):
        print(self._dump(indent, keys))

    def __str__(self):
        return self._dump(indent = 1)

    def __getitem__(self, key):
        try:
            v = self._args.get(key)
            return process_get(
                key, DEFAULTS[key] if v is None else v)
        except KeyError as error:
            msg = "Missing config key: '{key}'. "
            msg += "Try 'compdb config add {key} [your_value]"
            raise KeyError(msg.format(key = key)) from error

    def get(self, key, default = None):
        return process_get(
            key, self._args.get(key, DEFAULTS.get(key, default)))

    def __setitem__(self, key, value, force = False):
        if force:
            self._args[key] = value
        else:
            self._args[key] = process_set(key, value)

    def __contains__(self, key):
        return key in self._args

    def __delitem__(self, key):
        del self._args[key]

def read_config_file(filename):
    logger.debug("Reading config file '{}'.".format(filename))
    with open(filename) as file:
        return serializer.loads(file.read())

def _search_tree():
    from os import getcwd
    from os.path import realpath, join, isfile
    cwd = os.getcwd()
    while(True):
        for filename in CONFIG_FILENAMES:
            fn = realpath(join(cwd, filename))
            if isfile(fn):
                yield fn
                #return
        up = realpath(join(cwd, '..'))
        if up == cwd:
            msg = "Did not find project configuration file."
            logger.debug(msg)
            return
            #raise FileNotFoundError(msg)
        else:
            cwd = up

def search_tree():
    yield from _search_tree()
    #tree = list(_search_tree())
    #tree.reverse()
    #yield from tree

def search_standard_dirs():
    from os.path import realpath, join, isfile
    for path in CONFIG_PATH:
        for filename in CONFIG_FILENAMES:
            fn = realpath(join(path, filename))
            if isfile(fn):
                yield fn
                return

def read_environment():
    logger.debug("Reading environment variables.")
    import os
    args = dict()
    for key, var in ENVIRONMENT_VARIABLES.items():
        try:
            args[key] = os.environ[var]
            logger.debug("{}='{}'".format(key, args[key]))
        except KeyError:
            pass
    return args

def verify(args, strict = False):
    import os, warnings
    for key in args.keys():
        if not key in LEGAL_ARGS:
            msg = "Config key '{}' not recognized. Possible version conflict."
            logger.warning(msg.format(key))
            if strict:
                raise ValueError(msg.format(key))
            else:
                warnings.warn(msg.format(key), UserWarning)

    #for key in REQUIRED_KEYS:
    #    if not key in args.keys():
    #        msg = "Missing required config key: '{}'."
    #        logger.warning(msg.format(key))
    #        #raise KeyError(msg.format(key))

    # sanity check
    #assert set(args.keys()).issubset(set(LEGAL_ARGS))
    #assert set(REQUIRED_KEYS).issubset(set(args.keys()))

    dirs = [dir for dir in DIRS if dir in args]
    for dir_key in dirs:
        if not os.path.isabs(args[dir_key]):
            msg = "Directory specified for '{}': '{}' is not an absolute path."
            logger.warning(msg.format(dir_key, args[dir_key]))

def load_config():
    config = Config()
    config.load()
    return config
