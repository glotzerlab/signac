import os

import logging
logger = logging.getLogger('config')

CONFIG_FILENAMES = ['compdb.rc',]
HOME = os.path.expanduser('~')
CWD = os.getcwd()
CONFIG_PATH = [HOME, CWD]

ENVIRONMENT_VARIABLES = {
    'author_name' :     'COMPDB_AUTHOR_NAME',
    'author_email':     'COMPDB_AUTHOR_EMAIL',
    'project':          'COMPDB_PROJECT',
    'project_dir' :     'COMPDB_PROJECT_DIR',
    'filestorage_dir':  'COMPDB_FILESTORAGE_DIR',
    'working_dir':      'COMPDB_WORKING_DIR',
    'database_host':    'COMPDB_DATABASE_HOST',
}

REQUIRED_KEYS = [
    'author_name', 'author_email', 'project',
    'project_dir',  'filestorage_dir',
    ]

class Config(object):   

    def __init__(self):
        self._args = dict()
        pass

    def read(self):
        logger.debug('Reading config...')
        args = DEFAULTS
        args.update(read_config_files())
        args.update(read_environment())
        logger.debug('Verifying config...')
        verify(args)
        logger.debug('OK')
        self._args = args

    def __getitem__(self, key):
        try:
            return self._args[key]
        except KeyError:
            msg = "Missing config key: '{}'."
            raise KeyError(msg.format(key))

    def get(self, key, default = None):
        return self._args.get(key, default)

    def __setitem__(self, key, value):
        self._args[key] = value

DEFAULTS = {
    'database_host': 'localhost',
    'database_meta': '_compdb',
    'database_global_fs': '_compdb_fs',
    'working_dir': CWD,
}

LEGAL_ARGS = REQUIRED_KEYS + list(DEFAULTS.keys()) + [
    'global_fs_dir',
    ]

def read_config_files():
    args = dict()
    import json
    for filename in CONFIG_FILENAMES:
        for path in CONFIG_PATH:
            try:
                fn = os.path.join(path, filename)
                with open(fn) as file:
                    args.update(json.loads(file.read()))
            except (IOError, ) as error:
                continue
            except Exception as error:
                msg = "Error while reading config file '{}'."
                logger.error(fn)
                raise
    return args

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

def verify(args):
    import os
    for key in args.keys():
        if not key in LEGAL_ARGS:
            msg = "Illegal config key: '{}'."
            #logger.warning(msg.format(key))
            raise KeyError(msg.format(key))

    logger.debug(args)
    for key in REQUIRED_KEYS:
        if not key in args.keys():
            msg = "Missing required config key: '{}'."
            logger.warning(msg.format(key))
            #raise KeyError(msg.format(key))

    # sanity check
    #assert set(args.keys()).issubset(set(LEGAL_ARGS))
    #assert set(REQUIRED_KEYS).issubset(set(args.keys()))

    DIRS = ['working_dir', 'project_dir', 'filestorage_dir', 'global_fs_dir']
    dirs = (dir for dir in DIRS if dir in args)

    for dir_key in dirs:
        if dir_key in args:
            args[dir_key] = os.path.expanduser(args[dir_key])
    for dir_key in dirs:
        if dir_key in args:
            if os.path.isdir(os.path.abspath(args[dir_key])):
                args[dir_key] = os.path.abspath(args[dir_key])
            elif os.path.isdir(os.path.realpath(args[dir_key])):
                args[dir_key] = os.path.realpath(args[dir_key])
            else:
                msg = "Directory specified for '{}': '{}', does not exist."
                raise IOError(msg.format(dir_key, args[dir_key]))

def read_config():
    config = Config()
    config.read()
    return config
