import os

import logging
logger = logging.getLogger('config')

CONFIG_FILENAMES = ['compdb.rc',]
HOME = os.path.expanduser('~')
CWD = os.getcwd()
CONFIG_PATH = [HOME, CWD]

ENVIRONMENT_VARIABLES = {
    'author_name' : 'COMPDB_AUTHOR_NAME',
    'author_email': 'COMPDB_AUTHOR_EMAIL',
    'project_dir' : 'COMPDB_PROJECT_DIR',
}

REQUIRED_KEYS = [
    'author_name', 'author_email', 'project',
    'project_dir', 'database', 'filestorage_dir',
    ]

DEFAULTS = {
    'database_host': 'localhost',
    'database_meta': '_compdb',
    'working_dir': CWD,
}

LEGAL_ARGS = REQUIRED_KEYS + list(DEFAULTS.keys()) + []


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
    import os
    args = dict()
    for key, var in ENVIRONMENT_VARIABLES.items():
        try:
            args[key] = os.environ[var]
        except KeyError:
            pass
    return args

def verify(args):
    import os
    for key in args.keys():
        if not key in LEGAL_ARGS:
            msg = "Illegal config key: '{}'."
            raise KeyError(msg.format(key))

    for key in REQUIRED_KEYS:
        if not key in args.keys():
            msg = "Missing required config key: '{}'."
            raise KeyError(msg.format(key))

    # sanity check
    assert set(args.keys()).issubset(set(LEGAL_ARGS))
    assert set(REQUIRED_KEYS).issubset(set(args.keys()))

    dirs = ['working_dir', 'project_dir', 'filestorage_dir']
    for dir_key in dirs:
        args[dir_key] = os.path.realpath(args[dir_key])
        if not os.path.isdir(args[dir_key]):
            msg = "Directory specified for '{}': '{}', does not exist."
            raise IOError(msg.format(dir_key, args[dir_key]))


def read_config():
    logger.debug('Reading config...')
    args = DEFAULTS
    args.update(read_config_files())
    args.update(read_environment())
    logger.debug('Verifying config...')
    verify(args)
    logger.debug('OK')
    return args

CONFIG = read_config()
