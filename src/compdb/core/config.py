import os

import logging
logger = logging.getLogger('config')

DEFAULT_FILENAME = 'compdb.rc'
CONFIG_FILENAMES = ['compdb.rc',]
HOME = os.path.expanduser('~')
CONFIG_PATH = [HOME]
CWD = os.getcwd()

ENVIRONMENT_VARIABLES = {
    'author_name' :     'COMPDB_AUTHOR_NAME',
    'author_email':     'COMPDB_AUTHOR_EMAIL',
    'project':          'COMPDB_PROJECT',
    'project_dir' :     'COMPDB_PROJECT_DIR',
    'filestorage_dir':  'COMPDB_FILESTORAGE_DIR',
    'working_dir':      'COMPDB_WORKING_DIR',
    'database_host':    'COMPDB_DATABASE_HOST',
    'develop':          'COMPDB_DEVELOP',
}

REQUIRED_KEYS = [
    'author_name', 'author_email', 'project',
    'project_dir',  'filestorage_dir', 'working_dir',
    ]

DEFAULTS = {
    'database_host': 'localhost',
    'database_meta': '_compdb',
    'database_global_fs': '_compdb_fs',
}

LEGAL_ARGS = REQUIRED_KEYS + list(DEFAULTS.keys()) + [
    'global_fs_dir', 'develop', 
    ]


class Config(object):   

    def __init__(self, args = None):
        self._args = DEFAULTS
        if args is not None:
            self.update(args)

    def __str__(self):
        return str(self._args)

    def read(self, filename = DEFAULT_FILENAME):
        import json
        with open(filename) as file:
            args = json.loads(file.read())
            logger.debug("Read: {}".format(args))
        self._args.update(args)

    def _read_files(self):
        for fn in search_config_files():
            try:
                logger.debug("Reading config file '{}'.".format(fn))
                self.read(fn)
            except Exception as error:
                msg = "Error while reading config file '{}'."
                logger.error(msg.format(fn))

    def update(self, args):
        self._args.update(args)

    def load(self):
        logger.debug('Reading config...')
        self._read_files()
        self._args.update(read_environment())
        logger.debug('Verifying config...')
        self.verify()
        logger.debug('OK')

    def verify(self):
        verify(self._args)

    def write(self, filename = DEFAULT_FILENAME, indent = 0, keys = None):
        if keys is None:
            args = self._args
        else:
            args = {k: self._args[k] for k in keys if k in self._args}
        import json
        with open(filename, 'w') as file:
            json.dump(args, file, indent = indent)

    def dump(self, indent = 0, keys = None):
        import json
        if keys is None:
            args = self._args
        else:
            args = {k: self._args[k] for k in keys if k in self._args}
        print(json.dumps(args, indent = indent))

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

    def __contains__(self, key):
        return key in self._args

def search_tree():
    from os import getcwd
    from os.path import realpath, join, isfile
    cwd = os.getcwd()
    while(True):
        for filename in CONFIG_FILENAMES:
            fn = realpath(join(cwd, filename))
            if isfile(fn):
                yield fn
                return
        up = realpath(join(cwd, '..'))
        if up == cwd:
            msg = "Did not find project configuration file."
            logger.debug(msg)
            return
            #raise FileNotFoundError(msg)
        else:
            cwd = up

def search_standard_dirs():
    from os.path import realpath, join, isfile
    for path in CONFIG_PATH:
        for filename in CONFIG_FILENAMES:
            fn = realpath(join(path, filename))
            if isfile(fn):
                yield fn
                return

def search_config_files():
    yield from search_standard_dirs()
    yield from search_tree()

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

    for key in REQUIRED_KEYS:
        if not key in args.keys():
            msg = "Missing required config key: '{}'."
            #logger.warning(msg.format(key))
            #raise KeyError(msg.format(key))

    # sanity check
    #assert set(args.keys()).issubset(set(LEGAL_ARGS))
    #assert set(REQUIRED_KEYS).issubset(set(args.keys()))

    DIRS = ['working_dir', 'project_dir', 'filestorage_dir', 'global_fs_dir']
    dirs = [dir for dir in DIRS if dir in args]

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
                raise NotADirectoryError(msg.format(dir_key, args[dir_key]))

def load_config():
    config = Config()
    config.load()
    return config
