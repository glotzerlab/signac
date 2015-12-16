import os
import stat
import logging
import warnings

from .configobj import ConfigObj
from .validate import get_validator, cfg
from .errors import ConfigError

logger = logging.getLogger(__name__)

DEFAULT_FILENAME = '.signacrc'
CONFIG_FILENAMES = [DEFAULT_FILENAME, 'signac.rc']
HOME = os.path.expanduser('~')
CONFIG_PATH = [HOME]
CWD = os.getcwd()
FN_CONFIG = os.path.expanduser('~/.signacrc')


class PermissionsError(ConfigError):
    pass


def search_tree():
    cwd = os.getcwd()
    while(True):
        for filename in CONFIG_FILENAMES:
            fn = os.path.abspath(os.path.join(cwd, filename))
            if os.path.isfile(fn):
                yield fn
        up = os.path.abspath(os.path.join(cwd, '..'))
        if up == cwd:
            msg = "Reached filesystem root."
            logger.debug(msg)
            return
        else:
            cwd = up


def search_standard_dirs():
    for path in CONFIG_PATH:
        for filename in CONFIG_FILENAMES:
            fn = os.path.abspath(os.path.join(path, filename))
            if os.path.isfile(fn):
                yield fn
                return


def check_permissions(filename):
    st = os.stat(filename)
    if st.st_mode & stat.S_IROTH or st.st_mode & stat.S_IRGRP:
        raise PermissionsError("Permissions of configuration file '{fn}'"
                               "allow it to be read by others than the user. "
                               "Unable to read/write password.".format(
                                   fn=filename))


def read_config_file(filename):
    logger.debug("Reading config file '{}'.".format(filename))
    config = Config(filename, configspec=cfg.split('\n'))
    config.validate(get_validator())

    def is_pw(section, key):
        assert not key.endswith('password')
    try:
        config.walk(is_pw)
    except AssertionError:
        check_permissions(filename)
    return config


def write_config(config, filename):
    fn = config.filename
    config.filename = None
    try:
        with open(filename, 'wb') as file:
            for line in config.write():
                file.write((line + '\n').encode(type(config).encoding))
    finally:
        config.filename = fn


def get_config(infile=None, configspec=None, * args, **kwargs):
    if configspec is None:
        configspec = cfg.split('\n')
    return Config(infile, configspec=configspec, *args, **kwargs)


def load_config():
    config = Config(configspec=cfg.split('\n'))
    for fn in search_standard_dirs():
        tmp = read_config_file(fn)
        config.merge(tmp)
    for fn in search_tree():
        tmp = read_config_file(fn)
        config.merge(tmp)
        if 'project' in tmp:
            config['project_dir'] = os.path.split(fn)[0]
            break
    else:
        logger.debug("Did not find a project configuration file.")
    return config


class Config(ConfigObj):
    encoding = 'utf-8'

    def verify(self, validator=None, *args, **kwargs):
        if validator is None:
            validator = get_validator()
        super(Config, self).validate(validator, *args, **kwargs)
