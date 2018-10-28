# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import stat
import logging

from .configobj import ConfigObj, ConfigObjError
from .validate import get_validator, cfg
from .errors import ConfigError

logger = logging.getLogger(__name__)

DEFAULT_FILENAME = '.signacrc'
CONFIG_FILENAMES = [DEFAULT_FILENAME, 'signac.rc']
HOME = os.path.expanduser('~')
CONFIG_PATH = [HOME]
FN_CONFIG = os.path.expanduser('~/.signacrc')


class PermissionsError(ConfigError):
    pass


def search_tree(root=None):
    if root is None:
        root = os.getcwd()
    while(True):
        for fn in _search_local(root):
            yield fn
        up = os.path.abspath(os.path.join(root, '..'))
        if up == root:
            msg = "Reached filesystem root."
            logger.debug(msg)
            return
        else:
            root = up


def search_standard_dirs():
    for path in CONFIG_PATH:
        for fn in _search_local(path):
            yield fn


def check_permissions(filename):
    st = os.stat(filename)
    if st.st_mode & stat.S_IROTH or st.st_mode & stat.S_IRGRP:
        raise PermissionsError("Permissions of configuration file '{fn}'"
                               "allow it to be read by others than the user. "
                               "Unable to read/write password.".format(
                                   fn=filename))


def fix_permissions(filename):
    os.chmod(filename, stat.S_IRUSR | stat.S_IWUSR)


def check_and_fix_permissions(filename):
    try:
        check_permissions(filename)
    except PermissionsError as permissions_error:
        logger.debug(
            "{} Attempting to fix permissions.".format(permissions_error))
        try:
            fix_permissions(filename)
        except Exception as error:
            logger.error(
                "Failed to fix permissions with error: {}".format(error))
            raise permissions_error
        else:
            logger.debug("Fixed permissions.")


def read_config_file(filename):
    logger.debug("Reading config file '{}'.".format(filename))
    try:
        config = Config(filename, configspec=cfg.split('\n'))
    except (IOError, OSError, ConfigObjError) as error:
        msg = "Failed to read configuration file '{}':\n{}"
        raise ConfigError(msg.format(filename, error))
    verification = config.verify()
    if verification is not True:
        logger.debug("Config file '{}' may contain invalid values.".format(
            os.path.abspath(filename)))
    if config.has_password():
        check_and_fix_permissions(filename)
    return config


def get_config(infile=None, configspec=None, * args, **kwargs):
    if configspec is None:
        configspec = cfg.split('\n')
    return Config(infile, configspec=configspec, *args, **kwargs)


def _search_local(root):
    for fn in CONFIG_FILENAMES:
        fn_ = os.path.abspath(os.path.join(root, fn))
        if os.path.isfile(fn_):
            yield fn_


def load_config(root=None, local=False):
    if root is None:
        root = os.getcwd()
    config = Config(configspec=cfg.split('\n'))
    if local:
        for fn in _search_local(root):
            tmp = read_config_file(fn)
            config.merge(tmp)
            if 'project' in tmp:
                config['project_dir'] = os.path.dirname(fn)
                break
    else:
        for fn in search_standard_dirs():
            config.merge(read_config_file(fn))
        for fn in search_tree(root):
            tmp = read_config_file(fn)
            config.merge(tmp)
            if 'project' in tmp:
                config['project_dir'] = os.path.dirname(fn)
                break
    return config


class Config(ConfigObj):
    encoding = 'utf-8'

    def verify(self, validator=None, *args, **kwargs):
        if validator is None:
            validator = get_validator()
        return super(Config, self).validate(validator, *args, **kwargs)

    def has_password(self):
        def is_pw(section, key):
            assert not key.endswith('password')
        try:
            self.walk(is_pw)
            return False
        except AssertionError:
            return True

    def write(self, outfile=None, section=None):
        if outfile is not None:
            if self.has_password():
                check_and_fix_permissions(outfile)
        return super(Config, self).write(outfile=outfile, section=section)
