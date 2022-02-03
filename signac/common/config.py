# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Parses signac config files."""

import logging
import os

from .configobj import ConfigObj, ConfigObjError
from .errors import ConfigError
from .validate import cfg, get_validator

logger = logging.getLogger(__name__)

DEFAULT_FILENAME = ".signacrc"
CONFIG_FILENAMES = [DEFAULT_FILENAME, "signac.rc"]
HOME = os.path.expanduser("~")
CONFIG_PATH = [HOME]
FN_CONFIG = os.path.expanduser("~/.signacrc")


def _search_local(root):
    for fn in CONFIG_FILENAMES:
        fn_ = os.path.abspath(os.path.join(root, fn))
        if os.path.isfile(fn_):
            yield fn_


def _search_tree(root=None):
    """Locates signac configuration files in a directory hierarchy.

    Parameters
    ----------
    root : str
        Path to search. Uses ``os.getcwd()`` if None (Default value = None).

    """
    if root is None:
        root = os.getcwd()
    while True:
        yield from _search_local(root)
        up = os.path.abspath(os.path.join(root, ".."))
        if up == root:
            msg = "Reached filesystem root."
            logger.debug(msg)
            return
        else:
            root = up


def _search_standard_dirs():
    """Locates signac configuration files in standard directories."""
    for path in CONFIG_PATH:
        yield from _search_local(path)


def _read_config_file(filename, configspec=None, *args, **kwargs):
    """Read a configuration file.

    Parameters
    ----------
    filename : str
        The path to the file to read.
    configspec : List[str], optional
        The key-value pairs supported in the config.

    Returns
    --------
    :class:`Config`
        The config contained in the file.
    """
    logger.debug(f"Reading config file '{filename}'.")
    if configspec is None:
        configspec = cfg.split("\n")
    try:
        config = Config(filename, configspec=configspec, *args, **kwargs)
    except (OSError, ConfigObjError) as error:
        msg = "Failed to read configuration file '{}':\n{}"
        raise ConfigError(msg.format(filename, error))
    verification = config.verify()
    if verification is not True:
        logger.debug(
            "Config file '{}' may contain invalid values.".format(
                os.path.abspath(filename)
            )
        )
    return config


def _load_config(root=None, local=False):
    """Load configuration, searching upward from a root path if desired.

    Parameters
    ----------
    root : str
        The path from which to begin searching for config files.
    local : bool, optional
        If ``True``, only search in the provided directory and do not traverse
        upwards through the filesystem (Default value: False).

    Returns
    --------
    :class:`Config`
        The composite configuration including both local and global config data
        if requested.
    """
    if root is None:
        root = os.getcwd()
    config = Config(configspec=cfg.split("\n"))
    if local:
        for fn in _search_local(root):
            tmp = _read_config_file(fn)
            config.merge(tmp)
            if "project" in tmp:
                config["project_dir"] = os.path.dirname(fn)
                break
    else:
        for fn in _search_standard_dirs():
            config.merge(_read_config_file(fn))
        for fn in _search_tree(root):
            tmp = _read_config_file(fn)
            config.merge(tmp)
            if "project" in tmp:
                config["project_dir"] = os.path.dirname(fn)
                break
    return config


class Config(ConfigObj):
    """Manages configuration for a signac project."""

    encoding = "utf-8"

    def verify(self, validator=None, *args, **kwargs):
        """Validate the contents of this configuration."""
        if validator is None:
            validator = get_validator()
        return super().validate(validator, *args, **kwargs)
