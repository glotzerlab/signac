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

PROJECT_CONFIG_FN = "signac.rc"
USER_CONFIG_FN = os.path.expanduser("~/.signacrc")


def _search_local(root):
    fn_ = os.path.abspath(os.path.join(root, PROJECT_CONFIG_FN))
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
            logger.debug("Reached filesystem root, no config found.")
            return
        else:
            root = up


def search_standard_dirs():
    """Locates signac configuration files in standard directories."""
    # For now this search only finds user-specific files, but it could be
    # updated in the future to support e.g. system-wide config files.
    for fn in (USER_CONFIG_FN,):
        if os.path.isfile(fn):
            yield fn


def read_config_file(filename):
    """Read a configuration file."""
    logger.debug(f"Reading config file '{filename}'.")
    try:
        config = Config(filename, configspec=cfg.split("\n"))
    except (OSError, ConfigObjError) as error:
        raise ConfigError(f"Failed to read configuration file '{filename}':\n{error}")
    verification = config.verify()
    if verification is not True:
        # TODO: In the future this should raise an error, not just a
        # debug-level logging notice.
        logger.debug(
            "Config file '{}' may contain invalid values.".format(
                os.path.abspath(filename)
            )
        )
    return config


def get_config(infile=None, configspec=None, *args, **kwargs):
    """Get configuration from a file."""
    if configspec is None:
        configspec = cfg.split("\n")
    return Config(infile, configspec=configspec, *args, **kwargs)


def load_config(root=None, local=False):
    """Load configuration, searching upward from a root path."""
    if root is None:
        root = os.getcwd()
    config = Config(configspec=cfg.split("\n"))

    if local:
        # Local searches cannot proceed up the tree.
        search_func = _search_local
    else:
        # For non-local searches we grab the user's global config file first.
        for fn in search_standard_dirs():
            config.merge(read_config_file(fn))
        search_func = _search_tree

    root_dir = None
    for fn in search_func(root):
        tmp = read_config_file(fn)
        config.merge(tmp)
        # Once a valid config file is found, we cease looking any further, i.e.
        # we assume that the first directory with a valid config file is the
        # project root.
        root_dir = os.path.dirname(fn)
        break
    return root_dir, config


class Config(ConfigObj):
    """Manages configuration for a signac project."""

    encoding = "utf-8"

    def verify(self, validator=None, *args, **kwargs):
        """Validate the contents of this configuration."""
        if validator is None:
            validator = get_validator()
        return super().validate(validator, *args, **kwargs)
