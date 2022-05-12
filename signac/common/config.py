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

PROJECT_CONFIG_FN = os.path.join(".signac", "config")
USER_CONFIG_FN = os.path.expanduser(os.path.join("~", ".signacrc"))

# TODO: Consider making this entire module internal and removing all its
# functions from the public API.


def _get_project_config_fn(path):
    return os.path.abspath(os.path.join(path, PROJECT_CONFIG_FN))


def _locate_config_dir(search_path):
    """Locates directory containing a signac configuration file in a directory hierarchy.

    Parameters
    ----------
    search_path : str
        Starting path to search.

    Returns
    --------
    str or None
        The directory containing the configuration file if one is found, otherwise None.
    """
    search_path = os.path.abspath(search_path)
    while True:
        if os.path.isfile(_get_project_config_fn(search_path)):
            return search_path
        if (up := os.path.dirname(search_path)) == search_path:
            logger.debug("Reached filesystem root, no config found.")
            return None
        else:
            search_path = up


def read_config_file(filename):
    """Read a configuration file.

    Parameters
    ----------
    filename : str
        The path to the file to read.

    Returns
    --------
    :class:`Config`
        The config contained in the file.
    """
    logger.debug(f"Reading config file '{filename}'.")
    configspec = cfg.split("\n")
    try:
        config = Config(filename, configspec=configspec)
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


def load_config(path=None):
    """Load configuration from a project directory.

    Parameters
    ----------
    path : str
        The project path to pull project-local configuration data from.

    Returns
    --------
    :class:`Config`
        The composite configuration including both project-local and global
        config data if requested. Note that because this config is a composite,
        modifications to the returned value will not be reflected in the files.
    """
    if path is None:
        path = os.getcwd()
    config = Config(configspec=cfg.split("\n"))

    # Add in any global or user config files. For now this only finds user-specific
    # files, but it could be updated in the future to support e.g. system-wide config files.
    for fn in (USER_CONFIG_FN,):
        if os.path.isfile(fn):
            config.merge(read_config_file(fn))

    if os.path.isfile(_get_project_config_fn(path)):
        config.merge(read_config_file(_get_project_config_fn(path)))
    return config


class Config(ConfigObj):
    """Manages configuration for a signac project."""

    encoding = "utf-8"

    def verify(self, validator=None, *args, **kwargs):
        """Validate the contents of this configuration."""
        if validator is None:
            validator = get_validator()
        return super().validate(validator, *args, **kwargs)
