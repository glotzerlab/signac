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


def _get_project_config_fn(root):
    return os.path.abspath(os.path.join(root, PROJECT_CONFIG_FN))


def _contains_config_file(root):
    """Determine if the root directory contains a signac configuration file.

    Parameters
    ----------
    root : str
        Path to search. Uses ``os.getcwd()`` if None (Default value = None).

    Returns
    --------
    str or None
        The path to the configuration file if one is found, otherwise None.
    """
    fn_ = _get_project_config_fn(root)
    if os.path.isfile(fn_):
        return root
    return None


def _locate_config_dir(root):
    """Locates root directory containing a signac configuration file in a directory hierarchy.

    Parameters
    ----------
    root : str
        Starting path to search.

    Returns
    --------
    str or None
        The root directory containing the configuration file if one is found, otherwise None.
    """
    while True:
        if _contains_config_file(root):
            return root
        up = os.path.abspath(os.path.join(root, ".."))
        if up == root:
            logger.debug("Reached filesystem root, no config found.")
            return None
        else:
            root = up


def read_config_file(filename, configspec=None, *args, **kwargs):
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


def load_config(root=None):
    """Load configuration at root directory.

    Parameters
    ----------
    root : str
        The path from which to load the local config.

    Returns
    --------
    :class:`Config`
        The composite configuration including both local and global config data
        if requested. Note that because this config is a composite,
        modifications to the returned value will not be reflected in the files.
    """
    if root is None:
        root = os.getcwd()
    config = Config(configspec=cfg.split("\n"))

    # Add in any global or user config files. For now this search only finds user-specific
    # files, but it could be updated in the future to support e.g. system-wide config files.
    for fn in (USER_CONFIG_FN,):
        if os.path.isfile(fn):
            config.merge(read_config_file(fn))

    if _contains_config_file(root):
        config.merge(read_config_file(_get_project_config_fn(root)))
    return config


class Config(ConfigObj):
    """Manages configuration for a signac project."""

    encoding = "utf-8"

    def verify(self, validator=None, *args, **kwargs):
        """Validate the contents of this configuration."""
        if validator is None:
            validator = get_validator()
        return super().validate(validator, *args, **kwargs)
