# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Parses signac config files."""

import logging
import os

from ._vendor.configobj import ConfigObj, ConfigObjError
from ._vendor.configobj.validate import Validator
from .errors import ConfigError

logger = logging.getLogger(__name__)

PROJECT_CONFIG_FN = os.path.join(".signac", "config")
USER_CONFIG_FN = os.path.expanduser(os.path.join("~", ".signacrc"))
_CFG = """
schema_version = string(default='1')
"""


def _get_project_config_fn(path):
    return os.path.abspath(os.path.join(path, PROJECT_CONFIG_FN))


def _raise_if_older_schema(root):
    """Raise if an older schema version is detected at the search path.

    Parameters
    ----------
    root : str
        Directory to check schema for.

    Raises
    ------
    IncompatibleSchemaVersion
        If the project uses an older schema version that requires migration.
    """
    from .errors import IncompatibleSchemaVersion
    from .migration import _get_config_schema_version
    from .version import SCHEMA_VERSION, __version__

    try:
        schema_version = _get_config_schema_version(root, int(SCHEMA_VERSION))
        assert schema_version != int(SCHEMA_VERSION), (
            "Migration schema loader succeeded in loading a config file "
            "where normal loader failed. Do you have config files for multiple "
            "schemas? Otherwise, this indicates an internal "
            "error. Please contact the signac developers."
        )
        raise IncompatibleSchemaVersion(
            "Detected signac project using schema version "
            f"{schema_version}, but signac {__version__} requires "
            f"schema version {SCHEMA_VERSION}. Try running python -m "
            "signac migrate."
        )
    except RuntimeError:
        pass


def _locate_config_dir(search_path):
    """Locates directory containing a signac configuration file in a directory hierarchy.

    Parameters
    ----------
    search_path : str
        Starting path to search.

    Returns
    -------
    str or None
        The directory containing the configuration file if one is found, otherwise None.
    """
    orig_search_path = search_path
    search_path = os.path.abspath(search_path)
    while True:
        if os.path.isfile(_get_project_config_fn(search_path)):
            return search_path
        if (up := os.path.dirname(search_path)) == search_path:
            break
        else:
            search_path = up

    logger.debug(
        "Reached filesystem root, no config found. Checking whether a "
        "project created with an older signac schema may be found."
    )

    search_path = os.path.abspath(orig_search_path)
    while True:
        _raise_if_older_schema(search_path)
        if (up := os.path.dirname(search_path)) == search_path:
            logger.debug("Reached filesystem root, no config found.")
            return None
        else:
            search_path = up


class _Config(ConfigObj):
    """Manages configuration for a signac project."""

    def verify(self, *, preserve_errors=False):
        """Validate the contents of this configuration."""
        return super().validate(Validator(), preserve_errors=preserve_errors)


def _read_config_file(filename):
    logger.debug(f"Reading config file '{filename}'.")
    try:
        config = _Config(filename, configspec=_CFG.split("\n"))
    except (OSError, ConfigObjError) as error:
        raise ConfigError(f"Failed to read configuration file '{filename}':\n{error}")
    verification = config.verify()
    # config.verify() returns True if everything succeeded, but if the
    # validation failed it will return a dictionary of invalid results. We
    # cannot simply check for a truthy value here since a non-empty dict will
    # evaluate to True.
    if verification is not True:
        raise ConfigError(
            f"Config file '{os.path.abspath(filename)}' may contain invalid values."
        )
    return config


def _load_config(path=None):
    """Load configuration from a project directory.

    Parameters
    ----------
    path : str
        The project path to pull project-local configuration data from.

    Returns
    -------
    :class:`_Config`
        The composite configuration including both project-local and global
        config data if requested. Note that because this config is a composite,
        modifications to the returned value will not be reflected in the files.
    """
    if path is None:
        path = os.getcwd()
    config = _Config(configspec=_CFG.split("\n"))

    # Add in any global or user config files. For now this only finds user-specific
    # files, but it could be updated in the future to support e.g. system-wide config files.
    for fn in (USER_CONFIG_FN,):
        if os.path.isfile(fn):
            config.merge(_read_config_file(fn))

    if os.path.isfile(_get_project_config_fn(path)):
        config.merge(_read_config_file(_get_project_config_fn(path)))
    return config
