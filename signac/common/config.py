# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Parses signac config files."""

import logging
import os

from ..common.deprecation import deprecated
from ..version import __version__
from .configobj import ConfigObj, ConfigObjError
from .errors import ConfigError
from .validate import cfg, get_validator

logger = logging.getLogger(__name__)

PROJECT_CONFIG_FN = os.path.join(".signac", "config")
USER_CONFIG_FN = os.path.expanduser(os.path.join("~", ".signacrc"))


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
        "Reached filesystem root, no config found. Checking whether we "
        "can instead find a project created with an older signac schema."
    )

    from ..contrib.errors import IncompatibleSchemaVersion
    from ..contrib.migration import _get_config_schema_version
    from ..version import SCHEMA_VERSION, __version__

    schema_version = int(SCHEMA_VERSION)
    search_path = orig_search_path

    while True:
        try:
            schema_version = _get_config_schema_version(search_path, schema_version)
            assert schema_version != SCHEMA_VERSION, (
                "Migration schema loader succeeded in loading a config file "
                "where normal loader failed. This indicates an internal "
                "error, please contact the signac developers."
            )
            raise IncompatibleSchemaVersion(
                "The signac schema version used by this project is "
                f"{schema_version}, but signac {__version__} only "
                f"supports up to schema version {SCHEMA_VERSION}. Try running "
                "python -m signac migrate."
            )
        except RuntimeError:
            # No config file was found at this level, go to the next one.
            if (up := os.path.dirname(search_path)) == search_path:
                logger.debug("Reached filesystem root, no config found.")
                break
            else:
                search_path = up


def _read_config_file(filename):
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


@deprecated(
    deprecated_in="1.8",
    removed_in="2.0",
    current_version=__version__,
    details=(
        "The read_config_file method is deprecated. Configs should only be "
        "accessed via a Project instance.",
    ),
)
def read_config_file(filename):
    """Read a configuration file."""
    return _read_config_file(filename)


def _load_config(path=None):
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
            config.merge(_read_config_file(fn))

    if os.path.isfile(_get_project_config_fn(path)):
        config.merge(_read_config_file(_get_project_config_fn(path)))
    return config


@deprecated(
    deprecated_in="1.8",
    removed_in="2.0",
    current_version=__version__,
    details=(
        "The load_config method is deprecated. Configs should only be "
        "accessed via a Project instance.",
    ),
)
def load_config(root=None):
    """Load configuration, searching upward from a root path."""
    return _load_config(root)


class Config(ConfigObj):
    """Manages configuration for a signac project."""

    encoding = "utf-8"

    def verify(self, validator=None, *args, **kwargs):
        """Validate the contents of this configuration."""
        if validator is None:
            validator = get_validator()
        return super().validate(validator, *args, **kwargs)
