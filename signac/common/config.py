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


def _get_config_dirname(fn):
    # We could use pathlib's `.paths` attribute to remove the trailing project
    # config filename, but that has significant performance
    # implications: this takes ~200 ns, whereas
    # os.path.join(*(pathlib.PosixPath(fn).parts[:-2])) takes ~6 us, almost a
    # 20x slowdown.
    return fn.replace(os.sep + PROJECT_CONFIG_FN, "")


def _search_local(root):
    fn_ = _get_project_config_fn(root)
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


def _search_standard_dirs():
    """Locates signac configuration files in standard directories."""
    # For now this search only finds user-specific files, but it could be
    # updated in the future to support e.g. system-wide config files.
    for fn in (USER_CONFIG_FN,):
        if os.path.isfile(fn):
            yield fn


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


def load_config(root=None, local=False):
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
        search_func = _search_local
    else:
        for fn in _search_standard_dirs():
            config.merge(read_config_file(fn))
        search_func = _search_tree

    for fn in search_func(root):
        tmp = read_config_file(fn)
        config.merge(tmp)
        # Once a valid config file is found, we cease looking any further, i.e.
        # we assume that the first directory with a valid config file is the
        # project root.
        # TODO: Rather than adding the project directory to the config, it
        # should be returned separately (i.e. the return should become a tuple
        # (root_dir, config). The current approach confuses the discovery with
        # the contents of the config.
        config["project_dir"] = _get_config_dirname(fn)
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
