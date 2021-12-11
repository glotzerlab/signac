# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 0 to version 1.

This migration is a null-migration that serves as a template
for future migrations and testing purposes.
"""
import os
import warnings

from deprecation import deprecated

from ...version import __version__

# A minimal v1 config.
_cfg = """
schema_version = string()
"""


def _load_config_v1(root_directory):
    try:
        import configobj
    except ModuleNotFoundError:
        try:
            from signac.common import configobj
        except ModuleNotFoundError:
            raise RuntimeError(
                "signac schema version 1 can only be read with configobj. "
                "Please install configobj and try again."
            )
    cfg = configobj.ConfigObj(
        os.path.join(root_directory) + "signac.rc", configspec=_cfg.split("\n")
    )
    validator = configobj.validate.Validator()
    if not cfg.validate(validator):
        raise RuntimeError(
            "This project's config file is not compatible with signac's v1 schema."
        )
    return cfg


def _migrate_v0_to_v1(root_directory):
    """Migrate from schema version 0 to version 1."""
    from ..project import Project

    if isinstance(root_directory, Project):
        warnings.warn(
            "Migrations should be applied to a directory containing a signac project, "
            "not a project object.",
            FutureWarning,
        )
        root_directory = root_directory.root_directory()
    # nothing to do here, serves purely as an example


@deprecated(
    deprecated_in="1.7",
    removed_in="2.0",
    current_version=__version__,
    details=("Migrations should not be invoked directly, only via `apply_migrations`."),
)
def migrate_v0_to_v1(root_directory):
    """Migrate from schema version 0 to version 1."""
    _migrate_v0_to_v1(root_directory)
