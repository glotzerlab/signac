# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 0 to version 1.

This migration is a null-migration that serves as a template
for future migrations and testing purposes.
"""
import os

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
            "This project's config file is not compatible with " "signac's v1 schema."
        )
    return cfg


def _is_schema_version_1(root_directory):
    cfg = _load_config_v1(root_directory)
    return cfg["schema_version"] == 1


def migrate_v0_to_v1(project):
    """Migrate from schema version 0 to version 1."""
    pass  # nothing to do here, serves purely as an example
