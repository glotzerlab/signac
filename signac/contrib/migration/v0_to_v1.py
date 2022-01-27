# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 0 to version 1.

This migration is a null-migration that serves as a template
for future migrations and testing purposes.
"""
import os

from signac.common import configobj

# A minimal v1 config.
_cfg = """
schema_version = string(default='0')
project = string()
workspace_dir = string(default='workspace')
"""


def _load_config_v1(root_directory):
    cfg = configobj.ConfigObj(
        os.path.join(root_directory, "signac.rc"), configspec=_cfg.split("\n")
    )
    validator = configobj.validate.Validator()
    if cfg.validate(validator) is not True:
        raise RuntimeError(
            "This project's config file is not compatible with signac's v1 schema."
        )
    return cfg


def _migrate_v0_to_v1(root_directory):
    """Migrate from schema version 0 to version 1."""
    pass
