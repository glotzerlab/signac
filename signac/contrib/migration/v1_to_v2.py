# Copyright (c) 2022 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 1 to version 2.

This migration involves the following changes:
"""

import os

from signac.common import configobj

# A minimal v2 config.
_cfg = """
schema_version = string(default='0')
project = string()
workspace_dir = string(default='workspace')
"""


def _load_config_v2(root_directory):
    cfg = configobj.ConfigObj(
        os.path.join(root_directory, "signac.rc"), configspec=_cfg.split("\n")
    )
    validator = configobj.validate.Validator()
    if cfg.validate(validator) is not True:
        raise RuntimeError(
            "This project's config file is not compatible with signac's v1 schema."
        )
    return cfg


def _migrate_v1_to_v2(project):
    """Migrate from schema version 1 to version 2."""
    pass
