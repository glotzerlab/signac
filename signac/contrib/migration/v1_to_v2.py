# Copyright (c) 2022 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 1 to version 2.

This migration involves the following changes:
    - Moving the signac.rc config file to .signac/config
"""

import os

from signac.common import configobj
from signac.common.config import _get_project_config_fn
from signac.contrib.project import Project
from signac.synced_collections.backends.collection_json import BufferedJSONAttrDict

from .v0_to_v1 import _load_config_v1

# A minimal v2 config.
_cfg = """
schema_version = string(default='0')
workspace_dir = string(default='workspace')
"""


def _load_config_v2(root_directory):
    cfg = configobj.ConfigObj(
        os.path.join(root_directory, ".signac", "config"), configspec=_cfg.split("\n")
    )
    validator = configobj.validate.Validator()
    if cfg.validate(validator) is not True:
        raise RuntimeError(
            "This project's config file is not compatible with signac's v2 schema."
        )
    return cfg


def _migrate_v1_to_v2(root_directory):
    """Migrate from schema version 1 to version 2."""
    # Delete project name from config and store in project doc.
    cfg = _load_config_v1(root_directory)
    fn_doc = os.path.join(root_directory, Project.FN_DOCUMENT)
    doc = BufferedJSONAttrDict(filename=fn_doc, write_concern=True)
    doc["signac_project_name"] = cfg["project"]
    del cfg["project"]
    cfg.write()

    # Move signac.rc to .signac/config
    v1_fn = os.path.join(root_directory, "signac.rc")
    v2_fn = _get_project_config_fn(root_directory)
    os.mkdir(os.path.dirname(v2_fn))
    os.rename(v1_fn, v2_fn)
