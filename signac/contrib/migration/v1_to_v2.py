# Copyright (c) 2022 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 1 to version 2.

This migration involves the following changes:
    - Moving the signac.rc config file to .signac/config
    - Moving .signac_shell_history to .signac/shell_history
    - Moving .signac_sp_cache.json.gz to .signac/statepoint_cache.json.gz
    - Removing the project name from the config. Projects are now identified
      solely by their directories.
    - Removing the workspace_dir key from the config. The workspace directory
      is no longer configurable.
"""

import os

from signac._synced_collections.backends.collection_json import BufferedJSONAttrDict
from signac.common import configobj
from signac.common.config import _get_project_config_fn
from signac.contrib.project import Project

from .v0_to_v1 import _load_config_v1

# A minimal v2 config.
_cfg = """
schema_version = string(default='0')
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
    # Load the v1 config.
    cfg = _load_config_v1(root_directory)
    fn_doc = os.path.join(root_directory, Project.FN_DOCUMENT)
    doc = BufferedJSONAttrDict(filename=fn_doc, write_concern=True)

    # Try to migrate a custom workspace directory if one exists.
    current_workspace_name = cfg.get("workspace_dir")
    if current_workspace_name is not None:
        if current_workspace_name != "workspace":
            current_workspace = os.path.join(root_directory, current_workspace_name)
            new_workspace = os.path.join(root_directory, "workspace")
            if os.path.exists(new_workspace):
                raise RuntimeError(
                    "Workspace directories are no longer configurable in schema version 2, and "
                    f"must be 'workspace', but {new_workspace} already exists. Please remove or "
                    f"move it so that the currently configured workspace directory "
                    f"{current_workspace} can be moved to {new_workspace}."
                )
            os.replace(current_workspace, new_workspace)
        del cfg["workspace_dir"]

    # Delete project name from config and store in project doc.
    doc["signac_project_name"] = cfg["project"]
    del cfg["project"]
    cfg.write()

    # Move signac.rc to .signac/config
    v1_fn = os.path.join(root_directory, "signac.rc")
    v2_fn = _get_project_config_fn(root_directory)
    os.mkdir(os.path.dirname(v2_fn))
    os.replace(v1_fn, v2_fn)

    # Now move all other files.
    files_to_move = {
        ".signac_shell_history": os.sep.join((".signac", "shell_history")),
        ".signac_sp_cache.json.gz": os.sep.join(
            (".signac", "statepoint_cache.json.gz")
        ),
    }
    for src, dst in files_to_move.items():
        os.replace(
            os.sep.join((root_directory, src)), os.sep.join((root_directory, dst))
        )
