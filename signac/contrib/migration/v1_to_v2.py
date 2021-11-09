# Copyright (c) 2021 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 1 to version 2.

This migration involves the following changes:
    - Removal of the signac project id
"""

import os

from ...common.config import get_config


def migrate_v1_to_v2(project):
    """Migrate from schema version 1 to version 2."""
    config = get_config(os.path.join(project.root_directory(), "signac.rc"))
    config["schema_version"] = 2
    del config["project"]
    config.write()
