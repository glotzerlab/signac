# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Handle migrations of signac schema versions."""

import os
import sys
from contextlib import contextmanager

from filelock import FileLock
from packaging import version

from ...common.config import get_config
from ...version import SCHEMA_VERSION, __version__
from .v0_to_v1 import migrate_v0_to_v1

FN_MIGRATION_LOCKFILE = ".SIGNAC_PROJECT_MIGRATION_LOCK"


MIGRATIONS = {
    ("0", "1"): migrate_v0_to_v1,
}


def _reload_project_config(project):
    project_reloaded = project.get_project(
        root=project.root_directory(), search=False, _ignore_schema_version=True
    )
    project._config = project_reloaded._config


def _update_project_config(project, **kwargs):
    """Update the project configuration."""
    for fn in ("signac.rc", ".signacrc"):
        config = get_config(project.fn(fn))
        if "project" in config:
            break
    else:
        raise RuntimeError("Unable to determine project configuration file.")
    config.update(kwargs)
    config.write()
    _reload_project_config(project)


@contextmanager
def _lock_for_migration(project):
    lock = FileLock(project.fn(FN_MIGRATION_LOCKFILE))
    try:
        with lock:
            yield
    finally:
        try:
            os.unlink(lock.lock_file)
        except FileNotFoundError:
            pass


def _collect_migrations(project):
    schema_version = version.parse(SCHEMA_VERSION)

    def config_schema_version():
        return version.parse(project._config["schema_version"])

    if config_schema_version() > schema_version:
        # Project config schema version is newer and therefore not supported.
        raise RuntimeError(
            "The signac schema version used by this project is {}, but signac {} "
            "only supports up to schema version {}. Try updating signac.".format(
                config_schema_version, __version__, SCHEMA_VERSION
            )
        )

    while config_schema_version() < schema_version:
        for (origin, destination), migration in MIGRATIONS.items():
            if version.parse(origin) == config_schema_version():
                yield (origin, destination), migration
                break
        else:
            raise RuntimeError(
                "The signac schema version used by this project is {}, but signac {} "
                "uses schema version {} and does not know how to migrate.".format(
                    config_schema_version(), __version__, schema_version
                )
            )


def apply_migrations(project):
    """Apply migrations to a project."""
    with _lock_for_migration(project):
        for (origin, destination), migrate in _collect_migrations(project):
            try:
                print(
                    f"Applying migration for version {origin} to {destination}... ",
                    end="",
                    file=sys.stderr,
                )
                migrate(project)
            except Exception as e:
                raise RuntimeError(f"Failed to apply migration {destination}.") from e
            else:
                _update_project_config(project, schema_version=destination)
                print("OK", file=sys.stderr)
                yield origin, destination


__all__ = [
    "apply_migrations",
]
