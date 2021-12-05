# Copyright (c) 2019-2021 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Handle migrations of signac schema versions."""

import os
import sys

from filelock import FileLock
from packaging import version

from ...common.config import get_config, load_config
from ...version import SCHEMA_VERSION, __version__
from .v0_to_v1 import _load_config_v1, migrate_v0_to_v1

FN_MIGRATION_LOCKFILE = ".SIGNAC_PROJECT_MIGRATION_LOCK"


# Config loaders must be functions whose first argument is a directory from
# which to read configuration information.
_CONFIG_LOADERS = {
    0: _load_config_v1,
    1: load_config,
}


_MIGRATIONS = {
    ("0", "1"): migrate_v0_to_v1,
}


def _collect_migrations(root_directory):
    schema_version = version.parse(SCHEMA_VERSION)

    def get_config_schema_version(version_guess):
        # By default, try loading the schema using the loader corresponding to
        # the expected version.
        # Search versions in reverse order (assumes lexicographic ordering of
        # version strings).
        for version_guess in reversed(sorted(_CONFIG_LOADERS.keys())):
            try:
                config = _CONFIG_LOADERS[version_guess](root_directory)
                break
            except Exception:
                # The load failed, go to the next
                pass
        else:
            raise RuntimeError("Unable to load config file.")
        return version.parse(config["schema_version"])

    current_schema_version = get_config_schema_version(SCHEMA_VERSION)
    if current_schema_version > schema_version:
        # Project config schema version is newer and therefore not supported.
        raise RuntimeError(
            "The signac schema version used by this project is {}, but signac {} "
            "only supports up to schema version {}. Try updating signac.".format(
                get_config_schema_version(), __version__, SCHEMA_VERSION
            )
        )

    guess = current_schema_version
    while get_config_schema_version(guess) < schema_version:
        for (origin, destination), migration in _MIGRATIONS.items():
            if version.parse(origin) == get_config_schema_version(guess):
                yield (origin, destination), migration
                guess = destination
                break
        else:
            raise RuntimeError(
                "The signac schema version used by this project is {}, but signac {} "
                "uses schema version {} and does not know how to migrate.".format(
                    get_config_schema_version(), __version__, schema_version
                )
            )


def apply_migrations(project):
    """Apply migrations to a project."""
    root_directory = project.root_directory()
    lock = FileLock(project.fn(FN_MIGRATION_LOCKFILE))
    try:
        with lock:
            for (origin, destination), migrate in _collect_migrations(root_directory):
                try:
                    print(
                        f"Applying migration for version {origin} to {destination}... ",
                        end="",
                        file=sys.stderr,
                    )
                    migrate(project)
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to apply migration {destination}."
                    ) from e
                else:
                    config = get_config(project.fn("signac.rc"))
                    config["schema_version"] = destination
                    config.write()

                    print("OK", file=sys.stderr)
                    yield origin, destination
    finally:
        try:
            os.unlink(lock.lock_file)
        except FileNotFoundError:
            pass


__all__ = [
    "apply_migrations",
]
