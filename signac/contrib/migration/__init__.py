# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Handle migrations of signac schema versions."""

import os
import sys

from filelock import FileLock
from packaging import version

from ...common.config import get_config, load_config
from ...version import SCHEMA_VERSION, __version__

# To be uncommented when switching to version 2.
# from .v0_to_v1 import _load_config_v1, _migrate_v0_to_v1
from .v0_to_v1 import _migrate_v0_to_v1

FN_MIGRATION_LOCKFILE = ".SIGNAC_PROJECT_MIGRATION_LOCK"


# Config loaders must be functions whose first argument is a directory from
# which to read configuration information. If the logic for loading a schema
# has not changed from previous versions, it need not be added to this dict.
# This dictionary only needs to contains all unique loaders in signac's history
# to ensure that any prior config may be loaded for migration.
_CONFIG_LOADERS = {
    # The following line should be uncommented when schema version 2 is
    # introduced, making load_config fail for v1 schemas.
    # "1": _load_config_v1,
    "1": load_config,  # The latest version uses config.load_config
}


_MIGRATIONS = {
    ("0", "1"): _migrate_v0_to_v1,
}


def _get_config_schema_version(root_directory, version_guess):
    # By default, try loading the schema using the loader corresponding to
    # the expected version.
    versions = [version_guess] + list(
        reversed(sorted(version.parse(v) for v in _CONFIG_LOADERS.keys()))
    )
    for guess in versions:
        try:
            # Note: We could consider using a different component as the key
            # for _CONFIG_LOADERS, but since this is an internal detail it's
            # not terribly consequential.
            config = _CONFIG_LOADERS[guess.public](root_directory)
            break
        except Exception:
            # The load failed, go to the next
            pass
    else:
        raise RuntimeError("Unable to load config file.")
    try:
        return version.parse(config["schema_version"])
    except KeyError:
        # The default schema version is version 0.
        return version.parse("0")


def _collect_migrations(root_directory):
    schema_version = version.parse(SCHEMA_VERSION)

    current_schema_version = _get_config_schema_version(root_directory, SCHEMA_VERSION)
    if current_schema_version > schema_version:
        # Project config schema version is newer and therefore not supported.
        raise RuntimeError(
            f"The signac schema version used by this project is "
            f"{current_schema_version}, but signac {__version__} only "
            f"supports up to schema version {SCHEMA_VERSION}. Try updating "
            "signac."
        )

    guess = current_schema_version
    while _get_config_schema_version(root_directory, guess) < schema_version:
        for (origin, destination), migration in _MIGRATIONS.items():
            if version.parse(origin) == _get_config_schema_version(
                root_directory, guess
            ):
                yield (origin, destination), migration
                guess = destination
                break
        else:
            raise RuntimeError(
                "The signac schema version used by this project is "
                f"{_get_config_schema_version(root_directory, guess)}, but "
                f"signac {__version__} uses schema version {schema_version} "
                "and does not know how to migrate."
            )


def apply_migrations(root_directory):
    """Apply migrations to a project.

    This function identifies and performs all the necessary schema migrations
    to bring a project up to date with the current schema version of signac.
    The calling code does not require prior knowledge of the schema version of
    the project, and the function is idempotent when applied to projects that
    already have an up-to-date schema.

    Parameters
    ----------
    root_directory : str
        The path to the project to migrate.
    """
    try:
        lock = FileLock(os.path.join(root_directory, FN_MIGRATION_LOCKFILE))
        with lock:
            for (origin, destination), migrate in _collect_migrations(root_directory):
                try:
                    print(
                        f"Applying migration for version {origin} to {destination}... ",
                        end="",
                        file=sys.stderr,
                    )
                    migrate(root_directory)
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to apply migration {destination}."
                    ) from e
                else:
                    config = get_config(os.path.join(root_directory, "signac.rc"))
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
