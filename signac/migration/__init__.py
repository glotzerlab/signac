# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Handle migrations of signac schema versions."""

import os

from filelock import FileLock

from .._utility import _print_err
from ..version import SCHEMA_VERSION, __version__
from .v0_to_v1 import _load_config_v1, _migrate_v0_to_v1
from .v1_to_v2 import _load_config_v2, _migrate_v1_to_v2

FN_MIGRATION_LOCKFILE = ".SIGNAC_PROJECT_MIGRATION_LOCK"


# Config loaders must be functions with the signature
# def config_loader(root_directory: str) -> MutableMapping
# When a new schema version is introduced, a corresponding loader only needs to
# be added if the old loader will no longer function.  This dictionary must
# contain all unique loaders for schema versions that are supported as starting
# points for migration. The resulting MutableMapping config objects must be
# writeable, i.e. it must be possible to persist in-memory changes from these
# objects to the underlying config files.
_CONFIG_LOADERS = {
    1: _load_config_v1,
    2: _load_config_v2,
}


_MIGRATIONS = {
    (0, 1): _migrate_v0_to_v1,
    (1, 2): _migrate_v1_to_v2,
}

_VERSION_LIST = list(reversed(sorted(_CONFIG_LOADERS.keys())))


def _get_config_schema_version(root_directory, version_guess):
    # Try loading the schema using the loader corresponding to the expected
    # version if it has a configured loader.
    versions = _VERSION_LIST
    if version_guess in _CONFIG_LOADERS:
        versions = [version_guess] + versions
    for guess in versions:
        try:
            # Note: We could consider using a different component as the key
            # for _CONFIG_LOADERS, but since this is an internal detail it's
            # not terribly consequential.
            config = _CONFIG_LOADERS[guess](root_directory)
            break
        except Exception:
            # The load failed, go to the next
            pass
    else:
        raise RuntimeError("Unable to load config file.")
    try:
        return int(config["schema_version"])
    except KeyError:
        # The default schema version is version 0.
        return 0


def _collect_migrations(root_directory):
    schema_version = int(SCHEMA_VERSION)

    current_schema_version = _get_config_schema_version(root_directory, schema_version)
    if current_schema_version > schema_version:
        # Project config schema version is newer and therefore not supported.
        raise RuntimeError(
            "The signac schema version used by this project is "
            f"{current_schema_version}, but signac {__version__} only "
            f"supports up to schema version {SCHEMA_VERSION}. Try updating "
            "signac."
        )

    guess = current_schema_version
    while _get_config_schema_version(root_directory, guess) < schema_version:
        for (origin, destination), migration in _MIGRATIONS.items():
            if origin == _get_config_schema_version(root_directory, guess):
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
                    _print_err(
                        f"Applying migration for version {origin} to {destination}... ",
                        end="",
                    )
                    migrate(root_directory)
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to apply migration {destination}."
                    ) from e
                else:
                    config = _CONFIG_LOADERS[destination](root_directory)
                    config["schema_version"] = destination
                    config.write()
                    _print_err("OK")
    finally:
        try:
            os.unlink(lock.lock_file)
        except FileNotFoundError:
            pass


__all__ = [
    "apply_migrations",
]
