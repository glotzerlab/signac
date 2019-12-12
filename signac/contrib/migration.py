# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import sys
from abc import abstractmethod
from packaging import version
from contextlib import contextmanager

from filelock import FileLock

from ..common.config import get_config
from ..version import __version__, SCHEMA_VERSION


FN_MIGRATION_LOCKFILE = '.SIGNAC_PROJECT_MIGRATION_LOCK'


class Migration:

    @abstractmethod
    def apply(self, project):
        pass

    @abstractmethod
    def rollback(self, project):
        pass


class Migrate0To1(Migration):

    def apply(self, project):
        pass  # nothing to do

    def rollback(self, project):
        pass  # nothing to do


MIGRATIONS = {
    ('0', '1'):    Migrate0To1(),
}


def _reload_project_config(project):
    project_reloaded = project.get_project(
        root=project._rd, search=False, _ignore_schema_version=True)
    project._config = project_reloaded._config


def _update_project_config(project, **kwargs):
    "Update the project configuration."
    for fn in ('signac.rc', '.signacrc'):
        config = get_config(project.fn(fn))
        if 'project' in config:
            break
    else:
        raise RuntimeError("Unable to determine project configuration file.")
    config.update(kwargs)
    config.write()
    _reload_project_config(project)


def _apply_migrations(project):
    schema_version = version.parse(SCHEMA_VERSION)

    def config_schema_version():
        return version.parse(project._config['schema_version'])

    if config_schema_version() > schema_version:
        # Project config schema version is newer and therefore not supported.
        raise RuntimeError(
            "The signac schema version used by this project is {}, but signac {} "
            "only supports up to schema version {}. Try updating signac.".format(
                config_schema_version, __version__, SCHEMA_VERSION))

    while config_schema_version() < schema_version:
        for (origin, destination), migration in MIGRATIONS.items():
            if version.parse(origin) == config_schema_version():
                try:
                    print("Applying migration for "
                          "version {} to {}... ".format(origin, destination), end='',
                          file=sys.stderr)
                    migration.apply(project)
                except Exception as e:
                    print("FAILED. Rolling back... ", end='', file=sys.stderr)
                    migration.rollback(project)
                    print("DONE", file=sys.stderr)
                    raise RuntimeError(
                        "Failed to apply migration {}.".format(destination)) from e
                else:
                    _update_project_config(project, schema_version=destination)
                    print("OK", file=sys.stderr)
                    break
        else:
            raise RuntimeError(
                "The signac schema version used by this project is {}, but signac {} "
                "uses schema version {} and does not know how to migrate.".format(
                    config_schema_version(), __version__, schema_version))


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


def apply_migrations(project):
    with _lock_for_migration(project):
        _apply_migrations(project)


__all__ = [
    'apply_migrations',
    ]
