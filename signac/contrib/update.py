import logging
import os
import warnings
import json
import glob
from itertools import chain

import pymongo

from .. import VERSION_TUPLE, VERSION
from ..common.config import Config, search_tree, search_standard_dirs
from .hashing import generate_hash_from_spec
from . import get_project

logger = logging.getLogger(__name__)


def update_version_key(project, version):
    config = Config()
    fn_config = os.path.join(project.root_directory(), 'signac.rc')
    try:
        config.read(fn_config)
    except FileNotFoundError:
        pass
    config['signac_version'] = version
    config.verify()
    config.write(fn_config)


def get_version_key(project):
    return project.config.get('signac_version', (0, 1, 0))


def version_str(version_tuple):
    return '.'.join((str(v) for v in version_tuple))


def update_dummy(project, old_version, new_version):
    msg = "Updating project '{}' from version {} to {} ..."
    print(msg.format(project.get_id(), version_str(
        old_version), version_str(new_version)))
    update_version_key(project, new_version)


def update_010_to_011(project):
    print("Updating project '{}' from version 0.1.0 to 0.1.1 ...".format(
        project.get_id()))
    msg = "Updating job with old id {} to new id {}."
    old_jobs = set()
    for job in project.find_jobs():
        old_id = job.get_id()
        new_id = generate_hash_from_spec(job.parameters())
        if old_id != new_id:
            print(msg.format(old_id, new_id))
            new_job = project._open_job(
                parameters=job.parameters(), version=(0, 2))
            assert new_job.get_id() == new_id
            with new_job:
                new_job.import_job(job)
            old_jobs.add(job)
    for job in old_jobs:
        job.remove()
    update_version_key(project, (0, 1, 1))


def update_016_to_017(project):
    print("Updating project '{}' from version 0.1.6 to 0.1.7 ...".format(
        project.get_id()))
    msg = "Updating configuration file '{}'."
    for fn_config in chain(search_tree(), search_standard_dirs()):
        print(msg.format(fn_config))
        with open(fn_config, 'rb') as file:
            config = json.loads(file.read().decode())
        new_config = dict()
        for key, value in config.items():
            new_config[key.replace('compdb', 'signac').replace(
                'compmatdb', 'signacdb')] = value
        Config(new_config).write(fn_config)
    print("Renaming collections.")
    for name in ('compdb_jobs', 'compdb_job_queue', 'compdb_job_results', 'compdbfetched_set',):
        try:
            project.get_db()[name].rename(name.replace('compdb', 'signac'))
        except pymongo.errors.OperationFailure as error:
            if 'source namespace does not exist' in str(error) or \
                    'target namespace exists' in str(error):
                pass
            else:
                raise
        else:
            print("Rename database collection '{}' to '{}'.".format(
                name, name.replace('compdb', 'signac')))
    print("Renaming job files.")
    for job in project.find_jobs():
        for dir in (job.get_workspace_directory(), job.get_filestorage_directory()):
            for fn in chain(('compdb_jobs.json', '.compdb.json'), glob.glob(os.path.join(dir, '.compdb.*.OPEN'))):
                try:
                    fn_0 = os.path.join(dir, fn)
                    fn_1 = os.path.join(dir, fn.replace('compdb', 'signac'))
                    os.rename(fn_0, fn_1)
                except FileNotFoundError:
                    pass
                else:
                    print("Rename file '{}' to '{}'.".format(fn_0, fn_1))
    update_version_key(project, (0, 1, 7))


def update(args):
    project = get_project()
    project_version_tuple = get_version_key(project)
    project_version = '.'.join((str(v) for v in project_version_tuple))
    if project_version_tuple == VERSION_TUPLE:
        print("Project already up-to-date. ({}).".format(VERSION))
        return
    msg = "Updating project '{}'."
    print(msg.format(project.get_id(), project_version, VERSION))
    if project_version_tuple > VERSION_TUPLE:
        msg = "Unable to update project. Project has newer version than the installed version."
        raise RuntimeError(msg)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=UserWarning)
        if project_version_tuple == (0, 1, 0):
            update_010_to_011(project)
            update_dummy(project, get_version_key(project), (0, 1, 6))
        if project_version_tuple <= (0, 1, 7):
            update_016_to_017(project)
    if get_version_key(project) < VERSION_TUPLE:
        update_dummy(project, get_version_key(project), VERSION_TUPLE)
    print("Done")


def setup_parser(parser):
    parser.set_defaults(func=update)
