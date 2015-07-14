import logging
import os
import warnings

from .. import VERSION_TUPLE, VERSION
from ..core.config import Config
from .hashing import generate_hash_from_spec
from . import get_project

logger = logging.getLogger(__name__)

def update_version_key(project, version):
    print("Updating version key to {}.".format(version))
    config = Config()
    fn_config = os.path.join(project.root_directory(), 'compdb.rc')
    try:
        config.read(fn_config)
    except FileNotFoundError:
        pass
    config['compdb_version'] = version
    config.verify()
    config.write(fn_config)

def update_010_to_011(project):
    print("Updating project '{}' from version 0.1.0 to 0.1.1 ...".format(project.get_id()))
    msg = "Updating job with old id {} to new id {}."
    old_jobs = set()
    for job in project.find_jobs():
        old_id = job.get_id()
        new_id = generate_hash_from_spec(job.parameters())
        if old_id != new_id:
            print(msg.format(old_id, new_id))
            new_job = project._open_job(parameters=job.parameters(), version=(0,2))
            assert new_job.get_id() == new_id
            with new_job:
                new_job.import_job(job)
            old_jobs.add(job)
    for job in old_jobs:
        job.remove()
    update_version_key(project, (0,1,1))
    print("Done")

def update(args):
    project = get_project()
    project_version_tuple = project.config.get('compdb_version', (0,1,0))
    project_version = '.'.join((str(v) for v in project_version_tuple))
    if project_version_tuple == VERSION_TUPLE:
        print("Project alrady up-to-date. ({}).".format(VERSION))
        return
    msg = "Updating project '{}' with version {} to {}."
    print(msg.format(project.get_id(), project_version, VERSION))
    if project_version_tuple > VERSION_TUPLE:
        msg = "Unable to update project. Project has newer version than the installed version."
        raise RuntimeError(msg)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=UserWarning)
        if project_version_tuple == (0,1):
            update_010_to_011(project)

def setup_parser(parser):
    parser.set_defaults(func = update)
