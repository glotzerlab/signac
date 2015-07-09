import logging
logger = logging.getLogger(__name__)

import warnings

def update_version_key(project, version):
    import os
    from ..core.config import Config
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

def update_01_to_02(project):
    from .hashing import generate_hash_from_spec
    print("Updating project '{}' from version 0.1 to 0.2 ...".format(project.get_id()))
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
    update_version_key(project, (0,2))
    print("Done")

def update(args):
    from . import get_project
    from .. import VERSION_TUPLE, VERSION
    project = get_project()
    project_version = project.config.get('compdb_version', (0,1))
    if project_version == VERSION_TUPLE:
        print("Project alrady up-to-date. ({}).".format(VERSION))
        return
    msg = "Updating project '{}' with version {} to {}."
    print(msg.format(project.get_id(), project_version, VERSION))
    if project_version > VERSION_TUPLE:
        msg = "Unable to update project. Project has newer version than the installed version."
        raise RuntimeError(msg)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=UserWarning)
        if project_version == (0,1):
            update_01_to_02(project)

def setup_parser(parser):
    parser.set_defaults(func = update)
