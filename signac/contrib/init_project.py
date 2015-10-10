#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import stat
import argparse

from ..common.config import load_config, get_config, read_config_file
from .. import VERSION_TUPLE
from .templates import TEMPLATES
from . import get_project

logger = logging.getLogger(__name__)

DEFAULT_WORKSPACE = 'workspace'
DEFAULT_STORAGE = 'storage'
SCRIPT_HEADER = "#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n"
DEFAULT_VERSION = VERSION_TUPLE

MSG_SUCCESS = """Successfully created project '{project_name}' in directory '{project_dir}'.
Execute `signac check` to check your configuration."""
MSG_AUTHOR_INCOMPLETE = "Author information is incomplete. This will lead to problems during project execution. Execute `signac config` to create missing author information."


def check_for_existing_project(args):
    try:
        project = get_project()
        root_dir = project.root_directory()
    except (LookupError, FileNotFoundError, NotADirectoryError, KeyError):
        pass
    else:
        if os.path.abspath(root_dir) == os.path.abspath(args.directory):
            msg = "Project in directory '{}' already exists. Use '-f' or '--force' argument to ignore this warning and create a project anyways. This will lead to potential data loss!"
            print(msg.format(os.path.abspath(args.directory)))
            return True
    return False


def adjust_args(args):
    if args.workspace:
        args['workspace_dir'] = os.path.abspath(args.workspace)
    if args.storage:
        args['filestorage_dir'] = os.path.abspath(args.storage)


def make_dir(dirname):
    try:
        os.makedirs(dirname)
    except OSError:
        pass


def generate_config(args):
    global_config = load_config()
    if not args.workspace and global_config.get('workspace_dir') is None:
        args.workspace = DEFAULT_WORKSPACE
    if not args.storage and global_config.get('filestorage_dir') is None:
        args.storage = DEFAULT_STORAGE
    c_args = {
        'project':  args.project_name,
        'signac_version': args.signac_version,
    }
    if args.workspace:
        make_dir(args.workspace)
        c_args['workspace_dir'] = args.workspace
    if args.storage:
        make_dir(args.storage)
        c_args['filestorage_dir'] = args.storage
    if args.db_host:
        c_args['database_host'] = args.db_host
    fn_config = os.path.join(args.directory, 'signac.rc')
    try:
        config = read_config_file(fn_config)
    except FileNotFoundError:
        config = get_config()
    config.update(c_args)
    config.verify()
    return config


def get_templates():
    return TEMPLATES.keys()


def copy_templates(args):
    template = TEMPLATES[args.template]
    for filename, content in template.items():
        fn = os.path.join(args.directory, filename)
        if os.path.isfile(fn):
            msg = "Skipping template file '{}' because a file with the same name already exists."
            logger.warning(msg.format(fn))
            # warnings.warn(msg.format(fn), UserWarning)  # Too intimitading.
            #raise FileExistsError(msg.format(fn))
        else:
            with open(fn, 'wb') as file:
                c = SCRIPT_HEADER + content
                file.write(c.encode('utf-8'))
            os.chmod(fn, os.stat(fn).st_mode | stat.S_IEXEC)


def init_project(args):
    adjust_args(args)
    if not args.force:
        if check_for_existing_project(args):
            return
    config = generate_config(args)
    copy_templates(args)
    config.filename = os.path.join(args.directory, 'signac.rc')
    config.write()
    print(MSG_SUCCESS.format(
        project_name=args.project_name,
        project_dir=os.path.abspath(args.directory)))


def setup_parser(parser):
    parser.add_argument(
        'project_name',
        type=str,
        help="The project unique identifier.",
    )
    parser.add_argument(
        '-d', '--directory',
        type=str,
        default='.',
        help="The project's root directory. Defaults to the current working directory.",
    )
    parser.add_argument(
        '-w', '--workspace',
        type=str,
        help="The project's workspace directory. Defaults to directory '{}' within the project's root directory.".format(
            DEFAULT_WORKSPACE),
    )
    parser.add_argument(
        '-s', '--storage',
        type=str,
        help="The project's filestorage directory. Defaults to directory '{}' within the project's root directory.".format(
            DEFAULT_STORAGE),
    )
    parser.add_argument(
        '--db-host',
        type=str,
        help="The MongoDB database host address.",
    )
    parser.add_argument(
        '-t', '--template',
        type=str,
        default='minimal',
        choices=get_templates(),
        help="Choose the template for this project.",
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help="Ignore warnings that prevent project creation. This might lead to potential data loss!")
    parser.add_argument(
        '--signac-version',
        type=str,
        default=DEFAULT_VERSION,
        help="The signac version, to use for this new project. Defaults to '{}'.".format(DEFAULT_VERSION))


def main(arguments=None):
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(
        description="Make a new mock signac project.",
    )
    setup_parser(parser)
    args = parser.parse_args(arguments)
    init_project(args)

if __name__ == '__main__':
    main()
