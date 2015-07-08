#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger(__name__)

from .. import VERSION as DEFAULT_VERSION

DEFAULT_WORKSPACE = 'workspace'
DEFAULT_STORAGE = 'storage'
SCRIPT_HEADER = "#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n"

MSG_SUCCESS = """Successfully created project '{project_name}' in directory '{project_dir}'.
Execute `compdb check` to check your configuration."""
MSG_AUTHOR_INCOMPLETE = "Author information is incomplete. This will lead to problems during project execution. Execute `compdb config` to create missing author information."

def check_for_existing_project(args):
    from os.path import realpath
    from compdb.contrib import get_project
    try:
        project = get_project()
        root_dir = project.root_directory()
    except (LookupError, FileNotFoundError, NotADirectoryError, KeyError):
        pass
    else:
        if realpath(root_dir) == realpath(args.directory):
            msg = "Project in directory '{}' already exists. Use '-f' or '--force' argument to ignore this warning and create a project anyways. This will lead to potential data loss!"
            print(msg.format(realpath(args.directory)))
            return True
    return False

def adjust_args(args):
    from os.path import abspath
    if args.workspace:
        args['workspace_dir'] = abspath(args.workspace)
    if args.storage:
        args['filestorage_dir'] = abspath(args.storage)

def make_dir(dirname):
    import os
    try:
        os.makedirs(dirname)
    except OSError:
        pass
    
def generate_config(args):
    from compdb.core.config import Config, load_config
    import os
    global_config = load_config()
    if not args.workspace and global_config.get('workspace_dir') is None:
        args.workspace = DEFAULT_WORKSPACE
    if not args.storage and global_config.get('filestorage_dir') is None:
        args.storage = DEFAULT_STORAGE
    c_args = {
         'project':  args.project_name,
         'compdb_version': args.version,
    }
    if args.workspace:
        make_dir(args.workspace)
        c_args['workspace_dir'] = args.workspace
    if args.storage:
        make_dir(args.storage)
        c_args['filestorage_dir'] = args.storage
    if args.db_host:
         c_args['database_host'] = args.db_host
    config = Config()
    try:
        config.read(os.path.join(args.directory, 'compdb.rc'))
    except FileNotFoundError:
        pass
    config.update(c_args)
    config.verify()
    return config

def get_templates():
    from compdb.contrib.templates import TEMPLATES
    return TEMPLATES.keys()

def copy_templates(args):
    import os, stat, warnings
    from compdb.contrib.templates import TEMPLATES
    template = TEMPLATES[args.template]
    for filename, content in template.items():
        fn = os.path.join(args.directory, filename)
        if os.path.isfile(fn):
            msg = "Skipping template file '{}' because a file with the same name already exists."
            logger.warning(msg.format(fn))
            #warnings.warn(msg.format(fn), UserWarning)  # Too intimitading.
            #raise FileExistsError(msg.format(fn))
        else:
            with open(fn, 'wb') as file:
                c = SCRIPT_HEADER + content
                file.write(c.encode('utf-8'))
            os.chmod(fn, os.stat(fn).st_mode | stat.S_IEXEC)

def init_project(args):
    from . import check
    try:
        adjust_args(args)
        if not args.force:
            if check_for_existing_project(args):
                return
        config = generate_config(args)
        copy_templates(args)
    except Exception as error:
        raise
    else:
        import os
        config.write(os.path.join(args.directory, 'compdb.rc'))
        print(MSG_SUCCESS.format(
            project_name = args.project_name,
            project_dir = os.path.realpath(args.directory)))
        config.load()

def setup_parser(parser):
    parser.add_argument(
        'project_name',
        type = str,
        help = "The project unique identifier.",
        )
    parser.add_argument(
        '-d', '--directory',
        type = str,
        default = '.',
        help = "The project's root directory. Defaults to the current working directory.",
        )
    parser.add_argument(
        '-w', '--workspace',
        type = str,
        help = "The project's workspace directory. Defaults to directory '{}' within the project's root directory.".format(DEFAULT_WORKSPACE),
        )
    parser.add_argument(
        '-s', '--storage',
        type = str,
        help = "The project's filestorage directory. Defaults to directory '{}' within the project's root directory.".format(DEFAULT_STORAGE),
        )
    parser.add_argument(
        '--db-host',
        type = str,
        help = "The MongoDB database host address.",
        )
    parser.add_argument(
        '-t', '--template',
        type = str,
        default = 'minimal',
        choices = get_templates(),
        help = "Choose the template for this project.",
        )
    parser.add_argument(
        '-f', '--force',
        action = 'store_true',
        help = "Ignore warnings that prevent project creation. This might lead to potential data loss!")
    parser.add_argument(
        '--version',
        type = str,
        default = DEFAULT_VERSION,
        help = "The compdb version, to use for this new project. Defaults to '{}'.".format(DEFAULT_VERSION))

def main(arguments = None):
    logging.basicConfig(level = logging.DEBUG)
    from argparse import ArgumentParser
    parser = ArgumentParser(
        description = "Make a new mock compdb project.",
        )
    setup_parser(parser)
    args = parser.parse_args(arguments)
    init_project(args)

if __name__ == '__main__':
    main()
