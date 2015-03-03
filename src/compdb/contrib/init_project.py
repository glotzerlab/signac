#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger('make_project')

DEFAULT_WORKSPACE = 'workspace'
DEFAULT_STORAGE = 'storage'
SCRIPT_HEADER = "#/usr/bin/env python\n# -*- coding: utf-8 -*-\n"

MSG_SUCCESS = """Successfully created project '{project_name}' in directory '{project_dir}'. Now try to execute `python run.py` to test your project configuration."""
MSG_AUTHOR_INCOMPLETE = "Author information is incomplete. This will lead to problems during project execution. Execute `compdb config` to create missing author information."
MSG_NO_DB_ACCESS = "Unable to connect to database host '{}'. This does not prevent the project initialization. However the database must be accessable during job execution or analysis."
MSG_ENV_INCOMPLETE = "The following configuration variables are not set: '{}'. You can use these commands to set them:"
#PROJECT_CONFIG_KEYS = [
#    'project', '_project_dir', 'working_dir',
#    'filestorage_dir', 'database_host']

def check_for_database(args):
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    try:
        client = MongoClient(args.db_host)
    except ConnectionFailure:
        logger.warning(MSG_NO_DB_ACCESS.format(args.db_host))

def check_for_existing_project(args):
    from os.path import realpath
    from compdb.contrib import get_project
    try:
        project = get_project()
        root_dir = project.root_directory()
    except (FileNotFoundError, NotADirectoryError, KeyError):
        pass
    else:
        if realpath(root_dir) == realpath(args.directory):
            msg = "Project in directory '{}' already exists. Use '-f' or '--force' argument to ignore this warning and create a project anyways. This will lead to potential data loss!"
            print(msg.format(realpath(args.directory)))
            return True
    return False

def check_environment():
    from compdb.contrib import get_project
    project = get_project()

    keys = ['author_name', 'author_email', 'working_dir', 'filestorage_dir']
    missing = []
    for key in keys:
        if project.config.get(key) is None:
            missing.append(key)
    if len(missing):
        print(MSG_ENV_INCOMPLETE.format(missing))
        for key in missing:
            print("compdb config add {key} [your_value]".format(key = key))

def adjust_args(args):
    from os.path import abspath
    if args.workspace:
        args['working_dir'] = abspath(args.workspace)
    if args.storage:
        args['filestorage_dir'] = abspath(args.storage)
    
def generate_config(args):
    from compdb.core.config import Config
    c_args = {
         'project':  args.project_name,
    }
    if args.workspace:
        c_args['working_dir'] = args.workspace
    if args.storage:
        c_args['filestorage_dir'] = args.storage
    if args.db_host:
         c_args['database_host'] = args.db_host
    config = Config()
    config = Config(c_args)
    config.verify()
    return config

def get_templates():
    from compdb.contrib.templates import TEMPLATES
    return TEMPLATES.keys()

def copy_templates(args):
    import os
    from compdb.contrib.templates import TEMPLATES
    template = TEMPLATES[args.template]
    for filename, content in template.items():
        fn = os.path.join(args.directory, filename)
        if os.path.isfile(fn):
            msg = "Skipping template file '{}' because a file with the same name already exists."
            logger.warning(msg.format(fn))
            #raise FileExistsError(msg.format(fn))
        else:
            with open(fn, 'wb') as file:
                c = SCRIPT_HEADER + content
                file.write(c.encode('utf-8'))

def main(arguments = None):
    logging.basicConfig(level = logging.DEBUG)
    from argparse import ArgumentParser
    parser = ArgumentParser(
        description = "Make a new mock compdb project.",
        )
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

    args = parser.parse_args(arguments)
    try:
        adjust_args(args)
        if not args.force:
            if check_for_existing_project(args):
                return
        check_for_database(args)
        config = generate_config(args)
        copy_templates(args)
    except Exception as error:
        raise
    else:
        import os
        config.write(os.path.join(args.directory, 'compdb.rc'))
            #keys = PROJECT_CONFIG_KEYS)
        print(MSG_SUCCESS.format(
            project_name = args.project_name,
            project_dir = os.path.realpath(args.directory)))
        config.load()
        check_environment()

if __name__ == '__main__':
    main()
