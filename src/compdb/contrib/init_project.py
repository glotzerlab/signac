#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger('make_project')

DEFAULT_WORKSPACE = 'workspace'
DEFAULT_STORAGE = 'storage'
SCRIPT_HEADER = "#/usr/bin/env python\n# -*- coding: utf-8 -*-\n"

MSG_SUCCESS = """Successfully created project '{project_name}' in directory '{project_dir}'. Now try to execute `python run.py` to test your project configuration."""
MSG_AUTHOR_MISSING = "Did not find any author information. This will lead to problems during project execution. Execute `compdb config` to create missing author information."
MSG_NO_DB_ACCESS = "Unable to connect to database host '{}'. This does not prevent the project initialization. However the database must be accessable during job execution or analysis."
PROJECT_CONFIG_KEYS = [
    'project', '_project_dir', 'working_dir',
    'filestorage_dir', 'database_host']

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
    except (FileNotFoundError, KeyError):
        pass
    else:
        if realpath(root_dir) == realpath(args.directory):
            msg = "Project in directory '{}' already exists. Use '-f' or '--force' argument to ignore this warning and create a project anyways. This will lead to potential data loss!"
            raise RuntimeError(msg.format(realpath(args.directory)))

def check_for_author():
    from compdb.contrib import get_project
    project = get_project()
    p = False
    if project.config.get('author_name') is None:
        logger.warning("No author name defined.")
        p = True
    if project.config.get('author_email') is None:
        logger.warning("No author email address defined.")
        p = True
    if p:
        logger.warning(MSG_AUTHOR_MISSING)
    
def generate_config(args):
    from compdb.core.config import Config
    args = {
        'project':  args.project_name,
        'project_dir': args.directory,
        'working_dir': args.workspace,
        'filestorage_dir': args.storage,
        'database_host': args.db_host,
    }
    return Config(args)

def setup_default_dirs(args):
    import os
    if args.workspace is None:
        args.workspace = os.path.join(
            args.directory, DEFAULT_WORKSPACE)
    if args.storage is None:
        args.storage = os.path.join(
            args.directory, DEFAULT_STORAGE)

def mk_dirs(args):
    import os
    dirs =  [args.directory, args.workspace, args.storage]
    for path in dirs:
        try:
            os.mkdir(path)
        except FileExistsError:
            pass

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
    logging.basicConfig(level = logging.INFO)
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
        '-n', '--name',
        type = str,
        help = "The users' name.",
        )
    parser.add_argument(
        '--email',
        type = str,
        help = "The users' email address.",
        )
    parser.add_argument(
        '--db-host',
        type = str,
        default = 'localhost',
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
        if not args.force:
            check_for_existing_project(args)
        check_for_database(args)
        setup_default_dirs(args)
        config = generate_config(args)
        mk_dirs(args)
        copy_templates(args)
    except Exception as error:
        raise
    else:
        import os
        config.write(
            os.path.join(args.directory, 'compdb.rc'), 
            keys = PROJECT_CONFIG_KEYS)
        logger.info(MSG_SUCCESS.format(
            project_name = args.project_name,
            project_dir = os.path.realpath(args.directory)))
        config.load()
        check_for_author()

if __name__ == '__main__':
    main()
