#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger('make_project')

DEFAULT_WORKSPACE = 'workspace'
DEFAULT_STORAGE = 'storage'
SCRIPT_HEADER = "#/usr/bin/env python\n# -*- coding: utf-8 -*-\n"

SUCCESS_MESSAGE = """Successfully created project '{project_name}' in directory '{project_dir}'. Now try to execute `python run.py` to test your project configuration."""
MESSAGE_AUTHOR_MISSING = "Did not find any author information. This will lead to problems during project execution. Execute `make_author` to create missing author information."

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
    logger.warning(MESSAGE_AUTHOR_MISSING)
    
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

def copy_templates(args):
    import os
    from compdb.contrib.templates import MINIMAL
    for filename, content in MINIMAL.items():
        fn = os.path.join(args.directory, filename)
        if os.path.isfile(fn):
            msg = "Error while creating template files. File '{}' already exists."
            logger.warning(msg.format(fn))
            #raise FileExistsError(msg.format(fn))
        else:
            with open(fn, 'wb') as file:
                c = SCRIPT_HEADER + content
                file.write(c.encode('utf-8'))

def main():
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

        args = parser.parse_args()


        try:
            setup_default_dirs(args)
            copy_templates(args)
            config = generate_config(args)
            mk_dirs(args)
        except Exception as error:
            raise
        else:
            import os
            config.write(os.path.join(args.directory, 'compdb.rc'))
            logger.info(SUCCESS_MESSAGE.format(
                project_name = args.project_name,
                project_dir = args.directory))
            check_for_author()

if __name__ == '__main__':
        logging.basicConfig(level = logging.INFO)
        main()
