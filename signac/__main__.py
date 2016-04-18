from __future__ import print_function
import os
import sys
import argparse
import json

from . import get_project
from . import __version__


def main_project(args):
    project = get_project()
    if args.workspace:
        print(project.workspace())
    else:
        print(project)


def main_job(args):
    project = get_project()
    if args.statepoint is '-':
        sp = input()
    else:
        sp = args.statepoint
    try:
        statepoint = json.loads(sp)
    except ValueError:
        print(
            "Error while reading statepoint: '{}'".format(sp),
            file=sys.stderr)
        raise
    job = project.open_job(statepoint)
    if args.create:
        job.document
    if args.workspace:
        print(job.workspace())
    else:
        print(job)


def main_init(args):
    try:
        get_project()
    except LookupError:
        with open("signac.rc", 'a') as file:
            file.write('project={}\n'.format(args.project_id))
        assert str(get_project()) == args.project_id
        print("Initialized project '{}'.".format(args.project_id))
    else:
        raise RuntimeError(
            "Failed to initialize project '{}', '{}' is already a "
            "project root path.".format(args.project_id, os.getcwd()))


def main():
    parser = argparse.ArgumentParser(
        description="signac aids in the management, access and analysis of "
                    "large-scale computational investigations.")
    parser.add_argument(
        '--debug',
        action='store_true',
        help="Show traceback on error for debugging.")
    parser.add_argument(
        '--version',
        action='store_true',
        help="Display the version number and exit.")
    subparsers = parser.add_subparsers()

    parser_project = subparsers.add_parser('project')
    parser_project.add_argument(
        '-w', '--workspace',
        action='store_true',
        help="Print the project's workspace path instead of the project id.")
    parser_project.set_defaults(func=main_project)

    parser_job = subparsers.add_parser('job')
    parser_job.add_argument(
        'statepoint',
        nargs='?',
        default='-',
        type=str,
        help="The job's statepoint in JSON format. "
             "Omit this argument to read from STDIN.")
    parser_job.add_argument(
        '-w', '--workspace',
        action='store_true',
        help="print the job's workspace path instead of the job id.")
    parser_job.add_argument(
        '-c', '--create',
        action='store_true',
        help="Create the job's workspace directory if necessary.")
    parser_job.set_defaults(func=main_job)

    parser_init = subparsers.add_parser('init')
    parser_init.add_argument(
        'project_id',
        type=str,
        help="Initialize a project with the given project id.")
    parser_init.set_defaults(func=main_init)

    # This is a hack, as argparse itself does not
    # allow to parse only --version without any
    # of the other required arguments.
    if '--version' in sys.argv:
        print('signac', __version__)
        sys.exit(0)

    args = parser.parse_args()
    try:
        args.func(args)
    except AttributeError:
        parser.print_usage()
    except Exception as error:
        print('Error: {}'.format(str(error)), file=sys.stderr)
        if args.debug:
            raise
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
