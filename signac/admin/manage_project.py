import logging
import sys
import argparse

from ..contrib import admin
from ..contrib.admin import setup_parser as setup_user_parser
from ..contrib.utility import add_verbosity_argument, set_verbosity_level
from ..core.dbclient_connector import SUPPORTED_AUTH_MECHANISMS

logger = logging.getLogger(__name__)

def setup_parser(parser):
    parser.add_argument(
        'project',
        type = str,
        help = "The project to administrate.")
    subparsers = parser.add_subparsers()

    parser_user = subparsers.add_parser('user')
    setup_user_parser(parser_user)

def main():
    parser = argparse.ArgumentParser(
        description = "Administrate signac projects.")
    parser.add_argument(
        '-y', '--yes',
        action = 'store_true',
        help = "Assume yes to all questions.",)
    setup_parser(parser)
    add_verbosity_argument(parser)
    args = parser.parse_args()
    set_verbosity_level(args.verbosity)
    try:
        if 'func' in args:
            args.func(args)
        else:
            parser.print_usage()
    except Exception as error:
        if args.verbosity > 0:
            raise
        else:
            print("Error: {}".format(error))
            sys.exit(1)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main()
