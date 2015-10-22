#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import textwrap
import re
import argparse
from os.path import abspath, expanduser

from ..common import config
from . utility import prompt_password

logger = logging.getLogger(__name__)

RE_EMAIL = r"[^@]+@[^@]+\.[^@]+"

OPERATIONS = ['add', 'set', 'remove', 'show']
USER_GLOBAL = expanduser('~/.signacrc')
USER_LOCAL = expanduser('./signac.rc')


def process(args):
    if args.name:
        if args.name.endswith('password'):
            if not args.value:
                args.value = prompt_password()


def get_config(args, for_writing=False):
    try:
        if args._global:
            config_ = config.read_config_file(USER_GLOBAL)
        elif args.config:
            config_ = config.read_config_file(args.config)
        elif for_writing:
            config_ = config.read_config_file(USER_LOCAL)
        else:
            config_ = config.load_config()
    except FileNotFoundError:
        pass
    return config_


def write_config(config_, args):
    if args._global:
        config.write_config(config_, USER_GLOBAL)
    elif args.config == '-':
        for line in config_.write():
            print(line)
    elif args.config:
        config.write_config(config_, args.config)
    else:
        config.write_config(config_, USER_LOCAL)


def add(args):
    config_ = get_config(args, for_writing=True)
    if args.name in config_:
        msg = "Value for '{}' is already set. "\
              "Use 'set' instead of 'add' to overwrite."
        raise RuntimeError(msg.format(args.name))
    else:
        set_value(args)


def check(key, value):
    if key.endswith('email'):
        if not re.match(RE_EMAIL, value.strip()):
            msg = "Invalid email address: '{}'."
            raise ValueError(msg.format(value))


def set_value(args):
    config_ = get_config(args, for_writing=True)
    try:
        config_[args.name] = args.value
    except config.IllegalKeyError as error:
        msg = "'{}' does not seem to be a valid configuration key. "\
              "Use '-f' or '--force' to ignore this warning."
        raise ValueError(msg.format(args.name))
    except config.IllegalArgumentError as error:
        msg = "Value '{value}' for '{key}' is illegal. "\
              "Possible values: '{choices}'."
        key, value, choices = error.args
        raise ValueError(msg.format(
            key=args.name, value=args.value, choices=choices))
    write_config(config_, args)


def remove(args):
    config_ = get_config(args, for_writing=True)
    del config_[args.name]
    write_config(config_, args)


def show(args):
    config_ = get_config(args)
    for line in config_.write():
        print(line)


def verify(args):
    args.name = args.name.strip()
    args.email = args.email.strip()
    if not re.match(RE_EMAIL, args.email):
        msg = "Invalid email address: '{}'."
        raise ValueError(msg.format(args.email))
    if args.config != '-':
        args.config = abspath(expanduser(args.config))


def configure(args):
    process(args)
    if args.operation == 'add':
        add(args)
    elif args.operation == 'set':
        set_value(args)
    elif args.operation == 'remove':
        remove(args)
    elif args.operation == 'show':
        show(args)
    else:
        raise ValueError("Unknown operation: {}".format(args.operation))

HELP_OPERATION = """\
R|Configure signac for your local environment.
You can perform one of the following operations:

    set:    Set value of 'name' to 'value'.

    add:    Like 'set', but will not overwrite
            any existing values.

    remove: Remove configuration value 'name'.

    dump:   Dump the selected configuration.

    show:   Show the complete configuration
            including default values.

    """


def setup_parser(parser):
    parser.add_argument(
        'operation',
        type=str,
        choices=OPERATIONS,
        help=textwrap.dedent(HELP_OPERATION))
    parser.add_argument(
        'name',
        type=str,
        nargs='?',
        help="variable name")
    parser.add_argument(
        'value',
        type=str,
        nargs='?',
        default='',
        help="variable value")
    parser.add_argument(
        '-c', '--config',
        type=str,
        help="The config file to read and write from. Use '-'"
             "to print to standard output.")
    parser.add_argument(
        '-g', '--global',
        dest='_global',
        action='store_true',
        help="Write to the user's global configuration file.")
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help="Ignore all warnings.")


def main(arguments=None):
    parser = argparse.ArgumentParser(
        description="Change the compDB configuration.",
    )
    setup_parser(parser)
    args = parser.parse_args(arguments)
    return configure(args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
