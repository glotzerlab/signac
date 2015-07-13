#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger(__name__)

from os.path import expanduser
from . utility import prompt_new_password

RE_EMAIL = r"[^@]+@[^@]+\.[^@]+"

OPERATIONS= ['add', 'set', 'remove', 'dump', 'show']
USER_GLOBAL = expanduser('~/compdb.rc')
USER_LOCAL = expanduser('./compdb.rc')

def process(args):
    from compdb.core.config import DIRS, FILES
    from os.path import abspath, expanduser
    if args.name: 
        if args.name in DIRS or args.name in FILES:
            args.value = abspath(expanduser(args.value))
        if args.name.endswith('password'):
            if not args.value:
                args.value = prompt_new_password()

def get_config(args, for_writing = False):
    from compdb.core.config import Config, load_config
    config = Config()
    try:
        if args._global:
            config.read(USER_GLOBAL)
        elif args.config:
            config.read(args.config)
        elif for_writing:
            config.read(USER_LOCAL)
        else:
            config = load_config()
            #config.read(expanduser('./compdb.rc'))
    except FileNotFoundError:
        pass
    return config

def write_config(config, args):
    if args._global:
        config.write(USER_GLOBAL)
    elif args.config == '-':
        config.dump()
    elif args.config:
        config.write(args.config)
    else:
        config.write(USER_LOCAL)
        #msg = "You need to use option '--global' or '--config' to specify which config file to write to."
        #raise ValueError(msg)

def add(args):
    config = get_config(args, for_writing = True)
    if args.name in config:
        msg = "Value for '{}' is already set. Use 'set' instead of 'add' to overwrite."
        raise RuntimeError(msg.format(args.name))
    else:
        set_value(args)

def check(key, value):
    import re
    from compdb.core.config import is_legal_key, IllegalKeyError
    if not is_legal_key(key):
        raise IllegalKeyError(key)
    if key.endswith('email'):
        if not re.match(RE_EMAIL, value.strip()):
            msg = "Invalid email address: '{}'."
            raise ValueError(msg.format(value))

def set_value(args):
    from ..core.config import IllegalKeyError, IllegalArgumentError
    config = get_config(args, for_writing = True)
    try:
        if not args.force:
            check(args.name, args.value)
        config.__setitem__(args.name, args.value, args.force)
    except IllegalKeyError as error:
        msg = "'{}' does not seem to be a valid configuration key. Use '-f' or '--force' to ignore this warning."
        raise ValueError(msg.format(args.name))
    except IllegalArgumentError as error:
        msg = "Value '{value}' for '{key}' is illegal. Possible values: '{choices}'."
        key, value, choices = error.args
        raise ValueError(msg.format(key=args.name, value=args.value, choices=choices))
    write_config(config, args)

def remove(args):
    config = get_config(args, for_writing = True)
    del config[args.name]
    write_config(config, args)

def dump(args):
    config = get_config(args)
    config.dump(indent = 1)

def show(args):
    from ..core.config import LEGAL_ARGS, DEFAULTS
    config = get_config(args)
    legal_args = sorted(LEGAL_ARGS)
    l_column0 = max(len(arg) for arg in legal_args)
    print("Current configuration:")
    print()
    msg = "{arg:<" + str(l_column0) + "}: {value}"
    for arg in legal_args:
        line = msg.format(arg = arg, value = config.get(arg))
        print(line)

def verify(args):
    import re
    args.name = args.name.strip()
    args.email = args.email.strip()
    if not re.match(RE_EMAIL, args.email):
        msg = "Invalid email address: '{}'."
        raise ValueError(msg.format(args.email))
    if args.config != '-':
        from os.path import expanduser, realpath
        args.config = realpath(expanduser(args.config))

def configure(args):
    process(args)
    if args.operation == 'add':
        add(args)
    elif args.operation == 'set':
        set_value(args)
    elif args.operation == 'remove':
        remove(args)
    elif args.operation == 'dump':
        dump(args)
    elif args.operation == 'show':
        show(args)
    else:
        raise ValueError("Unknown operation: {}".format(args.operation))

HELP_OPERATION = """\
    R|Configure compdb for your local environment.
    You can perform one of the following operations:
        
        set:    Set value of 'name' to 'value'.

        add:    Like 'set', but will not overwrite
                any existing values.

        remove: Remove configuration value 'name'.

        dump:   Dump the selected configuration.

        show:   Show the complete configuration
                including default values.

    """
import textwrap

def setup_parser(parser):
        parser.add_argument(
            'operation',
            type = str,
            choices = OPERATIONS,
            help = textwrap.dedent(HELP_OPERATION))
        parser.add_argument(
            'name',
            type = str,
            nargs = '?',
            help = "variable name")
        parser.add_argument(
            'value',
            type = str,
            nargs = '?',
            default = '',
            help = "variable value")
        parser.add_argument(
            '-c', '--config',
            type = str,
            #default = expanduser('./compdb.rc'),
            help = "The config file to read and write from. Use '-' to print to standard output.")
        parser.add_argument(
            '-g', '--global',
            dest = '_global',
            action = 'store_true',
            help = "Write to the user's global configuration file.")
        parser.add_argument(
            '-f', '--force',
            action = 'store_true',
            help = "Ignore all warnings.")

def main(arguments = None):
        from argparse import ArgumentParser
        parser = ArgumentParser(
            description = "Change the compDB configuration.",
            )
        setup_parser(parser)
        args = parser.parse_args(arguments)
        return configure(args)

if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)
    main()
