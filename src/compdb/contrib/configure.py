#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger('make_author')

from os.path import expanduser

RE_EMAIL = r"[^@]+@[^@]+\.[^@]+"

OPERATIONS= ['add', 'set', 'remove']

def filter(args):
    from os.path import abspath
    if args.name.endswith('_dir'):
        args.value = abspath(args.value)

def add(args):
    from compdb.core.config import Config
    config = Config()
    config.read(args.output)
    if args.name in config:
        msg = "Value for '{}' is already set in '{}'. Use 'set' instead of 'add' to overwrite."
        print(msg.format(args.name, args.output))
        return
    config[args.name] = args.value
    config.write(args.output)

def set_value(args):
    from compdb.core.config import Config
    config = Config()
    config.read(args.output)
    config[args.name] = args.value
    config.write(args.output)

def remove(args):
    from compdb.core.config import Config
    config = Config()
    config.read(args.output)
    del config[args.name]
    config.write(args.output)

def verify(args):
    import re
    args.name = args.name.strip()
    args.email = args.email.strip()
    if not re.match(RE_EMAIL, args.email):
        msg = "Invalid email address: '{}'."
        raise ValueError(msg.format(args.email))
    if args.output != '-':
        from os.path import expanduser, realpath
        args.output = realpath(expanduser(args.output))

def make_author(args):
    from compdb.core.config import Config
    import os
    c = {
        'author_name': args.name,
        'author_email': args.email,
    }
    config = Config()
    if not args.output == '-':
        try:
            config.read(args.output)
        except FileNotFoundError:
            pass
    config.update(c)
    if args.output == '-':
        config.dump()
    else:
        config.write(args.output)

def main(arguments = None):
        from argparse import ArgumentParser
        parser = ArgumentParser(
            description = "Change the compDB configuration.",
            )
        parser.add_argument(
            'operation',
            type = str,
            help = "The config operation you would like to perform.",
            choices = OPERATIONS,
            )
        parser.add_argument(
            'name',
            type = str,
            help = "variable name",
            )
        parser.add_argument(
            'value',
            type = str,
            nargs = '?',
            default = '',
            help = "variable value",
            )
        parser.add_argument(
            '-o', '--output',
            type = str,
            default = expanduser('~/compdb.rc'),
            help = "The config file to write configuration to. Use '-' to print to standard output.",
            )

        args = parser.parse_args(arguments)
        filter(args)
        if args.operation == 'add':
            add(args)
        elif args.operation == 'set':
            set_value(args)
        elif args.operation == 'remove':
            remove(args)
        else:
            print("Unknown operation: {}".format(args.operation))

if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)
    main()
