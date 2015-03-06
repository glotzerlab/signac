#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger('make_author')

from os.path import expanduser

RE_EMAIL = r"[^@]+@[^@]+\.[^@]+"

OPERATIONS= ['add', 'set', 'remove', 'show']

def process(args):
    from os.path import abspath
    if args.name: 
        if args.name.endswith('_dir'):
            args.value = abspath(args.value)

def add(args):
    from compdb.core.config import Config
    config = Config()
    try:
        config.read(args.config)
    except FileNotFoundError:
        pass
    if args.name in config:
        msg = "Value for '{}' is already set in '{}'. Use 'set' instead of 'add' to overwrite."
        print(msg.format(args.name, args.config))
        return
    config[args.name] = args.value
    if args.config == '-':
        config.dump()
    else:
        config.write(args.config)

def set_value(args):
    from compdb.core.config import Config
    config = Config()
    try:
        config.read(args.config)
    except FileNotFoundError:
        pass
    config[args.name] = args.value
    if args.config == '-':
        config.dump()
    else:
        config.write(args.config)

def remove(args):
    from compdb.core.config import Config
    config = Config()
    try:
        config.read(args.config)
    except FileNotFoundError:
        pass
    del config[args.name]
    config.write(args.config)

def show(args):
    from compdb.core.config import Config
    config = Config()
    try:
        config.read(args.config)
    except FileNotFoundError:
        pass
    config.dump(indent = 1)


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

def make_author(args):
    from compdb.core.config import Config
    import os
    c = {
        'author_name': args.name,
        'author_email': args.email,
    }
    config = Config()
    if not args.config == '-':
        try:
            config.read(args.config)
        except FileNotFoundError:
            pass
    config.update(c)
    if args.config == '-':
        config.dump()
    else:
        config.write(args.config)

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
        print("Unknown operation: {}".format(args.operation))

def setup_parser(parser):
        parser.add_argument(
            'operation',
            type = str,
            help = "The config operation you would like to perform.",
            choices = OPERATIONS,
            )
        parser.add_argument(
            'name',
            type = str,
            nargs = '?',
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
            '-c', '--config',
            type = str,
            default = expanduser('~/compdb.rc'),
            help = "The config file to read and write from. Use '-' to print to standard output.",
            )

def main(arguments = None):
        from argparse import ArgumentParser
        parser = ArgumentParser(
            description = "Change the compDB configuration.",
            )

        args = parser.parse_args(arguments)
        configure(args)

if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)
    main()
