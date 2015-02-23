#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger('make_author')

RE_EMAIL = r"[^@]+@[^@]+\.[^@]+"

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

def main():
        from argparse import ArgumentParser
        parser = ArgumentParser(
            description = "Setup up author meta data for compdb projects.",
            )
        parser.add_argument(
            'name',
            type = str,
            help = "The users' name. Example: 'John Doe'",
            )
        parser.add_argument(
            'email',
            type = str,
            help = "The users' email address. Example: 'johndoe@example.com'."
            )
        parser.add_argument(
            '-o', '--output',
            type = str,
            default = '~/compdb.rc',
            help = "The config file to write configuration to. Use '-' to print to standard output.",
            )

        args = parser.parse_args()

        verify(args)
        make_author(args)

if __name__ == '__main__':
    main()
