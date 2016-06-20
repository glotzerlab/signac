# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging
import sys
import os
import getpass
import argparse

logger = logging.getLogger(__name__)


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning
    an answer is required of the user).

    The "answer" return value is one of "yes" or "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        # Compatible with python 2.7 and 3.x
        choice = raw_input().lower() if sys.hexversion < 0x03000000 else input().lower()  # NOQA
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def prompt_password(prompt='Password: '):
    return getpass.getpass(prompt)


def prompt_new_password(prompt='Password: '):
    pwd = getpass.getpass(prompt)
    pwd2 = getpass.getpass("Confirm password: ")
    if pwd != pwd2:
        raise ValueError("Passwords do not match.")
    return pwd


def add_verbosity_argument(parser, default=0):
    """Add a verbosity argument to parser.

    Note:
      The argument is '-v' or '--verbosity'.
      Add multiple '-v' arguments, e.g. '-vv' or '-vvv' to
      increase the level of verbosity.

    Args:
      parser: A argparse object.
      default: The default level, defaults to 0.
    """
    parser.add_argument(
        '-v', '--verbosity',
        help="Set level of verbosity.",
        action='count',
        default=default,
    )


def add_verbosity_action_argument(parser, default=0):
    """Add a verbosity argument to parser.

    Note:
      The argument is '-v'.
      Add multiple '-v' arguments, e.g. '-vv' or '-vvv' to
      increase the level of verbosity.

    Args:
      parser: A argparse object.
      default: The default level, defaults to 0.
    """
    parser.add_argument(
        '-v',
        default=0,
        nargs='?',
        action=VerbosityLoggingConfigAction,
        dest='verbosity',
    )


def set_verbosity_level(verbosity, default=None, increment=10):
    """Set the verbosity level as a function of an integer level.

    Args:
      verbosity: The verbosity level as integer.
      default: The default verbosity level, defaults to logging.ERROR.
    """
    if default is None:
        default = logging.ERROR
    logging.basicConfig(
        level=default - increment * verbosity)


class VerbosityAction(argparse.Action):

    def __call__(self, parser, args, values, option_string=None):
        if values is None:
            values = '1'
        try:
            values = int(values)
        except ValueError:
            values = values.count('v') + 1
        setattr(args, self.dest, values)


class VerbosityLoggingConfigAction(VerbosityAction):

    def __call__(self, parser, args, values, option_string=None):
        super(VerbosityLoggingConfigAction, self).__call__(
            parser, args, values, option_string)
        v_level = getattr(args, self.dest)
        set_verbosity_level(v_level)


class EmptyIsTrue(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        if values is None:
            values = True
        setattr(namespace, self.dest, values)


class SmartFormatter(argparse.HelpFormatter):

    def _split_lines(self, text, width):
        if text.startswith('R|'):
            return text[2:].splitlines()
        return argparse.HelpFormatter._split_lines(self, text, width)


def walkdepth(path, depth=0):
    if depth == 0:
        for p in os.walk(path):
            yield p
    elif depth > 0:
        path = path.rstrip(os.path.sep)
        if not os.path.isdir(path):
            raise OSError("Not a directory: '{}'.".format(path))
        num_sep = path.count(os.path.sep)
        for root, dirs, files in os.walk(path):
            yield root, dirs, files
            num_sep_this = root.count(os.path.sep)
            if num_sep + depth <= num_sep_this:
                del dirs[:]
    else:
        raise ValueError("The value of depth must be non-negative.")
