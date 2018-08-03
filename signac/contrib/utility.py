# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import logging
import sys
import os
import getpass
import argparse
import errno
import zipfile
import tarfile
from time import time
from datetime import timedelta
from contextlib import contextmanager

from ..common import six
from ..common.tempdir import TemporaryDirectory

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


def _mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as error:
        if not (error.errno == errno.EEXIST and os.path.isdir(path)):
            raise


def is_string(s):
    if six.PY2:
        return isinstance(s, basestring)  # noqa
    else:
        return isinstance(s, str)


def split_and_print_progress(iterable, num_chunks=10, write=None, desc='Progress: '):
    if write is None:
        write = print
    assert num_chunks > 0
    if num_chunks > 1:
        N = len(iterable)
        len_chunk = int(N / num_chunks)
        intervals = []
        show_est = False
        for i in range(num_chunks-1):
            if i:
                msg = "{}{:3.0f}%".format(desc, 100 * i / num_chunks)
                if intervals:
                    mean_interval = sum(intervals) / len(intervals)
                    est_remaining = int(mean_interval * (num_chunks - i))
                    if est_remaining > 10 or show_est:
                        show_est = True
                        msg += " (ETR: {}h)".format(timedelta(seconds=est_remaining))
                write(msg)
            start = time()
            yield iterable[i * len_chunk:(i+1) * len_chunk]
            intervals.append(time() - start)
        yield iterable[(i+1) * len_chunk:]
        write("{}100%".format(desc))
    else:
        yield iterable


@contextmanager
def _extract(filename):
    with TemporaryDirectory() as tmpdir:
        if zipfile.is_zipfile(filename):
            with zipfile.ZipFile(filename) as file:
                file.extractall(tmpdir)
                yield tmpdir
        elif tarfile.is_tarfile(filename):
            with tarfile.open(filename) as file:
                file.extractall(path=tmpdir)
                yield tmpdir
        else:
            raise RuntimeError("Unknown file type: '{}'.".format(filename))
