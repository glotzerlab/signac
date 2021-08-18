# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Utilities for signac."""

import logging
import os
import sys
from collections.abc import Mapping
from datetime import timedelta
from time import time

logger = logging.getLogger(__name__)


def _query_yes_no(question, default="yes"):  # pragma: no cover
    """Ask a yes/no question via input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning
    an answer is required of the user).

    The "answer" return value is one of "yes" or "no".

    Parameters
    ----------
    question : str
        Question presented to the user.
    default : str
        Presumed answer if the user just hits <Enter> (Default value = "yes").

    Returns
    -------
    bool
        ``True`` if yes, ``False`` if no.

    Raises
    ------
    ValueError
        When default is set to invalid answer.

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
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def _add_verbosity_argument(parser, default=0):
    """Add a verbosity argument to parser.

    Parameters
    ----------
    parser : :class:`argparse.ArgumentParser`
        The parser to which to add a verbosity argument.
    default : int
        The default level, defaults to 0.

    Notes
    -----
    The argument is '-v' or '--verbosity'.
    Add multiple '-v' arguments, e.g. '-vv' or '-vvv' to
    increase the level of verbosity.

    """
    parser.add_argument(
        "-v",
        "--verbosity",
        help="Set level of verbosity.",
        action="count",
        default=default,
    )


def _mkdir_p(path):
    """Make a new directory, or do nothing if the directory already exists.

    Parameters
    ----------
    path : str
        New directory name.

    """
    # Performance: `isdir` is fast and eliminates the rest of os.makedirs
    # if the path already exists. Typically this function is called in cases
    # where the path already exists. If the path is a file, this check returns
    # False and allows os.makedirs to raise FileExistsError as usual.
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def _split_and_print_progress(iterable, num_chunks=10, write=None, desc="Progress: "):
    """Split the progress and prints it.

    Parameters
    ----------
    iterable : list
        List of values to be chunked.
    num_chunks : int
        Number of chunks to split the given iterable (Default value = 10).
    write :
        Logging level used to log messages (Default value = None).
    desc : str
        Prefix of message to log (Default value = 'Progress: ').

    Yields
    ------
    iterable

    Raises
    ------
    AssertionError
        If num_chunks <= 0.

    """
    if write is None:
        write = print
    assert num_chunks > 0
    if num_chunks > 1:
        N = len(iterable)
        len_chunk = int(N / num_chunks)
        intervals = []
        show_est = False
        for i in range(num_chunks - 1):
            if i:
                msg = f"{desc}{100 * i / num_chunks:3.0f}%"
                if intervals:
                    mean_interval = sum(intervals) / len(intervals)
                    est_remaining = int(mean_interval * (num_chunks - i))
                    if est_remaining > 10 or show_est:
                        show_est = True
                        msg += f" (ETR: {timedelta(seconds=est_remaining)}h)"
                write(msg)
            start = time()
            yield iterable[i * len_chunk : (i + 1) * len_chunk]
            intervals.append(time() - start)
        yield iterable[(i + 1) * len_chunk :]
        write(f"{desc}100%")
    else:
        yield iterable


def _dotted_dict_to_nested_dicts(dotted_dict, delimiter_nested="."):
    """Convert dotted keys in the state point dict to a nested dict.

    Parameters
    ----------
    dotted_dict : dict
        A mapping with dots/delimiter_nested in its keys, e.g. {'a.b': 'c'}.
    delimiter_nested : str
        A string delimiter between keys, defaults to '.'.

    Returns
    -------
    dict
        A mapping instance with nested dicts, e.g. {'a': {'b': 'c'}}.

    """
    nested_dict = {}
    for key, value in dotted_dict.items():
        tokens = key.split(delimiter_nested)
        if len(tokens) > 1:
            tmp = nested_dict.setdefault(tokens[0], {})
            for token in tokens[1:-1]:
                tmp = tmp.setdefault(token, {})
            tmp[tokens[-1]] = value
        else:
            nested_dict[tokens[0]] = value
    return nested_dict


class _hashable_dict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


def _to_hashable(obj):
    """Create a hash of passed type.

    Parameters
    ----------
    obj
        Object to create a hashable version of. Lists are converted
        to tuples, and hashes are defined for dicts.

    Returns
    -------
    Hash created for obj.

    """
    if type(obj) is list:
        return tuple(_to_hashable(_) for _ in obj)
    elif type(obj) is dict:
        return _hashable_dict(obj)
    else:
        return obj


def _encode_tree(x):
    """Encode if type of x is list.

    Parameters
    ----------
    x :
        type to encode.

    Returns
    -------
    Hashable version of ``x``.

    """
    if type(x) is list:
        return _to_hashable(x)
    else:
        return x


def _nested_dicts_to_dotted_keys(t, encode=_encode_tree, key=None):
    """Generate tuples of key in dotted string format and value from nested dict.

    Parameters
    ----------
    t : dict
        A mapping instance with nested dicts, e.g. {'a': {'b': 'c'}}.
    encode :
        By default, values are encoded to be hashable. Use ``None`` to skip encoding.
    key : str
        Key of root at current point in the recursion, used to
        build up nested keys in the top-level dict through
        multiple recursive calls (Default value = None).

    Yields
    ------
    tuple
        Tuples of dotted key and values e.g. ('a.b', 'c')

    """
    if encode is not None:
        t = encode(t)
    if isinstance(t, Mapping):
        if t:
            for k in t:
                k_ = k if key is None else ".".join((key, k))
                yield from _nested_dicts_to_dotted_keys(t[k], encode=encode, key=k_)
        elif key is not None:
            yield key, t
    else:
        yield key, t
