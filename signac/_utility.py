# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Utility functions."""

import os.path
import sys
from collections.abc import Mapping


def _print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def _safe_relpath(path):
    """Attempt to make a relative path, or return the original path.

    This is useful for logging and representing objects, where an absolute path
    may be very long.
    """
    try:
        return os.path.relpath(path)
    except ValueError:
        # Windows cannot find relative paths across drives, so show the
        # original path instead.
        return path


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
    default : str, optional
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
        choice = input(question + prompt).lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').")


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


def _nested_dicts_to_dotted_keys(d, key=None):
    """Generate tuples of key in dotted string format and value from nested dict.

    Parameters
    ----------
    d : dict
        A mapping instance with nested dicts, e.g. {'a': {'b': 'c'}}.
    key : str
        Key of root at current point in the recursion, used to
        build up nested keys in the top-level dict through
        multiple recursive calls (Default value = None).

    Yields
    ------
    tuple
        Tuples of dotted key and values e.g. ('a.b', 'c')

    """
    if isinstance(d, Mapping):
        if d:
            for k in d:
                k_ = k if key is None else ".".join((key, k))
                yield from _nested_dicts_to_dotted_keys(d[k], key=k_)
        elif key is not None:
            yield key, d
    else:
        if type(d) is list:
            d = _to_hashable(d)
        yield key, d
