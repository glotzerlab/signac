# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Parse the filter arguments."""

import sys

from ..core import json


def _print_err(msg=None):
    """Print the provided message to stderr.

    Parameters
    ----------
    msg : str
        Error message to be printed (Default value = None).

    """
    print(msg, file=sys.stderr)


def _with_message(query, file):
    """Print the interpreted filter arguments to the provided file.

    Parameters
    ----------
    query : dict
        Filter arguments.
    file :
        The file where the filter interpretation is printed.

    Returns
    -------
    query : dict
        Filter arguments.

    """
    print("Interpreted filter arguments as '{}'.".format(json.dumps(query)), file=file)
    return query


def _read_index(project, fn_index=None):
    """Read index from the file passed.

    Parameters
    ----------
    project : :class:`~signac.Project`
        Project handle.
    fn_index : str
        File name of the index (Default value = None).

    Returns
    -------
    generator
        Returns the file contents, parsed as JSON-encoded lines.

    """
    if fn_index is not None:
        _print_err(f"Reading index from file '{fn_index}'...")
        fd = open(fn_index)
        return (json.loads(line) for line in fd)


def _is_json(q):
    """Check if q is JSON.

    Parameters
    ----------
    q : str
        Query string.

    Returns
    -------
    bool
        True if q starts with "{" and ends with "}".

    """
    return q.strip().startswith("{") and q.strip().endswith("}")


def _is_regex(q):
    """Check if q is a regular expression.

    Parameters
    ----------
    q : str
        Query string.

    Returns
    -------
    bool
        True if q starts with "/" and ends with "/".

    """
    return q.startswith("/") and q.endswith("/")


def _parse_json(q):
    """Parse a query argument as JSON.

    Parameters
    ----------
    q : json
        Query argument.

    Raises
    ------
    JSONDecodeError
        Raised if the input cannot be parsed as JSON.

    """
    try:
        return json.loads(q)
    except json.JSONDecodeError:
        _print_err(f"Failed to parse query argument. Ensure that '{q}' is valid JSON!")
        raise


CAST_MAPPING = {
    "true": True,
    "false": False,
    "null": None,
}

CAST_MAPPING_WARNING = {
    "True": "true",
    "False": "false",
    "None": "null",
    "none": "null",
}


def _cast(x):
    """Attempt to interpret x with the correct type.

    Parameters
    ----------
    x : str
        The value to cast.

    Returns
    -------
    object
        Value of x, cast from a str to an appropriate type (bool, NoneType, int, float, str).

    """
    try:
        if x in CAST_MAPPING_WARNING:
            print("Did you mean {}?".format(CAST_MAPPING_WARNING[x]), file=sys.stderr)
        return CAST_MAPPING[x]
    except KeyError:
        try:
            return int(x)
        except ValueError:
            try:
                return float(x)
            except ValueError:
                return x


def _parse_simple(key, value=None):
    """Parse simple search syntax.

    Parameters
    ----------
    key : str
        The filter key.
    value :
        The filter value. If None, the filter returns
        True if the provided key exists (Default value = None).

    Returns
    -------
    dict
        Parsed filter arguments.

    Raises
    ------
    ValueError
        If filter arguments have an invalid key.

    """
    if value is None or value == "!":
        return {key: {"$exists": True}}
    elif _is_json(value):
        return {key: _parse_json(value)}
    elif _is_regex(value):
        return {key: {"$regex": value[1:-1]}}
    elif _is_json(key):
        raise ValueError(
            "Please check your filter arguments. "
            "Using a JSON expression as a key is not allowed: '{}'.".format(key)
        )
    else:
        return {key: _cast(value)}


def parse_filter_arg(args, file=sys.stderr):
    """Parse a series of filter arguments into a dictionary.

    Parameters
    ----------
    args : sequence of str
        Filter arguments to parse.
    file :
        The file to write message (Default value = sys.stderr).

    Returns
    -------
    dict
        Filter arguments.

    """
    if args is None or len(args) == 0:
        return None
    elif len(args) == 1:
        if _is_json(args[0]):
            return _parse_json(args[0])
        else:
            return _with_message(_parse_simple(args[0]), file)
    else:
        q = {}
        for i in range(0, len(args), 2):
            key = args[i]
            if i + 1 < len(args):
                value = args[i + 1]
            else:
                value = None
            q.update(_parse_simple(key, value))
        return _with_message(q, file)
