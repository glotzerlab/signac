# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Parse the filter arguments."""

import json
import sys
from collections.abc import Mapping


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
    print(f"Interpreted filter arguments as '{json.dumps(query)}'.", file=file)
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


def _is_json_like(q):
    """Check if q is JSON like.

    Parameters
    ----------
    q : str
        Query string.

    Returns
    -------
    bool
        True if q starts with "{" and ends with "}".

    """
    return (q[0] == "{" and q[-1] == "}") or (q[0] == "[" and q[-1] == "]")


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
            print(f"Did you mean {CAST_MAPPING_WARNING[x]}?", file=sys.stderr)
        return CAST_MAPPING[x]
    except KeyError:
        try:
            return int(x)
        except ValueError:
            try:
                return float(x)
            except ValueError:
                return x


def _parse_single(key, value=None):
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
    if _is_json_like(key):
        raise ValueError(
            "Please check your filter arguments. "
            f"Using a JSON expression as a key is not allowed: '{key}'."
        )
    elif value is None or value == "!":
        return key, {"$exists": True}
    elif _is_json_like(value):
        return key, _parse_json(value)
    elif _is_regex(value):
        return key, {"$regex": value[1:-1]}
    else:
        return key, _cast(value)


def parse_simple(tokens):
    """Parse a set of string tokens into a suitable filter.

    Parameters
    ----------
    tokens : Sequence[str]
        A Sequence of strings composing key-value pairs.

    Yields
    ------
    tuple
        A single key-value pair of input tokenized filter.

    """
    for i in range(0, len(tokens), 2):
        key = tokens[i]
        if i + 1 < len(tokens):
            value = tokens[i + 1]
        else:
            value = None
        yield _parse_single(key, value)


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
        if _is_json_like(args[0]):
            return _parse_json(args[0])
        else:
            key, value = _parse_single(args[0])
            return _with_message({key: value}, file)
    else:
        q = dict(parse_simple(args))
        return _with_message(q, file)


def _add_prefix(prefix, filter):
    """Add desired prefix (e.g. 'sp.' or 'doc.') to a (possibly nested) filter."""
    if filter:
        for key, value in filter.items():
            if key in ("$and", "$or"):
                if isinstance(value, list) or isinstance(value, tuple):
                    yield key, [dict(_add_prefix(prefix, item)) for item in value]
                else:
                    raise ValueError(
                        "The argument to a logical operator must be a list or a tuple!"
                    )
            elif "." in key and key.split(".", 1)[0] in ("sp", "doc"):
                yield key, value
            elif key in ("sp", "doc"):
                yield key, value
            else:
                yield prefix + key, value


def _root_keys(filter):
    for key, value in filter.items():
        if key in ("$and", "$or"):
            assert isinstance(value, (list, tuple))
            for item in value:
                for key in _root_keys(item):
                    yield key
        elif "." in key:
            yield key.split(".", 1)[0]
        else:
            yield key


def parse_filter(filter):
    """Parse a provided sequence of filters.

    Parameters
    ----------
    filter : Sequence, Mapping, or str
        A set of key, value tuples corresponding to a single filter. This
        filter may itself be a compound filter containing and/or statements. The
        filter may be provided as a sequence of tuples, a mapping-like object,
        or a string. In the last case, the string will be parsed to generate a
        valid set of filters.

    Yields
    ------
    tuple
        A key value pair to be used as a filter.

    """
    if isinstance(filter, str):
        yield from parse_simple(filter.split())
    elif isinstance(filter, Mapping):
        yield from filter.items()
    else:
        try:
            yield from filter
        except TypeError:
            # This type was not iterable.
            raise ValueError(
                f"Invalid filter type {type(filter)}. The filter must "
                "be a Sequence, Mapping, or str."
            )
