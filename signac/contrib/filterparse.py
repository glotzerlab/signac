# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import sys
from ..core import json


def _print_err(msg=None):
    """Prints the error message.

    Parameters
    ----------
    msg : str
        Error message to be printed.
         (Default value = None)

    """
    print(msg, file=sys.stderr)

#function that is called no where but this file.
def _with_message(query, file):
    """Writes the message to the passed file.

    Parameters
    ----------
    query : dict
        Filter arguments.
    file :
        The file to write message for the query passed.

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
    project : class:`~.contrib.project.Project`
        Signac project handle

    fn_index : str
        File name having index.
         (Default value = None)

    Returns
    -------
    generator
        Returns the file content.

    """
    if fn_index is not None:
        _print_err("Reading index from file '{}'...".format(fn_index))
        fd = open(fn_index)
        return (json.loads(l) for l in fd)

def _is_json(q):
    """Check if q is json.

    Parameters
    ----------
    q : str

    Returns
    -------
    bool
        True if q starts with "{" and ends with "}"

    """
    return q.strip().startswith('{') and q.strip().endswith('}')

def _is_regex(q):
    """Check if q is regex.

    Parameters
    ----------
    q : str

    Returns
    -------
    bool
        True if q starts with "/" and ends with "/"

    """
    return q.startswith('/') and q.endswith('/')


def _parse_json(q):
    """Parse json q.

    Parameters
    ----------
    q : json
        Query arguement.

    Raises
    ------
    JSONDecodeError
        When fail to parse query arguement(q).

    """
    try:
        return json.loads(q)
    except json.JSONDecodeError:
        _print_err("Failed to parse query argument. "
                   "Ensure that '{}' is valid JSON!".format(q))
        raise


CAST_MAPPING = {
    'true': True,
    'false': False,
    'null': None,
}

CAST_MAPPING_WARNING = {
    'True': 'true',
    'False': 'false',
    'None': 'null',
    'none': 'null',
}


def _cast(x):
    """Attempt to interpret x with the correct type.

    Parameters
    ----------
    x :


    Returns
    -------

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
    """

    Parameters
    ----------
    key :

    value :
         (Default value = None)

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        filter arguments have an invalid key.

    """
    if value is None or value == '!':
        return {key: {'$exists': True}}
    elif _is_json(value):
        return {key: _parse_json(value)}
    elif _is_regex(value):
        return {key: {'$regex': value[1:-1]}}
    elif _is_json(key):
        raise ValueError(
            "Please check your filter arguments. "
            "Using as JSON expression as key is not allowed: '{}'.".format(key))
    else:
        return {key: _cast(value)}


def parse_filter_arg(args, file=sys.stderr):
    """

    Parameters
    ----------
    args :

    file :
        The file to write message.
         (Default value = sys.stderr)

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
        q = dict()
        for i in range(0, len(args), 2):
            key = args[i]
            if i+1 < len(args):
                value = args[i+1]
            else:
                value = None
            q.update(_parse_simple(key, value))
        return _with_message(q, file)
