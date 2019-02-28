# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import sys
from ..core import json


def _print_err(msg=None):
    print(msg, file=sys.stderr)


def _with_message(query, file):
    print("Interpreted filter arguments as '{}'.".format(json.dumps(query)), file=file)
    return query


def _read_index(project, fn_index=None):
    if fn_index is not None:
        _print_err("Reading index from file '{}'...".format(fn_index))
        fd = open(fn_index)
        return (json.loads(l) for l in fd)


def _is_json(q):
    return q.strip().startswith('{') and q.strip().endswith('}')


def _is_regex(q):
    return q.startswith('/') and q.endswith('/')


def _parse_json(q):
    try:
        return json.loads(q)
    except json.decoder.JSONDecodeError:
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
    "Attempt to interpret x with the correct type."
    try:
        if x in CAST_MAPPING_WARNING:
            print("Did you mean {}?".format(CAST_MAPPING_WARNING[x], file=sys.stderr))
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
