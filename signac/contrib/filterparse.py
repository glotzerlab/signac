# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import sys
from ..core import json
from ..common import six


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
        return key, {'$exists': True}
    elif _is_json(value):
        return key, _parse_json(value)
    elif _is_regex(value):
        return key, {'$regex': value[1:-1]}
    elif _is_json(key):
        raise ValueError(
            "Please check your filter arguments. "
            "Using as JSON expression as key is not allowed: '{}'.".format(key))
    else:
        return key, _cast(value)


def parse_filter_arg(args, file=sys.stderr):
    if args is None or len(args) == 0:
        return None
    elif len(args) == 1:
        if _is_json(args[0]):
            return _parse_json(args[0])
        else:
            key, value = _parse_simple(args[0])
            return _with_message({key: value}, file)
    else:
        q = dict(parse_simple(args))

        return _with_message(q, file)


def parse_simple(tokens):
    for i in range(0, len(tokens), 2):
        key = tokens[i]
        if i+1 < len(tokens):
            value = tokens[i+1]
        else:
            value = None
        yield _parse_simple(key, value)


def _add_prefix(filter, prefix):
    for key, value in filter:
        if key in ('$and', '$or'):
            if isinstance(value, list) or isinstance(value, tuple):
                yield key, [dict(_add_prefix(item.items(), prefix)) for item in value]
            else:
                raise ValueError(
                    "The argument to a logical operator must be a sequence (e.g. a list)!")
        elif '.' in key and key.split('.', 1)[0] in ('sp', 'doc'):
            yield key, value
        elif key in ('sp', 'doc'):
            yield key, value
        else:
            yield prefix + '.' + key, value


def _root_keys(filter):
    for key, value in filter.items():
        if key in ('$and', '$or'):
            assert isinstance(value, (list, tuple))
            for item in value:
                for key in _root_keys(item):
                    yield key
        elif '.' in key:
            yield key.split('.', 1)[0]
        else:
            yield key


def _parse_filter(filter):
    if isinstance(filter, six.string_types):
        # yield from parse_simple(filter.split())  # TODO: After dropping Py27.
        for key, value in parse_simple(filter.split()):
            yield key, value
    elif filter:
        # yield from filter.items()   # TODO: After dropping Py27.
        for key, value in filter.items():
            yield key, value


def parse_filter(filter, prefix='sp'):
    # yield from _add_prefix(_parse_filter(filter), prefix)  # TODO: After dropping Py27.
    for key, value in _add_prefix(_parse_filter(filter), prefix):
        yield key, value
