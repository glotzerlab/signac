# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
import errno
from collections import OrderedDict
from collections import defaultdict

from ..core.json import json


_DOT_MAGIC_WORD = '__DOT__'


RE_TYPES = {
    'str': r'\w+',
    'int': r'[+-]?[0-9]+',
    'float': r'[+-]?([0-9]*[\.])?[0-9]+',
    'bool': r'\w+',
}


def _make_path_function(project, prefix='.', delimiter_nested='.'):
    """Generate explicit data paths for a given project."""
    jsi = project.build_job_statepoint_index(exclude_const=True)
    sp_index = OrderedDict(jsi)
    tmp = defaultdict(list)
    for key, values in sp_index.items():
        for value, group in values.items():
            path = delimiter_nested.join((str(k) for k in key)) + os.path.sep + str(value)
            for job_id in group:
                tmp[job_id].append(path)

    def get_path(job):
        return os.path.join(* tmp[job.get_id()])

    return get_path


def _convert_bool(value):
    "Convert a boolean value encoded as string to corresponding bool."
    return {
        'true': True,   '1': True,
        'false': False, '0': False,
    }.get(value.lower(), bool(value))


def _convert_schema_path_to_regex(schema_path):
    r"""Convert a schema path to a regular expression.

    For example, the following path 'data\/foo\/{foo:int}' would be converted to
    the following regular expression: 'data\/foo\/(?P<foo>\w+)'.

    When no type is specified, we default to str.
    """
    # The regular expression below is used to identify the {value:type} specifications
    # in the schema path.
    re_key_type_field = r'\{(?P<key>[\.\w]+)(?::(?P<type>[a-z]+))?\}'
    schema_regex = ''    # the return value
    types = dict()       # maps values to their designated types
    index = 0
    while True:
        m = re.search(re_key_type_field, schema_path[index:])
        if m:
            key = m.groupdict()['key'].replace('.', _DOT_MAGIC_WORD)
            types[key] = m.groupdict()['type'] or 'str'
            start, stop = m.span()
            schema_regex += schema_path[index:index+start].replace('.', r'\.')
            schema_regex += r'(?P<{}>{})'.format(key, RE_TYPES[types[key]])
            index += stop
            continue
        break
    schema_regex += '$'

    for key in types:
        if types[key] in ('int', 'float', 'str'):
            types[key] = eval(types[key])
        elif types[key] == 'bool':
            types[key] = _convert_bool
        else:
            raise ValueError("Invalid type '{}'.".format(types[key]))
    return schema_regex, types


def _make_schema_path_function(schema_path):
    schema_regex, types = _convert_schema_path_to_regex(schema_path)

    def parse_path(path):
        match = re.match(schema_regex, path)
        if match:
            sp = match.groupdict()
            for key in types:
                if key in sp:
                    sp[key] = types[key](sp[key])
            return _convert_to_nested(sp)

    return parse_path


def _convert_to_nested(sp):
    """Convert a flat state point dict to a nested dict."""
    ret = dict()
    for key, value in sp.items():
        tokens = key.split(_DOT_MAGIC_WORD)
        if len(tokens) > 1:
            tmp = ret.setdefault(tokens[0], dict())
            for token in tokens[1:-1]:
                tmp = tmp.setdefault(tokens, dict())
            tmp[tokens[-1]] = value
        else:
            ret[tokens[0]] = value
    return ret


def _parse_workspaces(fn_manifest):

    def _parse_workspace(path):
        try:
            with open(os.path.join(path, fn_manifest), 'rb') as file:
                return json.loads(file.read().decode())
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise error

    return _parse_workspace
