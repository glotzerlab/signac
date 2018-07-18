# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
from collections import OrderedDict
from collections import defaultdict

from ..core.json import json
from .errors import SchemaPathMismatchError


_DOT_MAGIC_WORD = '__DOT__'

RE_TYPES = {
    'str': r'\w+',
    'int': r'[+-]?[0-9]+',
    'float': r'[+-]?([0-9]*[\.])?[0-9]+',
    'bool': r'\w+',
}


def _generate_data_paths(project, delimiter_nested='.', sep='/'):
    """Generate explicit data paths for a given project."""
    jsi = project.build_job_statepoint_index(exclude_const=True)
    sp_index = OrderedDict(jsi)
    tmp = defaultdict(list)
    for key, values in sp_index.items():
        for value, group in values.items():
            path = delimiter_nested.join((str(k) for k in key)) + sep + str(value)
            for job_id in group:
                tmp[job_id].append(path)
    links = dict()
    for job_id, p in tmp.items():
        path = os.path.join(* p)
        if path in links:
            raise RuntimeError("Duplicate path: '{}'!".format(path))
        links[path] = project.open_job(id=job_id).workspace()
    return links


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
        if types[key] not in ('int', 'float', 'str', 'bool'):
            raise ValueError("Invalid type '{}'.".format(types[key]))
        types[key] = eval(types[key])
    return schema_regex, types


def _convert_to_nested(sp):
    """Convert a flat state point dict a nested dict."""
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


def _find_data_dirs(root, schema_path):
    """Find data directories for a given root directory and schema path.

    A valid schema path would for example be 'foo/{foo:int}', which means that we
    assume that the part of the path after 'foo/' is to be parsed as the foo variable and
    to be typecast to int.

    If no type is provided, it is assumed to be str.
    """
    pattern, types = _convert_schema_path_to_regex(schema_path.replace('/', r'\/'))
    p = root + r'\/' + pattern
    for root, dirs, files in os.walk(root):
        m = re.match(p, root)
        if m:
            sp = m.groupdict()
            for key in types:
                if key in sp:
                    sp[key] = types[key](sp[key])
            yield root, _convert_to_nested(sp)
            del dirs[:]     # skip sub-directories


def _find_workspaces(root, fn_manifest):
    """Find all valid workspace directories within the given root path.

    Workspace directories are identified by the presence of a state point manifest file.
    """
    for root, dirs, files in os.walk(root):
        if fn_manifest in files:
            with open(os.path.join(root, fn_manifest), 'rb') as file:
                sp = json.loads(file.read().decode())
                yield root, sp
            del dirs[:]     # skip sub-directories


def _parse_metadata(project, paths):
    """Parse the metadata for the given paths and check their consistency."""
    for path, sp_from_path in paths:
        try:
            with open(os.path.join(path, project.Job.FN_MANIFEST), 'rb') as file:
                sp_from_manifest = json.loads(file.read().decode())
                if sp_from_path != sp_from_manifest:
                    raise SchemaPathMismatchError(path, sp_from_manifest, sp_from_path)
                    #raise RuntimeError("Directory '{}' already contains a state point "
                                       #"file, which does not match the given schema.".format(path))
        except FileNotFoundError:
            pass
        job = project.open_job(sp_from_path)
        dst = job.workspace()
        if os.path.realpath(dst) != os.path.realpath(path):
            if os.path.exists(dst):
                raise RuntimeError("Destination path '{}' already exists.".format(dst))
            yield job, path
