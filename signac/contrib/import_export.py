# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
import errno
import shutil
import tarfile
from zipfile import ZipFile, ZIP_DEFLATED
from collections import OrderedDict
from collections import defaultdict

from ..common import six
from ..core.json import json
from .errors import DestinationExistsError
from .utility import _mkdir_p, _extract

import logging


logger = logging.getLogger(__name__)


_DOT_MAGIC_WORD = '__DOT__'


RE_TYPES = {
    'str': r'\w+',
    'int': r'[+-]?[0-9]+',
    'float': r'[+-]?([0-9]*[\.])?[0-9]+',
    'bool': r'\w+',
}


#  ### Export related  ###


def _make_path_function(jobs, delimiter_nested='.'):
    from .schema import _build_job_statepoint_index
    if len(jobs) <= 1:
        return lambda job: ''

    index = [{'_id': job._id, 'statepoint': job.sp()} for job in jobs]
    jsi = _build_job_statepoint_index(jobs=jobs, exclude_const=True, index=index)
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


class _SchemaPathEvaluationError(RuntimeError):
    pass


def _export_jobs(jobs, path, copytree):
    "This is a generic export function."
    if path is None:
        path_function = _make_path_function(jobs=jobs)
    elif isinstance(path, six.string_types):

        def path_function(job):
            try:
                return path.format(job=job)
            except AttributeError as error:
                raise _SchemaPathEvaluationError("Attribute Error: {}.".format(error))
            except KeyError as error:
                raise _SchemaPathEvaluationError("Key Error: {}.".format(error))
            except Exception as error:
                raise _SchemaPathEvaluationError("Unknown error: '{}'.".format(error))

    else:
        path_function = path

    # Determine export path for each job.
    paths = {job.workspace(): path_function(job) for job in jobs}

    # Check whether the mapped paths are unique.
    if not len(set(paths.values())) == len(paths):
        raise RuntimeError("Paths generated with given path function are not unique!")

    for src, dst in paths.items():
        copytree(src, dst)
        yield src, dst


def export_to_directory(jobs, target, path=None, copytree=None, progress=False):
    if copytree is None:
        copytree = shutil.copytree

    def copytree_to_directory(src, dst):
        full_dst_path = os.path.join(target, dst)
        _mkdir_p(os.path.dirname(os.path.normpath(full_dst_path)))
        copytree(src, full_dst_path)

    return _export_jobs(jobs=jobs, path=path, copytree=copytree_to_directory)


def export_to_tarfile(jobs, tarfile, path=None):
    return _export_jobs(jobs=jobs, path=path, copytree=tarfile.add)


def export_to_zipfile(jobs, zipfile, path=None):

    def copytree_to_zip(src, dst):
        for root, dirnames, filenames in os.walk(src):
            for fn in filenames:
                zipfile.write(
                    filename=os.path.join(root, fn),
                    arcname=os.path.join(dst, os.path.relpath(root, src), fn))

    return _export_jobs(jobs=jobs, path=path, copytree=copytree_to_zip)


def export_jobs(jobs, target, path=None, copytree=None):
    if copytree is not None:
        if not (isinstance(target, six.string_types) and os.path.splitext(target)[1] == ''):
            raise ValueError(
                "The copytree argument can only be used in combination "
                "with directories as targets.")

    # All of the generator delegations below should be refactored to use 'yield from'
    # once we drop Python 2.7 support.

    if isinstance(target, six.string_types):
        ext = os.path.splitext(target)[1]
        if ext == '':  # target is directory
            for src_dst in export_to_directory(
                    jobs=jobs, target=target, path=path, copytree=copytree):
                yield src_dst
        elif ext == '.zip':     # target is zip-archive
            with ZipFile(target, mode='w', compression=ZIP_DEFLATED) as zipfile:
                for src_dst in export_to_zipfile(jobs=jobs, zipfile=zipfile, path=path):
                    yield src_dst
        elif ext == '.tar':     # target is uncompressed tarball
            with tarfile.open(name=target, mode='a') as file:
                for src_dst in export_to_tarfile(jobs=jobs, tarfile=file, path=path):
                    yield src_dst
        elif ext in ('.gz', '.bz2', '.xz'):    # target is compressed tarball
            with tarfile.open(name=target, mode='w:' + ext[1:]) as file:
                for src_dst in export_to_tarfile(jobs=jobs, tarfile=file, path=path):
                    yield src_dst
    elif isinstance(target, ZipFile):
        for src_dst in export_to_zipfile(jobs=jobs, zipfile=target, path=path):
            yield src_dst
    elif isinstance(target, tarfile.TarFile):
        for src_dst in export_to_tarfile(jobs=jobs, tarfile=target, path=path):
            yield src_dst
    else:
        raise TypeError("Unknown target type", target)


# ### Import related ###

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
        except (IOError, OSError) as error:
            if error.errno != errno.ENOENT:
                raise error

    return _parse_workspace


def _import_into_project(root, project, schema, copytree):
    if root is None:
        root = os.getcwd()
    if schema is None:
        schema_function = _parse_workspaces(project.Job.FN_MANIFEST)
    elif callable(schema):
        schema_function = schema
    elif isinstance(schema, six.string_types):
        if not schema.startswith(root):
            schema = root + r'\/' + schema
        schema_function = _make_schema_path_function(schema)
    else:
        raise TypeError("The schema variable must be None, callable, or a string.")
    if copytree is None:
        copytree = shutil.copytree

    workspace_real_path = os.path.realpath(project.workspace())
    ws_exists = os.path.isdir(project.workspace())
    for path, dirs, _ in os.walk(root):
        sp = schema_function(path)
        if sp is not None:
            del dirs[:]         # skip sub-directories
            if not ws_exists:   # create project workspace if necessary
                os.makedirs(project.workspace())
                ws_exists = True
            job = project.open_job(sp)
            dst = job.workspace()
            if os.path.realpath(path) == os.path.realpath(dst):
                continue     # skip (already part of the data space)
            elif os.path.realpath(path).startswith(workspace_real_path):
                continue     # skip (part of the project's workspace)

            dst_exists = os.path.exists(dst)
            try:
                copytree(path, dst)
            except OSError as error:
                if error.errno in (errno.ENOTEMPTY, errno.EEXIST):
                    raise DestinationExistsError(dst)
                else:
                    raise
            try:
                job._init()  # Ensure existence and correctness of job manifest file.
            except Exception:   # rollback
                if not dst_exists:
                    if copytree == os.rename:
                        os.rename(dst, path)
                    else:
                        shutil.rmtree(dst)
                raise

            yield path, dst


def import_into_project(origin, project, schema=None, copytree=None):
    if os.path.isfile(origin):
        if copytree is not None:
            raise ValueError(
                "Cannot use `copytree` argument when importing from a file!")
        logger.info("Extracting '{}'...".format(origin))
        with _extract(origin) as tmp_root:
            for src_dst in _import_into_project(
                    root=tmp_root, project=project, schema=schema, copytree=shutil.copytree):
                yield src_dst
    elif origin is None or os.path.isdir(origin):
        for src_dst in _import_into_project(
                root=origin, project=project, schema=schema, copytree=copytree):
            yield src_dst
    else:
        raise ValueError("Unable to import from '{}'. Does the origin exist?".format(origin))
