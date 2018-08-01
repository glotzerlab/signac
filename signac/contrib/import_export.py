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
from contextlib import contextmanager

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


def _make_schema_based_path_function(jobs, delimiter_nested='.'):
    "Generate a schema based path function for the given jobs."
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
    "Generic export function for jobs, using the provided copytree method."
    if path is None:
        path_function = _make_schema_based_path_function(jobs=jobs)
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


def export_to_directory(jobs, target, path=None, copytree=None):
    """Export jobs to a directory target.

    :param jobs:
        A sequence of jobs to export.
    :param target:
        A path to a directory to export to. The directory can not
        already exist.
    :param path:
        The path function for export, must be a function of job or
        a string, which is evaluated with ``path.format(job=job)``.
    :param copytree:
        The function used for the actualy copying of directory tree
        structures. Defaults to :func:`shutil.copytree`.
    :returns:
        A dict that maps the source directory paths, to the target
        directory paths.
    """
    if copytree is None:
        copytree = shutil.copytree

    def copytree_to_directory(src, dst):
        full_dst_path = os.path.join(target, dst)
        _mkdir_p(os.path.dirname(os.path.normpath(full_dst_path)))
        copytree(src, full_dst_path)

    return _export_jobs(jobs=jobs, path=path, copytree=copytree_to_directory)


def export_to_tarfile(jobs, tarfile, path=None):
    """Like :func:`~.export_to_directory`, but target is an instance of :class:`tarfile.TarFile`."""
    return _export_jobs(jobs=jobs, path=path, copytree=tarfile.add)


def export_to_zipfile(jobs, zipfile, path=None):
    """Like :func:`~.export_to_directory`, but target is an instance of :class:`zipfile.ZipFile`."""

    def copytree_to_zip(src, dst):
        for root, dirnames, filenames in os.walk(src):
            for fn in filenames:
                zipfile.write(
                    filename=os.path.join(root, fn),
                    arcname=os.path.join(dst, os.path.relpath(root, src), fn))

    return _export_jobs(jobs=jobs, path=path, copytree=copytree_to_zip)


def export_jobs(jobs, target, path=None, copytree=None):
    """Export jobs to a target location, such as a directory or a (zipped) archive file.

    :param jobs:
        A sequence of jobs to export.
    :param target:
        A path to a directory to export to. The directory can not
        already exist.
    :param path:
        The path function for export, must be a function of job or
        a string, which is evaluated with ``path.format(job=job)``.
    :param copytree:
        The function used for the actualy copying of directory tree
        structures. Defaults to :func:`shutil.copytree`.
        Can only be used when the target is a directory.
    :returns:
        A dict that maps the source directory paths, to the target
        directory paths.
    """
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
        elif ext == '.zip':     # target is zipfile
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
    "Generate a schema function that is based on a directory path schema."
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
    "Generate a schema function that is based on parsing state point manifest files."

    def _parse_workspace(path):
        try:
            with open(os.path.join(path, fn_manifest), 'rb') as file:
                return json.loads(file.read().decode())
        except (IOError, OSError) as error:
            if error.errno != errno.ENOENT:
                raise error

    return _parse_workspace


def _crawl_data_space(root, project, schema_function):
    # We compare paths to the 'realpath' of the project workspace to catch loops.
    workspace_real_path = os.path.realpath(project.workspace())

    for path, dirs, _ in os.walk(root):
        sp = schema_function(path)
        if sp is not None:
            del dirs[:]         # skip sub-directories
            job = project.open_job(sp)
            dst = job.workspace()
            if os.path.realpath(path) == os.path.realpath(dst):
                continue     # skip (already part of the data space)
            elif os.path.realpath(path).startswith(workspace_real_path):
                continue     # skip (part of the project's workspace)
            yield path, job


def _import_data_into_project(src, job, copytree):
    dst = job.workspace()
    dst_exists = os.path.exists(dst)

    try:
        copytree(src, dst)
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
                os.rename(dst, src)
            else:
                shutil.rmtree(dst)
        raise

    return dst


def _analyze_data_space_for_import(root, project, schema):
    "Prepare the data space located at the root directory for import into project."
    # Determine schema function
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

    # Determine the data space mapping from directories at root to project jobs.
    jobs = set()
    for src, job in _crawl_data_space(root, project, schema_function):
        if job in jobs:
            raise RuntimeError("The jobs identified with the given schema function are not unique!")
        else:
            jobs.add(job)
            yield src, job


@contextmanager
def _prepare_import_into_project(origin, project, schema=None):
    "Prepare the data space at origin for import into project with the given schema function."
    if os.path.isfile(origin):
        logger.info("Extracting '{}'...".format(origin))
        with _extract(origin) as tmp_root:
            with _prepare_import_into_project(tmp_root, project, schema) as tmp:
                yield tmp
    elif os.path.isdir(origin):
        yield _analyze_data_space_for_import(root=origin, project=project, schema=schema)
    else:
        raise ValueError("Unable to import from '{}'. Does the origin exist?".format(origin))


def import_into_project(origin, project, schema=None, copytree=None):
    """Import the data space located at origin into project.

    This function will walk through the data space located at origin and try to identify
    data space paths that can be imported as a job workspace into project.

    The default schema function will simply look for state point manifest files -- usually named
    ``signac_statepoint.json`` -- and then import all data located within that path into the job
    workspace corresponding to the state point specified in the manifest file.

    Alternatively the schema argument may be a string, that is converted into a schema function,
    for example: Providing ``foo/{foo:int}`` as schema argument means that all directories under
    ``foo/`` will be imported and their names will be interpeted as the value for ``foo`` within
    the state point.

    .. tip::

        Use ``copytree=os.rename`` or ``copytree=shutil.move`` to move dataspaces on import
        instead of copying them.

        Warning: Imports can fail due to conflicts. Moving data instead of copying may
        therefore lead to inconsistent states and users are advised to apply caution.

    :param origin:
        The path to the data space origin, which is to be imported. This may be a path to
        a directory, a zipfile, or a tarball archive.
    :param project:
        The project to import the data into.
    :param schema:
        An optional schema function, which is either a string or a function that accepts a
        path as its first and only argument and returns the corresponding state point as dict.
    :param copytree:
        Specify which exact function to use for the actual copytree operation.
        Defaults to :func:`shutil.copytree`.
    :returns:
        A dict that maps the source directory paths, to the target
        directory paths.
    """
    if origin is None:
        origin = os.getcwd()

    with _prepare_import_into_project(origin, project, schema) as data_mapping:
        if copytree is None:
            copytree = shutil.copytree

        for src, job in data_mapping:
            yield src, _import_data_into_project(src, job, copytree)
