# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
import sys
import errno
import shutil
import zipfile
import tarfile
from zipfile import ZipFile, ZIP_DEFLATED
from collections import OrderedDict
from contextlib import contextmanager, closing
from string import Formatter

from ..common import six
from ..common.tempdir import TemporaryDirectory
from ..core import json
from .errors import StatepointParsingError
from .errors import DestinationExistsError
from .utility import _mkdir_p

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


def _make_schema_based_path_function(jobs, exclude_keys=None, delimiter_nested='.'):
    "Generate schema based paths as a function of the given jobs."
    from .schema import _build_job_statepoint_index
    if len(jobs) <= 1:
        return lambda job: ''

    index = [{'_id': job._id, 'statepoint': job.sp()} for job in jobs]
    jsi = _build_job_statepoint_index(jobs=jobs, exclude_const=True, index=index)
    sp_index = OrderedDict(jsi)

    paths = dict()
    for key_tokens, values in sp_index.items():
        key = delimiter_nested.join(map(str, key_tokens))
        if exclude_keys and key in exclude_keys:
            continue
        for value, group in values.items():
            path_tokens = key, str(value)
            for job_id in group:
                paths.setdefault(job_id, list())
                paths[job_id].extend(path_tokens)

    def path(job, sep=None):
        try:
            if sep:
                return os.path.normpath(sep.join(paths[job._id]))
            else:
                return os.path.normpath(os.path.join(* paths[job._id]))
        except KeyError:
            raise RuntimeError(
                "Unable to determine path for job '{}'.\nThis is usually caused by a "
                "heterogeneous schema, where some keys are only present in some jobs. "
                "Try providing a custom path.".format(job))
    return path


class _AutoPathFormatter(Formatter):

    def __init__(self, paths):
        self.paths = paths

    def format_field(self, value, format_spec):
        from .job import Job
        if isinstance(value, Job):
            return self.paths(value, format_spec)
        else:
            return super(_AutoPathFormatter, self).format_field(value, format_spec)


class _SchemaPathEvaluationError(RuntimeError):
    pass


def _make_path_function(jobs, path):
    "Generate a path function for jobs or use `path` if its a callable."

    if path is None:
        # Generate a path function based on the schema detected for jobs.
        path_function = _make_schema_based_path_function(jobs=jobs)

    elif path is False:

        # Just use the job-id as path.
        def path_function(job):
            return str(job.get_id())

    elif isinstance(path, six.string_types):
        # Detect keys that are already provided as part of the path specifier and
        # and should therefore be excluded from the 'auto'-part.
        exclude_keys = [x[1] for x in Formatter().parse(path)]

        # Generate the paths based on the provided selection, excluding any keys
        # that have already been provided by the user.
        paths = _make_schema_based_path_function(jobs=jobs, exclude_keys=exclude_keys)

        def path_function(job):
            try:
                try:
                    ret = path.format(job=job, **job.sp)
                except TypeError as error:
                    if str(error) == "format() got multiple values for keyword argument 'job'":
                        try:
                            ret = path.format(job=job)
                        except KeyError:
                            raise _SchemaPathEvaluationError(
                                "You must use fully qualified fields for this path, because the "
                                "state point contains a key called 'job', e.g.: '{job.sp.job}' "
                                "instead of '{job}'.")
                    ret = path.format(job=job)
                return _AutoPathFormatter(paths).format(ret, auto=job)
            except AttributeError as error:
                raise _SchemaPathEvaluationError(error)
            except KeyError as error:
                raise _SchemaPathEvaluationError("Unknown key: {}".format(error))
            except Exception as error:
                raise _SchemaPathEvaluationError(error)
    else:
        raise ValueError(
            "The path argument must either be `None`, `False`, or of type `str`.")

    return path_function


def _check_directory_structure_validity(paths):
    "Check the consistency of the directory structure for export."
    check = set()
    for dst in paths:
        if dst in check:
            raise RuntimeError(
                "The path '{}' is both a leaf and node in the path structure.".format(dst))
        tokens = dst.split(os.path.sep)
        for i in range(1, len(tokens)):
            check.add(os.path.sep.join(tokens[:i]))


def _export_jobs(jobs, path, copytree):
    "Generic export function for jobs, using the provided copytree method."

    # Transform the path argument into a callable if necessary.
    if callable(path):
        path_function = path
    else:
        path_function = _make_path_function(jobs, path)

    # Determine export path for each job.
    paths = {job.workspace(): path_function(job) for job in jobs}

    # Check whether the mapped paths are unique.
    if len(set(paths.values())) != len(paths):
        raise RuntimeError("Paths generated with given path function are not unique!")

    # Check leaf/node consistency
    _check_directory_structure_validity(paths.values())

    for src, dst in paths.items():
        copytree(src, dst)
        yield src, dst


def export_to_directory(jobs, target, path=None, copytree=None):
    """Export jobs to a directory.

    :param jobs:
        A sequence of jobs to export.
    :param target:
        A path to a directory to export to. The directory can not already exist.
    :param path:
        The path (function) used to structure the exported data space.
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
    """Export jobs to a target location, such as a directory or a (compressed) archive file.

    :param jobs:
        A sequence of jobs to export.
    :param target:
        A path to a directory or archive file to export to.
    :param path:
        The path (function) used to structure the exported data space.
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
        else:
            raise TypeError("Unknown extension '{}'.".format(ext))
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


def _make_path_based_schema_function(schema_path):
    "Generate a schema function that is based on a directory path schema."
    schema_regex, types = _convert_schema_path_to_regex(schema_path)

    def parse_path(path):
        match = re.match(schema_regex, os.path.normpath(path))
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
                tmp = tmp.setdefault(token, dict())
            tmp[tokens[-1]] = value
        else:
            ret[tokens[0]] = value
    return ret


def _with_consistency_check(schema_function, read_sp_manifest_file):
    "Check whether the state point detected from the schema function matches the manifest file."

    def _check(path):
        if schema_function is read_sp_manifest_file:
            return schema_function(path)
        else:
            sp = schema_function(path)
            sp_default = read_sp_manifest_file(path)
            if sp and sp_default and sp_default != sp:
                raise StatepointParsingError(
                    "Identified state point conflicts with state point in job manifest file!")
            return sp
    return _check


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


def _crawl_directory_data_space(root, project, schema_function):
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


def _copy_to_job_workspace(src, job, copytree):
    dst = job.workspace()
    try:
        copytree(src, dst)
    except (IOError, OSError) as error:
        if error.errno in (errno.ENOTEMPTY, errno.EEXIST):
            raise DestinationExistsError(job)
        raise
    else:
        job._init()
    return dst


class _CopyFromDirectoryExecutor(object):

    def __init__(self, src, job):
        self.src = src
        self.job = job

    def __call__(self, copytree=None):
        if copytree is None:
            copytree = shutil.copytree
        return _copy_to_job_workspace(self.src, self.job, copytree)


def _analyze_directory_for_import(root, project, schema):
    "Prepare the data space located at the root directory for import into project."

    # Determine schema function
    read_sp_manifest_file = _parse_workspaces(project.Job.FN_MANIFEST)
    if schema is None:
        schema_function = read_sp_manifest_file
    elif callable(schema):
        schema_function = _with_consistency_check(schema, read_sp_manifest_file)
    elif isinstance(schema, six.string_types):
        if not schema.startswith(root):
            schema = os.path.normpath(os.path.join(root, schema))
        schema_function = _with_consistency_check(
            _make_path_based_schema_function(schema), read_sp_manifest_file)
    else:
        raise TypeError("The schema variable must be None, callable, or a string.")

    # Determine the data space mapping from directories at root to project jobs.
    jobs = set()
    for src, job in _crawl_directory_data_space(root, project, schema_function):
        if job in jobs:
            raise StatepointParsingError(
                "The jobs identified with the given schema function are not unique!")
        else:
            jobs.add(job)
            yield src, _CopyFromDirectoryExecutor(src, job)


class _CopyFromZipFileExecutor(object):

    def __init__(self, zipfile, root, job, names):
        self.zipfile = zipfile
        self.root = root
        self.job = job
        self.names = names

    def __call__(self, copytree=None):
        assert copytree is None

        for name in self.names:
            fn_dst = self.job.fn(os.path.relpath(name, self.root))
            _mkdir_p(os.path.dirname(fn_dst))
            with open(fn_dst, 'wb') as dst:
                dst.write(self.zipfile.read(name))
        return self.job.workspace()

    def __str__(self):
        return "{}({} -> {})".format(type(self).__name__, self.root, self.job)


def _analyze_zipfile_for_import(zipfile, project, schema):
    names = zipfile.namelist()

    def read_sp_manifest_file(path):
        fn_manifest = os.path.join(path, project.Job.FN_MANIFEST)
        if fn_manifest in names:
            return json.loads(zipfile.read(fn_manifest).decode())

    if schema is None:
        schema_function = read_sp_manifest_file
    elif callable(schema):
        schema_function = _with_consistency_check(schema, read_sp_manifest_file)
    elif isinstance(schema, six.string_types):
        schema_function = _with_consistency_check(
            _make_path_based_schema_function(schema), read_sp_manifest_file)
    else:
        raise TypeError("The schema variable must be None, callable, or a string.")

    mappings = dict()
    skip_subdirs = set()

    dirs = {os.path.dirname(name) for name in names}
    for name in sorted(dirs):
        cont = False
        for skip in skip_subdirs:
            if name.startswith(skip):
                cont = True
                break
        if cont:
            continue

        sp = schema_function(name)
        if sp is not None:
            job = project.open_job(sp)
            if os.path.exists(job.workspace()):
                raise DestinationExistsError(job)
            mappings[name] = job
            skip_subdirs.add(name)

    # Check uniqueness
    if len(set(mappings.values())) != len(mappings):
        raise RuntimeError("The jobs identified with the given schema function are not unique!")

    for path, job in mappings.items():
        _names = [name for name in names if name.startswith(path)]
        yield path, _CopyFromZipFileExecutor(zipfile, path, job, _names)


class _CopyFromTarFileExecutor(object):

    def __init__(self, src, job):
        self.src = src
        self.job = job

    def __call__(self, copytree=None):
        assert copytree is None
        assert os.path.isdir(self.src)
        return _copy_to_job_workspace(self.src, self.job, shutil.copytree)


def _analyze_tarfile_for_import(tarfile, project, schema, tmpdir):

    def read_sp_manifest_file(path):
        fn_manifest = os.path.join(path, project.Job.FN_MANIFEST)
        try:
            with closing(tarfile.extractfile(fn_manifest)) as file:
                if six.PY3 and sys.version_info.minor < 6:
                    return json.loads(file.read().decode())
                else:
                    return json.loads(file.read())
        except KeyError:
            pass

    if schema is None:
        schema_function = read_sp_manifest_file
    elif callable(schema):
        schema_function = _with_consistency_check(schema, read_sp_manifest_file)
    elif isinstance(schema, six.string_types):
        schema_function = _with_consistency_check(
            _make_path_based_schema_function(schema), read_sp_manifest_file)
    else:
        raise TypeError("The schema variable must be None, callable, or a string.")

    mappings = dict()
    skip_subdirs = set()

    dirs = [member.name for member in tarfile.getmembers() if member.isdir()]
    for name in sorted(dirs):
        if os.path.dirname(name) in skip_subdirs:   # skip all sub-dirs of identified dirs
            skip_subdirs.add(name)
            continue

        sp = schema_function(name)
        if sp is not None:
            job = project.open_job(sp)
            if os.path.exists(job.workspace()):
                raise DestinationExistsError(job)
            mappings[name] = job
            skip_subdirs.add(name)

    # Check uniqueness
    if len(set(mappings.values())) != len(mappings):
        raise StatepointParsingError(
            "The jobs identified with the given schema function are not unique!")

    tarfile.extractall(path=tmpdir)
    for path, job in mappings.items():
        src = os.path.join(tmpdir, path)
        assert os.path.isdir(tmpdir)
        assert os.path.isdir(src)
        yield src, _CopyFromTarFileExecutor(src, job)


@contextmanager
def _prepare_import_into_project(origin, project, schema=None):
    "Prepare the data space at origin for import into project with the given schema function."
    if os.path.isfile(origin):
        if zipfile.is_zipfile(origin):
            with zipfile.ZipFile(origin) as file:
                yield _analyze_zipfile_for_import(file, project, schema)
        elif tarfile.is_tarfile(origin):
            with TemporaryDirectory() as tmpdir:
                with tarfile.open(origin) as file:
                    yield _analyze_tarfile_for_import(file, project, schema, tmpdir)
        else:
            raise RuntimeError("Unknown file type: '{}'.".format(origin))
    elif os.path.isdir(origin):
        yield _analyze_directory_for_import(root=origin, project=project, schema=schema)
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

        Use ``copytree=os.renames`` or ``copytree=shutil.move`` to move dataspaces on import
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
        if copytree is None and os.path.isdir(origin):
            copytree = shutil.copytree

        for src, copy in data_mapping:
            yield src, copy(copytree)
