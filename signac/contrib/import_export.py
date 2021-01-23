# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Provides features for importing and exporting data."""

import errno
import logging
import os
import re
import shutil
import tarfile
import zipfile
from collections import OrderedDict
from contextlib import closing, contextmanager
from string import Formatter
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile

from ..core import json
from .errors import DestinationExistsError, StatepointParsingError
from .utility import _dotted_dict_to_nested_dicts, _mkdir_p

logger = logging.getLogger(__name__)


_DOT_MAGIC_WORD = "__DOT__"


RE_TYPES = {
    "str": r"\w+",
    "int": r"[+-]?[0-9]+",
    "float": r"[+-]?([0-9]*[\.])?[0-9]+",
    "bool": r"\w+",
}


#  ### Export related  ###


def _make_schema_based_path_function(jobs, exclude_keys=None, delimiter_nested="."):
    """Generate schema-based paths as a function of the given jobs.

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs (instances of :class:`~signac.contrib.job.Job`).
    exclude_keys : sequence[str]
        A sequence of keys to exclude (Default value = None).
    delimiter_nested : str
        Delimiter used for nesting keys (Default value = '.').

    Returns
    -------
    callable
        Function that returns a normalized path.

    """
    from .schema import _build_job_statepoint_index

    if len(jobs) <= 1:
        # The lambda must (optionally) take a format spec argument to match the
        # signature of the path function below.
        return lambda job, sep=None: ""

    index = [{"_id": job.id, "statepoint": job.statepoint()} for job in jobs]
    jsi = _build_job_statepoint_index(exclude_const=True, index=index)
    sp_index = OrderedDict(jsi)

    paths = {}
    for key_tokens, values in sp_index.items():
        key = key_tokens.replace(".", delimiter_nested)
        if exclude_keys and key in exclude_keys:
            continue
        for value, group in values.items():
            path_tokens = key, str(value)
            for job_id in group:
                paths.setdefault(job_id, list())
                paths[job_id].extend(path_tokens)

    def path(job, sep=None):
        """Normalize the path.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            An instance of :class:`~signac.contrib.job.Job`.
        sep : str
            (Default value = None)

        Returns
        -------
        str
            Normalized path.

        Raises
        ------
        RuntimeError
            If unable to determine path for job.

        """
        try:
            if sep:
                return os.path.normpath(sep.join(paths[job.id]))
            else:
                return os.path.normpath(os.path.join(*paths[job.id]))
        except KeyError:
            raise RuntimeError(
                "Unable to determine path for job '{}'.\nThis is usually caused by a "
                "heterogeneous schema, where some keys are only present in some jobs. "
                "Try providing a custom path.".format(job)
            )

    return path


class _AutoPathFormatter(Formatter):
    """A custom formatter used to format jobs with their respective paths."""

    def __init__(self, paths):
        self.paths = paths

    def format_field(self, value, format_spec):
        """Format string fields, replacing jobs with their paths.

        Parameters
        ----------
        value :
            The value to be formatted.
        format_spec :
            The format specification.

        Returns
        -------
        str
            Formatted string.

        """
        from .job import Job

        if isinstance(value, Job):
            return self.paths(value, format_spec)
        else:
            return super().format_field(value, format_spec)


class _SchemaPathEvaluationError(RuntimeError):
    """Raised for errors in schema path evaluation."""

    pass


def _make_path_function(jobs, path):
    """Generate a path function for jobs or use ``path`` if it is callable.

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs (instances of :class:`~signac.contrib.job.Job`).
    path : callable
        A callable path generating function.

    Returns
    -------
    callable
        Path function for given job and path.

    Raises
    ------
    ValueError
        The path argument must either be ``None``, ``False``, or of type ``str``.

    """
    if path is None:
        # Generate a path function based on the schema detected for jobs.
        path_function = _make_schema_based_path_function(jobs=jobs)

    elif path is False:

        # Just use the job id as path.
        def path_function(job):
            """Use job id to construct path.

            Parameters
            ----------
            job : :class:`~signac.contrib.job.Job`
                An instance of :class:`~signac.contrib.job.Job`.

            Returns
            -------
            str
                Job id.

            """
            return str(job.id)

    elif isinstance(path, str):
        # Detect keys that are already provided as part of the path specifier and
        # and should therefore be excluded from the 'auto'-part.
        exclude_keys = [x[1] for x in Formatter().parse(path)]

        # Generate the paths based on the provided selection, excluding any keys
        # that have already been provided by the user.
        paths = _make_schema_based_path_function(jobs=jobs, exclude_keys=exclude_keys)

        def path_function(job):
            """Format a path based on a given string and schema.

            Parameters
            ----------
            job : :class:`~signac.contrib.job.Job`
                An instance of :class:`~signac.contrib.job.Job`.

            Returns
            -------
            str
                Formatted path.

            """
            try:
                try:
                    ret = path.format(job=job, **job.statepoint)
                except TypeError as error:
                    if (
                        str(error)
                        == "format() got multiple values for keyword argument 'job'"
                    ):
                        try:
                            ret = path.format(job=job)
                        except KeyError:
                            raise _SchemaPathEvaluationError(
                                "You must use fully qualified fields for this path, because the "
                                "state point contains a key called 'job', e.g.: '{job.sp.job}' "
                                "instead of '{job}'."
                            )
                    ret = path.format(job=job)
                return _AutoPathFormatter(paths).format(ret, auto=job)
            except AttributeError as error:
                raise _SchemaPathEvaluationError(error)
            except KeyError as error:
                raise _SchemaPathEvaluationError(f"Unknown key: {error}")
            except Exception as error:
                raise _SchemaPathEvaluationError(error)

    else:
        raise ValueError(
            "The path argument must either be `None`, `False`, or of type `str`."
        )

    return path_function


def _check_directory_structure_validity(paths):
    """Validate directory structure consistency.

    Parameters
    ----------
    paths : sequence[str]
        Sequence of path strings to validate.

    Raises
    ------
    RuntimeError
        If a path is repeated as both a leaf and a node in the directory structure.

    """
    check = set()
    for dst in paths:
        if dst in check:
            raise RuntimeError(
                f"The path '{dst}' is both a leaf and node in the path structure."
            )
        tokens = dst.split(os.path.sep)
        for i in range(1, len(tokens)):
            check.add(os.path.sep.join(tokens[:i]))


def _export_jobs(jobs, path, copytree):
    """Export jobs using the provided copytree method.

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs (instance of :class:`~signac.contrib.job.Job`).
    path : str or callable
        The path (function) used to structure the exported data space (Default value = None).
    copytree : callable
        The function used for copying directory tree structures.

    Yields
    ------
    src : str
        Source path.
    dst : str
        Destination path.

    Raises
    ------
    RuntimeError
        If paths generated with given path function are not unique.

    """
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

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs (instances of :class:`~signac.contrib.job.Job`).
    target : str
        A path to a directory to export to. The directory can not already exist.
    path : str or callable
        The path (function) used to structure the exported data space (Default value = None).
    copytree : callable
        The function used for copying directory tree structures. Uses
        :func:`shutil.copytree` if ``None`` (Default value = None). The function
        requires that the target is a directory.

    Returns
    -------
    generator
        Generator that maps the source directory paths to the target directory paths.

    """
    if copytree is None:
        copytree = shutil.copytree

    def copytree_to_directory(src, dst):
        """Create and export data to a directory.

        Parameters
        ----------
        src : str
            Source path.
        dst : str
            Destination path.

        """
        full_dst_path = os.path.join(target, dst)
        _mkdir_p(os.path.dirname(os.path.normpath(full_dst_path)))
        copytree(src, full_dst_path)

    return _export_jobs(jobs=jobs, path=path, copytree=copytree_to_directory)


def export_to_tarfile(jobs, tarfile, path=None):
    """Like :func:`~.export_to_directory`, but target is an instance of :class:`tarfile.TarFile`.

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs (instances of :class:`~signac.contrib.job.Job`).
    tarfile : :class:`tarfile.TarFile`
        An instance of :class:`tarfile.TarFile`.
    path : str or callable
        The path (function) used to structure the exported data space (Default value = None).

    Returns
    -------
    generator
        Generator that maps the source directory paths to the target directory paths.

    """
    return _export_jobs(jobs=jobs, path=path, copytree=tarfile.add)


def export_to_zipfile(jobs, zipfile, path=None):
    """Like :func:`~.export_to_directory`, but target is an instance of :class:`zipfile.ZipFile`.

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs (instances of :class:`~signac.contrib.job.Job`).
    zipfile : :class:`zipfile.ZipFile`
        An instance of :class:`zipfile.ZipFile`.
    path : str or callable
        The path (function) used to structure the exported data space (Default value = None).

    Returns
    -------
    generator
        Generator that maps the source directory paths to the target directory paths.

    """

    def copytree_to_zip(src, dst):
        """Write a file into a zip archive.

        Parameters
        ----------
        src : str
            Source path.
        dst : str
            Destination path.

        """
        for root, dirnames, filenames in os.walk(src):
            for fn in filenames:
                zipfile.write(
                    filename=os.path.join(root, fn),
                    arcname=os.path.join(dst, os.path.relpath(root, src), fn),
                )

    return _export_jobs(jobs=jobs, path=path, copytree=copytree_to_zip)


def export_jobs(jobs, target, path=None, copytree=None):
    """Export jobs to a target location, such as a directory or a (compressed) archive file.

     Yield tuples ``(src, dst)`` of the exported path sources and destinations.

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs(instance of :class:`~signac.contrib.job.Job`).
    target : str
        A path to a directory or archive file to export to.
    path : str or callable
        The path (function) used to structure the exported data space. (Default value = None)
    copytree : callable
        The function used for copying of directory tree
        structures. Defaults to :func:`shutil.copytree`.
        Can only be used when the target is a directory.

    Yields
    ------
    src : str
        Source path.
    dst : str
        Destination path.

    Raises
    ------
    ValueError
        When copytree argument is given and target is of type `str`.
    TypeError
        When the target type given is unknown. Or
        When the target given is of type `str` and has a unknown extension.

    """
    if copytree is not None:
        if not (isinstance(target, str) and os.path.splitext(target)[1] == ""):
            raise ValueError(
                "The copytree argument can only be used in combination "
                "with directories as targets."
            )

    if isinstance(target, str):
        ext = os.path.splitext(target)[1]
        if ext == "":  # target is directory
            yield from export_to_directory(
                jobs=jobs, target=target, path=path, copytree=copytree
            )
        elif ext == ".zip":  # target is zipfile
            with ZipFile(target, mode="w", compression=ZIP_DEFLATED) as zipfile:
                yield from export_to_zipfile(jobs=jobs, zipfile=zipfile, path=path)
        elif ext == ".tar":  # target is uncompressed tarball
            with tarfile.open(name=target, mode="a") as file:
                yield from export_to_tarfile(jobs=jobs, tarfile=file, path=path)
        elif ext in (".gz", ".bz2", ".xz"):  # target is compressed tarball
            with tarfile.open(name=target, mode="w:" + ext[1:]) as file:
                yield from export_to_tarfile(jobs=jobs, tarfile=file, path=path)
        else:
            raise TypeError(f"Unknown extension '{ext}'.")
    elif isinstance(target, ZipFile):
        yield from export_to_zipfile(jobs=jobs, zipfile=target, path=path)
    elif isinstance(target, tarfile.TarFile):
        yield from export_to_tarfile(jobs=jobs, tarfile=target, path=path)
    else:
        raise TypeError("Unknown target type", target)


# ### Import related ###


def _convert_bool(value):
    """Convert a boolean value encoded as string to corresponding bool.

    Parameters
    ----------
    value : str
        String representation of boolean.

    Returns
    -------
    bool
        Boolean interpreted from string.

    """
    return {"true": True, "1": True, "false": False, "0": False}.get(
        value.lower(), bool(value)
    )


def _convert_schema_path_to_regex(schema_path):
    r"""Convert a schema path to a regular expression.

    For example, the following path 'data\/foo\/{foo:int}' would be converted to
    the following regular expression: 'data\/foo\/(?P<foo>\w+)'.

    When no type is specified, we default to str.

    Parameters
    ----------
    schema_path : str
        Path of schema.

    Returns
    -------
    schema_regex : str
        Regular expression generated from schema path.
    types : dict
        Mapping of keys to their types (``int``, ``float``, ``str``, or ``bool``).

    Raises
    ------
    ValueError
        If an unsupported type is found.

    """
    # First, replace escaped backslashes with double-escaped backslashes.
    # This is needed for compatibility with Windows, which uses backslashes.
    schema_path = re.sub(r"\\", r"\\\\", schema_path)

    # The regular expression below is used to identify the {value:type} specifications
    # in the schema path.
    re_key_type_field = r"\{(?P<key>[\.\w]+)(?::(?P<type>[a-z]+))?\}"
    schema_regex = ""  # the return value
    types = {}  # maps values to their designated types
    index = 0
    while True:
        m = re.search(re_key_type_field, schema_path[index:])
        if m:
            key = m.groupdict()["key"].replace(".", _DOT_MAGIC_WORD)
            types[key] = m.groupdict()["type"] or "str"
            start, stop = m.span()
            schema_regex += schema_path[index : index + start].replace(".", r"\.")
            schema_regex += r"(?P<{}>{})".format(key, RE_TYPES[types[key]])
            index += stop
            continue
        break
    schema_regex += "$"

    for key in types:
        if types[key] in ("int", "float", "str"):
            types[key] = eval(types[key])
        elif types[key] == "bool":
            types[key] = _convert_bool
        else:
            raise ValueError("Invalid type '{}'.".format(types[key]))
    return schema_regex, types


def _make_path_based_schema_function(schema_path):
    """Generate a schema function that is based on a directory path schema.

    Parameters
    ----------
    schema_path : str
        Path of schema.

    Returns
    -------
    callable
        Function that parses the schema path.

    """
    schema_regex, types = _convert_schema_path_to_regex(schema_path)

    def parse_path(path):
        """Parse the provided path.

        Parameters
        ----------
        path : str
            The path to parse.

        Returns
        -------
        dict
            A mapping instance with nested dicts, e.g. {'a': {'b': 'c'}}.

        """
        match = re.match(schema_regex, os.path.normpath(path))
        if match:
            statepoint = match.groupdict()
            for key in types:
                if key in statepoint:
                    statepoint[key] = types[key](statepoint[key])
            return _dotted_dict_to_nested_dicts(
                statepoint, delimiter_nested=_DOT_MAGIC_WORD
            )

    return parse_path


def _with_consistency_check(schema_function, read_sp_manifest_file):
    """Return a function to check schema consistency.

    Parameters
    ----------
    schema_function : callable
        Schema function.
    read_sp_manifest_file : callable
        Function to read state point manifest.

    Returns
    -------
    callable
        Schema checking function.

    """

    def _check(path):
        """Check if the schema-detected state point matches the manifest file.

        Parameters
        ----------
        path : str
            Path to parse with schema function.

        Returns
        -------
        dict
            State point identified.

        Raises
        ------
        :class:`~signac.errors.StatepointParsingError`
            If identified state point conflicts with state point in job manifest file.

        """
        if schema_function is read_sp_manifest_file:
            return schema_function(path)
        else:
            sp = schema_function(path)
            sp_default = read_sp_manifest_file(path)
            if sp and sp_default and sp_default != sp:
                raise StatepointParsingError(
                    "Identified state point conflicts with state point in job manifest file!"
                )
            return sp

    return _check


def _parse_workspaces(fn_manifest):
    """Generate a schema function based on parsing state point manifest files.

    Parameters
    ----------
    fn_manifest : str
        Manifest file name.

    Returns
    -------
    callable
        Function to parse a manifest, given a path.

    """

    def _parse_workspace(path):
        """Parse a manifest, given a path.

        Parameters
        ----------
        path : str
            Path containing manifest file.

        Returns
        -------
        dict
            Parsed manifest contents.

        """
        try:
            with open(os.path.join(path, fn_manifest), "rb") as file:
                return json.loads(file.read().decode())
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise error

    return _parse_workspace


def _crawl_directory_data_space(root, project, schema_function):
    """Crawl the directory data space.

    Parameters
    ----------
    root : str
        Path to the root directory.
    project : :class:`~signac.Project`
        The signac project.
    schema_function : callable
        Schema function.

    Yields
    ------
    path : str
        Path.
    job : :class:`~signac.contrib.job.Job`
        Job instance.

    """
    # We compare paths to the 'realpath' of the project workspace to catch loops.
    workspace_real_path = os.path.realpath(project.workspace())

    for path, dirs, _ in os.walk(root):
        sp = schema_function(path)
        if sp is not None:
            del dirs[:]  # skip sub-directories
            job = project.open_job(sp)
            dst = job.workspace()
            if os.path.realpath(path) == os.path.realpath(dst):
                continue  # skip (already part of the data space)
            elif os.path.realpath(path).startswith(workspace_real_path):
                continue  # skip (part of the project's workspace)
            yield path, job


def _copy_to_job_workspace(src, job, copytree):
    """Copy the source to job's workspace.

    Parameters
    ----------
    src : str
        Name of source file copy.
    job : :class:`~signac.contrib.job.Job`
        An instance of :class:`~signac.contrib.job.Job`.
    copytree : callable
        Function to use for the copytree operation. Defaults to
        :func:`shutil.copytree`.

    Returns
    -------
    str
        Destination filename.

    """
    dst = job.workspace()
    try:
        copytree(src, dst)
    except OSError as error:
        if error.errno in (errno.EEXIST, errno.ENOTEMPTY, errno.EACCES):
            raise DestinationExistsError(job)
        raise
    else:
        job._init()
    return dst


class _CopyFromDirectoryExecutor:
    """Copy the source to job's workspace when the source is a directory.

    Parameters
    ----------
    src : str
        Name of source file copy.
    job : :class:`~signac.contrib.job.Job`
        An instance of :class:`~signac.contrib.job.Job`.

    """

    def __init__(self, src, job):
        self.src = src
        self.job = job

    def __call__(self, copytree=None):
        if copytree is None:
            copytree = shutil.copytree
        return _copy_to_job_workspace(self.src, self.job, copytree)


def _analyze_directory_for_import(root, project, schema):
    """Prepare the data space located at the root directory for import into project.

    Parameters
    ----------
    root : str
        Path of the root directory.
    project : :class:`~signac.Project`
        The signac project.
    schema : str or callable
        An optional schema function, which is either a string or a function that accepts a
        path as its first and only argument and returns the corresponding state point as dict
        (Default value = None).

    Yields
    ------
    src : str
        Source path.
    copy_executor : str
        A callable that uses a provided function to copy to a destination.

    Raises
    ------
    TypeError
        If the schema given is not None, callable, or a string.
    :class:`~signac.errors.StatepointParsingError`
        If the jobs identified with the given schema function are not unique.

    """
    # Determine schema function
    read_sp_manifest_file = _parse_workspaces(project.Job.FN_MANIFEST)
    if schema is None:
        schema_function = read_sp_manifest_file
    elif callable(schema):
        schema_function = _with_consistency_check(schema, read_sp_manifest_file)
    elif isinstance(schema, str):
        if not schema.startswith(root):
            schema = os.path.normpath(os.path.join(root, schema))
        schema_function = _with_consistency_check(
            _make_path_based_schema_function(schema), read_sp_manifest_file
        )
    else:
        raise TypeError("The schema variable must be None, callable, or a string.")

    # Determine the data space mapping from directories at root to project jobs.
    jobs = set()
    for src, job in _crawl_directory_data_space(root, project, schema_function):
        if job in jobs:
            raise StatepointParsingError(
                "The jobs identified with the given schema function are not unique!"
            )
        else:
            jobs.add(job)
            copy_executor = _CopyFromDirectoryExecutor(src, job)
            yield src, copy_executor


class _CopyFromZipFileExecutor:
    """Copy the source to job's workspace when the source is a zipfile.

    Parameters
    ----------
    zipfile : zipfile.ZipFile
        An instance of ZipFile.
    root : str
        Path of the root directory.
    job : :class:`~signac.contrib.job.Job`
        An instance of :class:`~signac.contrib.job.Job`.
    names : sequence[str]
        File names to copy.

    """

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
            with open(fn_dst, "wb") as dst:
                dst.write(self.zipfile.read(name))
        return self.job.workspace()

    def __str__(self):
        return "{}({} -> {})".format(type(self).__name__, self.root, self.job)


def _analyze_zipfile_for_import(zipfile, project, schema):
    """Validate paths in zipfile.

    Parameters
    ----------
    zipfile : zipfile.ZipFile
        An instance of ZipFile.
    project : :class:`~signac.Project`
        The signac project.
    schema : str or callable
        An optional schema function, which is either a string or a function that accepts a
        path as its first and only argument and returns the corresponding state point as dict
        (Default value = None).

    Yields
    ------
    src : str
        Source path.
    copy_executor : callable
        A callable that uses a provided function to copy to a destination.

    Raises
    ------
    TypeError
        If the schema provided is not None, callable, or a string.
    :class:`~signac.errors.DestinationExistsError`
        If a job is already initialized.
    :class:`~signac.errors.StatepointParsingError`
        If the jobs identified with the given schema function are not unique.

    """
    names = zipfile.namelist()

    def read_sp_manifest_file(path):
        """Read a state point manifest file.

        Parameters
        ----------
        path : str
            Path to manifest file.

        Returns
        -------
        dict
            Parsed manifest contents.

        """
        # Must use forward slashes, not os.path.sep.
        fn_manifest = path + "/" + project.Job.FN_MANIFEST
        if fn_manifest in names:
            return json.loads(zipfile.read(fn_manifest).decode())

    if schema is None:
        schema_function = read_sp_manifest_file
    elif callable(schema):
        schema_function = _with_consistency_check(schema, read_sp_manifest_file)
    elif isinstance(schema, str):
        schema_function = _with_consistency_check(
            _make_path_based_schema_function(schema), read_sp_manifest_file
        )
    else:
        raise TypeError("The schema variable must be None, callable, or a string.")

    mappings = {}
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
        raise StatepointParsingError(
            "The jobs identified with the given schema function are not unique!"
        )

    for src, job in mappings.items():
        _names = [name for name in names if name.startswith(src)]
        copy_executor = _CopyFromZipFileExecutor(zipfile, src, job, _names)
        yield src, copy_executor


class _CopyFromTarFileExecutor:
    """Copy the source to job's workspace when the source is a tarfile.

    Parameters
    ----------
    src : str
        Source path.
    job : :class:`~signac.contrib.job.Job`
        An instance of :class:`~signac.contrib.job.Job`.

    """

    def __init__(self, src, job):
        self.src = src
        self.job = job

    def __call__(self, copytree=None):
        assert copytree is None
        assert os.path.isdir(self.src)
        return _copy_to_job_workspace(self.src, self.job, shutil.copytree)


def _tarfile_path_join(path, fn):
    """Join paths like os.path.join but always with forward slashes.

    Due to this bug in Python tarfile (https://bugs.python.org/issue21987) we
    may or may not have a trailing backslash in the provided path. Rather than
    checking the exact length, which could lead to backwards incompatibilities,
    we simply strip trailing slashes and always add them back.

    Parameters
    ----------
    path : str
        Path.
    fn : str
        File name.

    Returns
    -------
    str
        Path with normalized forward slashes.

    """
    path = path.rstrip("/")
    return path + "/" + fn


def _analyze_tarfile_for_import(tarfile, project, schema, tmpdir):
    """Validate paths in tarfile.

    Parameters
    ----------
    tarfile : :class:`tarfile.TarFile`
        tarfile to analyze.
    project : :class:`~signac.Project`
        The project to import the data into.
    schema : str or callable
        An optional schema function, which is either a string or a function that accepts a
        path as its first and only argument and returns the corresponding state point as dict.
        (Default value = None).
    tmpdir : :class:`tempfile.TemporaryDirectory`
        Temporary directory, an instance of ``TemporaryDirectory``.

    Yields
    ------
    src : str
        Source path.
    copy_executor : callable
        A callable that uses a provided function to copy to a destination.

    Raises
    ------
    TypeError
        If the schema given is not None, callable, or a string.
    :class:`~signac.errors.DestinationExistsError`
        If a job is already initialized.
    :class:`~signac.errors.StatepointParsingError`
        If the jobs identified with the given schema function are not unique.
    AssertionError
        If ``tmpdir`` given is not a directory.

    """

    def read_sp_manifest_file(path):
        """Read state point from the manifest file.

        Parameters
        ----------
        path : str
            Path to manifest file.

        Returns
        -------
        dict
            state point.

        """
        # Must use forward slashes, not os.path.sep.
        fn_manifest = _tarfile_path_join(path, project.Job.FN_MANIFEST)
        try:
            with closing(tarfile.extractfile(fn_manifest)) as file:
                return json.loads(file.read())
        except KeyError:
            pass

    if schema is None:
        schema_function = read_sp_manifest_file
    elif callable(schema):
        schema_function = _with_consistency_check(schema, read_sp_manifest_file)
    elif isinstance(schema, str):
        schema_function = _with_consistency_check(
            _make_path_based_schema_function(schema), read_sp_manifest_file
        )
    else:
        raise TypeError("The schema variable must be None, callable, or a string.")

    mappings = {}
    skip_subdirs = set()

    dirs = [member.name for member in tarfile.getmembers() if member.isdir()]
    for name in sorted(dirs):
        if (
            os.path.dirname(name) in skip_subdirs
        ):  # skip all sub-dirs of identified dirs
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
            "The jobs identified with the given schema function are not unique!"
        )

    tarfile.extractall(path=tmpdir)
    for path, job in mappings.items():
        assert os.path.isdir(tmpdir)
        src = os.path.join(tmpdir, path)
        assert os.path.isdir(src)
        copy_executor = _CopyFromTarFileExecutor(src, job)
        yield src, copy_executor


@contextmanager
def _prepare_import_into_project(origin, project, schema=None):
    """Prepare the data space at origin for import into project with the given schema function.

    Parameters
    ----------
    origin : str
        Path to current working directory.
    project : :class:`~signac.Project`
        The project to import the data into.
    schema : str or callable
        An optional schema function, which is either a string or a function that accepts a
        path as its first and only argument and returns the corresponding state point as dict
        (Default value = None).

    Yields
    ------
    src : str
        Source path.
    dst : str
        Destination path.

    Raises
    ------
    RuntimeError
        When file type of `origin` is unknown.
    ValueError
        When given `origin` can not be imported.

    """
    if os.path.isfile(origin):
        if zipfile.is_zipfile(origin):
            with zipfile.ZipFile(origin) as file:
                yield _analyze_zipfile_for_import(file, project, schema)
        elif tarfile.is_tarfile(origin):
            with TemporaryDirectory() as tmpdir:
                with tarfile.open(origin) as file:
                    yield _analyze_tarfile_for_import(file, project, schema, tmpdir)
        else:
            raise RuntimeError(f"Unknown file type: '{origin}'.")
    elif os.path.isdir(origin):
        yield _analyze_directory_for_import(root=origin, project=project, schema=schema)
    else:
        raise ValueError(f"Unable to import from '{origin}'. Does the origin exist?")


def import_into_project(origin, project, schema=None, copytree=None):
    """Import the data space located at origin into project.

    This function will walk through the data space located at origin and try to identify
    data space paths that can be imported as a job workspace into project.

    The default schema function will simply look for state point manifest files -- usually named
    ``signac_statepoint.json`` -- and then import all data located within that path into the job
    workspace corresponding to the state point specified in the manifest file.

    Alternatively the schema argument may be a string, that is converted into a schema function,
    for example: Providing ``foo/{foo:int}`` as schema argument means that all directories under
    ``foo/`` will be imported and their names will be interpreted as the value for ``foo`` within
    the state point.

    .. tip::

        Use ``copytree=os.replace`` or ``copytree=shutil.move`` to move dataspaces on import
        instead of copying them.

        Warning: Imports can fail due to conflicts. Moving data instead of copying may
        therefore lead to inconsistent states and users are advised to apply caution.

    Parameters
    ----------
    origin : str
        The path to the data space origin, which is to be imported. This may be a path to
        a directory, a zipfile, or a tarball archive.
    project : :class:`~signac.Project`
        The project to import the data into.
    schema : str or callable
        An optional schema function, which is either a string or a function that accepts a
        path as its first and only argument and returns the corresponding state point as dict
        (Default value = None).
    copytree : callable
        Function to use for the copytree operation. Defaults to :func:`shutil.copytree`.

    Yields
    ------
    src : str
        Source path.
    dst : str
        Destination path.

    """
    if origin is None:
        origin = os.getcwd()

    with _prepare_import_into_project(origin, project, schema) as data_mapping:
        if copytree is None and os.path.isdir(origin):
            copytree = shutil.copytree

        for src, copy in data_mapping:
            yield src, copy(copytree)
