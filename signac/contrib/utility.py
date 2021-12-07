# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Utilities for signac."""

import argparse
import getpass
import logging
import os
import sys
import tarfile
import zipfile
from collections import OrderedDict
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import timedelta
from string import Formatter
from tempfile import TemporaryDirectory
from time import time

from deprecation import deprecated

from ..version import __version__

logger = logging.getLogger(__name__)


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning
    an answer is required of the user).

    The "answer" return value is one of "yes" or "no".

    Parameters
    ----------
    question : str
        Question presented to the user.
    default : str
        Presumed answer if the user just hits <Enter> (Default value = "yes").

    Returns
    -------
    bool
        ``True`` if yes, ``False`` if no.

    Raises
    ------
    ValueError
        When default is set to invalid answer.

    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def prompt_password(prompt="Password: "):
    """Prompt password for user.

    Parameters
    ----------
    prompt : str
        String to prompt (Default value = 'Password: ').

    Returns
    -------
    str
        Password input by the user.

    """
    return getpass.getpass(prompt)


def add_verbosity_argument(parser, default=0):
    """Add a verbosity argument to parser.

    Parameters
    ----------
    parser : :class:`argparse.ArgumentParser`
        The parser to which to add a verbosity argument.
    default : int
        The default level, defaults to 0.

    Notes
    -----
    The argument is '-v' or '--verbosity'.
    Add multiple '-v' arguments, e.g. '-vv' or '-vvv' to
    increase the level of verbosity.

    """
    parser.add_argument(
        "-v",
        "--verbosity",
        help="Set level of verbosity.",
        action="count",
        default=default,
    )


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="This function is obsolete.",
)
def add_verbosity_action_argument(parser, default=0):
    """Add a verbosity argument to parser.

    Parameters
    ----------
    parser : :class:`argparse.ArgumentParser`
        The parser to which to add a verbosity argument.
    default :
        The default level, defaults to 0.

    Notes
    -----
    The argument is '-v'.
    Add multiple '-v' arguments, e.g. '-vv' or '-vvv' to
    increase the level of verbosity.

    """
    parser.add_argument(
        "-v",
        default=0,
        nargs="?",
        action=VerbosityLoggingConfigAction,
        dest="verbosity",
    )


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="This function is obsolete.",
)
def set_verbosity_level(verbosity, default=None, increment=10):
    """Set the verbosity level as a function of an integer level.

    Parameters
    ----------
    verbosity :
        The verbosity level as integer.
    default :
        The default verbosity level, defaults to logging.ERROR.
    increment :
        (Default value = 10).

    """
    if default is None:
        default = logging.ERROR
    logging.basicConfig(level=default - increment * verbosity)


# this class is deprecated
class VerbosityAction(argparse.Action):  # noqa: D101, E261
    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="This class is obsolete",
    )
    def __call__(self, parser, args, values, option_string=None):  # noqa: D102, E261
        if values is None:
            values = "1"
        try:
            values = int(values)
        except ValueError:
            values = values.count("v") + 1
        setattr(args, self.dest, values)


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The VerbosityLoggingConfigAction class is obsolete.",
)
class VerbosityLoggingConfigAction(VerbosityAction):  # noqa: D101, E261
    def __call__(self, parser, args, values, option_string=None):  # noqa:D102, E261
        super().__call__(parser, args, values, option_string)
        v_level = getattr(args, self.dest)
        set_verbosity_level(v_level)


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The EmptyIsTrue class is obsolete.",
)
class EmptyIsTrue(argparse.Action):  # noqa: D101, E261
    def __call__(
        self, parser, namespace, values, option_string=None
    ):  # noqa: D102, E261
        if values is None:
            values = True
        setattr(namespace, self.dest, values)


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The SmartFormatter class is obsolete.",
)
class SmartFormatter(argparse.HelpFormatter):  # noqa: D101, E261
    def _split_lines(self, text, width):

        if text.startswith("R|"):
            return text[2:].splitlines()
        return argparse.HelpFormatter._split_lines(self, text, width)


def walkdepth(path, depth=0):
    """Transverse the directory starting from path.

    Parameters
    ----------
    path :str
        Directory passed to walk (transverse from).
    depth : int
        (Default value = 0)

    Yields
    ------
    str
        When depth==0.
    tuple
        When depth>0.

    Raises
    ------
    ValueError
        When the value of depth is negative.
    OSError
        When path is not name of a directory.

    """
    if depth == 0:
        yield from os.walk(path)
    elif depth > 0:
        path = path.rstrip(os.path.sep)
        if not os.path.isdir(path):
            raise OSError(f"Not a directory: '{path}'.")
        num_sep = path.count(os.path.sep)
        for root, dirs, files in os.walk(path):
            yield root, dirs, files
            num_sep_this = root.count(os.path.sep)
            if num_sep + depth <= num_sep_this:
                del dirs[:]
    else:
        raise ValueError("The value of depth must be non-negative.")


def _mkdir_p(path):
    """Make a new directory, or do nothing if the directory already exists.

    Parameters
    ----------
    path : str
        New directory name.

    """
    # Performance: `isdir` is fast and eliminates the rest of os.makedirs
    # if the path already exists. Typically this function is called in cases
    # where the path already exists. If the path is a file, this check returns
    # False and allows os.makedirs to raise FileExistsError as usual.
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def split_and_print_progress(iterable, num_chunks=10, write=None, desc="Progress: "):
    """Split the progress and prints it.

    Parameters
    ----------
    iterable : list
        List of values to be chunked.
    num_chunks : int
        Number of chunks to split the given iterable (Default value = 10).
    write :
        Logging level used to log messages (Default value = None).
    desc : str
        Prefix of message to log (Default value = 'Progress: ').

    Yields
    ------
    iterable

    Raises
    ------
    AssertionError
        If num_chunks <= 0.

    """
    if write is None:
        write = print
    assert num_chunks > 0
    if num_chunks > 1:
        N = len(iterable)
        len_chunk = int(N / num_chunks)
        intervals = []
        show_est = False
        for i in range(num_chunks - 1):
            if i:
                msg = f"{desc}{100 * i / num_chunks:3.0f}%"
                if intervals:
                    mean_interval = sum(intervals) / len(intervals)
                    est_remaining = int(mean_interval * (num_chunks - i))
                    if est_remaining > 10 or show_est:
                        show_est = True
                        msg += f" (ETR: {timedelta(seconds=est_remaining)}h)"
                write(msg)
            start = time()
            yield iterable[i * len_chunk : (i + 1) * len_chunk]
            intervals.append(time() - start)
        yield iterable[(i + 1) * len_chunk :]
        write(f"{desc}100%")
    else:
        yield iterable


@contextmanager
def _extract(filename):
    """Extract zipfile and tarfile.

    Parameters
    ----------
    filename : str
        Name of zipfile/tarfile to extract.

    Yields
    ------
    str
        Path to the extracted directory.

    Raises
    ------
    RuntimeError
        When the provided file is neither a zipfile nor a tarfile.

    """
    with TemporaryDirectory() as tmpdir:
        if zipfile.is_zipfile(filename):
            with zipfile.ZipFile(filename) as file:
                file.extractall(tmpdir)
                yield tmpdir
        elif tarfile.is_tarfile(filename):
            with tarfile.open(filename) as file:
                file.extractall(path=tmpdir)
                yield tmpdir
        else:
            raise RuntimeError(f"Unknown file type: '{filename}'.")


def _dotted_dict_to_nested_dicts(dotted_dict, delimiter_nested="."):
    """Convert dotted keys in the state point dict to a nested dict.

    Parameters
    ----------
    dotted_dict : dict
        A mapping with dots/delimiter_nested in its keys, e.g. {'a.b': 'c'}.
    delimiter_nested : str
        A string delimiter between keys, defaults to '.'.

    Returns
    -------
    dict
        A mapping instance with nested dicts, e.g. {'a': {'b': 'c'}}.

    """
    nested_dict = {}
    for key, value in dotted_dict.items():
        tokens = key.split(delimiter_nested)
        if len(tokens) > 1:
            tmp = nested_dict.setdefault(tokens[0], {})
            for token in tokens[1:-1]:
                tmp = tmp.setdefault(token, {})
            tmp[tokens[-1]] = value
        else:
            nested_dict[tokens[0]] = value
    return nested_dict


class _hashable_dict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


def _to_hashable(obj):
    """Create a hash of passed type.

    Parameters
    ----------
    obj
        Object to create a hashable version of. Lists are converted
        to tuples, and hashes are defined for dicts.

    Returns
    -------
    Hash created for obj.

    """
    if type(obj) is list:
        return tuple(_to_hashable(_) for _ in obj)
    elif type(obj) is dict:
        return _hashable_dict(obj)
    else:
        return obj


def _encode_tree(x):
    """Encode if type of x is list.

    Parameters
    ----------
    x :
        type to encode.

    Returns
    -------
    Hashable version of ``x``.

    """
    if type(x) is list:
        return _to_hashable(x)
    else:
        return x


def _nested_dicts_to_dotted_keys(t, encode=_encode_tree, key=None):
    """Generate tuples of key in dotted string format and value from nested dict.

    Parameters
    ----------
    t : dict
        A mapping instance with nested dicts, e.g. {'a': {'b': 'c'}}.
    encode :
        By default, values are encoded to be hashable. Use ``None`` to skip encoding.
    key : str
        Key of root at current point in the recursion, used to
        build up nested keys in the top-level dict through
        multiple recursive calls (Default value = None).

    Yields
    ------
    tuple
        Tuples of dotted key and values e.g. ('a.b', 'c')

    """
    if encode is not None:
        t = encode(t)
    if isinstance(t, Mapping):
        if t:
            for k in t:
                k_ = k if key is None else ".".join((key, k))
                yield from _nested_dicts_to_dotted_keys(t[k], encode=encode, key=k_)
        elif key is not None:
            yield key, t
    else:
        yield key, t


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

    index = [{"_id": job.id, "sp": job.sp()} for job in jobs]
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


def _check_path_function(jobs, path_spec, path_function):
    """Validate that path function specifies a 1-1 mapping.

    Parameters
    ----------
    jobs : iterable of :class:`~signac.contrib.job.Job`
        A sequence of jobs (instances of :class:`~signac.contrib.job.Job`).
    path_spec : str
        The path string that generated the path_function. Displayed in the error message.
    path_function : callable
        A callable path generating function.

    Raises
    -----
    RuntimeError
        If paths generated with given path function are not unique.

    """
    # quick check first
    links = {path_function(job) for job in jobs}
    if len(links) != len(jobs):
        # report all mismatches
        links = set()
        for job in jobs:
            job_path = path_function(job)
            if job_path in links:
                logger.debug(f"Generated path '{job_path}' is not unique.")
            else:
                links.add(job_path)
        raise RuntimeError(
            f"The path specification {path_spec} would result in duplicate links. "
            "See the debug log for the list. The easiest way to fix "
            "this is to append the job id to the path "
            f"specification like '{os.path.join(str(path_spec), 'id', '{job.id}')}'."
        )


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
        # Detect keys that are already provided as part of the path specifier
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

        # Check that the user-specified path generates a 1-1 mapping
        _check_path_function(jobs, path_spec=path, path_function=path_function)

    else:
        raise ValueError(
            "The path argument must either be `None`, `False`, or of type `str`."
        )

    return path_function
