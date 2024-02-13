# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The signac Project and JobsCursor classes."""

import errno
import gzip
import json
import logging
import os
import re
import shutil
import time
import warnings
from collections import defaultdict
from collections.abc import Iterable
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from itertools import groupby
from multiprocessing.pool import ThreadPool
from tempfile import TemporaryDirectory
from threading import RLock

from synced_collections.backends.collection_json import BufferedJSONAttrDict

from ._config import (
    _Config,
    _get_project_config_fn,
    _load_config,
    _locate_config_dir,
    _raise_if_older_schema,
    _read_config_file,
)
from ._search_indexer import _SearchIndexer
from ._utility import _mkdir_p, _nested_dicts_to_dotted_keys
from .errors import (
    DestinationExistsError,
    IncompatibleSchemaVersion,
    JobsCorruptedError,
    WorkspaceError,
)
from .filterparse import _add_prefix, _root_keys, parse_filter
from .h5store import H5StoreManager
from .job import Job, calc_id
from .schema import ProjectSchema
from .sync import sync_projects
from .version import SCHEMA_VERSION, __version__

logger = logging.getLogger(__name__)

JOB_ID_LENGTH = 32
JOB_ID_REGEX = re.compile(f"[a-f0-9]{{{JOB_ID_LENGTH}}}")


def _split_and_print_progress(iterable, num_chunks=10, write=None, desc="Progress: "):
    """Split the progress and prints it.

    Parameters
    ----------
    iterable : list
        List of values to be chunked.
    num_chunks : int, optional
        Number of chunks to split the given iterable (Default value = 10).
    write : callable, optional
        Callable used to log messages. If None, ``print`` is used (Default
        value = None).
    desc : str, optional
        Prefix of message to log (Default value = 'Progress: ').

    Yields
    ------
    iterable

    Raises
    ------
    ValueError
        If num_chunks <= 0.

    """
    if num_chunks <= 0:
        raise ValueError("num_chunks must be a positive integer.")
    if write is None:
        write = print
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
            start = time.time()
            yield iterable[i * len_chunk : (i + 1) * len_chunk]
            intervals.append(time.time() - start)
        yield iterable[(i + 1) * len_chunk :]
        write(f"{desc}100%")
    else:
        yield iterable


class _ProjectConfig(_Config):
    r"""Extends the project config to make it immutable.

    Parameters
    ----------
    \*args :
        Forwarded to :class:`~signac.config.Config` constructor.
    \*\*kwargs :
        Forwarded to :class:`~signac.config.Config` constructor.

    """

    def __init__(self, *args, **kwargs):
        self._mutable = True
        super().__init__(*args, **kwargs)
        self._mutable = False

    def __setitem__(self, key, value):
        if not self._mutable:
            raise ValueError("The project configuration is immutable.")
        return super().__setitem__(key, value)


class Project:
    """The handle on a signac project.

    A :class:`Project` may only be constructed in a directory that is already a
    signac project, i.e. a directory in which :func:`~signac.init_project` has
    already been run. To search upwards in the folder hierarchy until a project
    is found, instead invoke :func:`~signac.get_project` or
    :meth:`Project.get_project`.

    Parameters
    ----------
    path : str, optional
        The project directory. By default, the current working directory
        (Default value = None).

    """

    FN_DOCUMENT = "signac_project_document.json"
    "The project's document filename."

    KEY_DATA = "signac_data"
    "The project's datastore key."

    FN_CACHE = os.sep.join((".signac", "statepoint_cache.json.gz"))
    "The default filename for the state point cache file."

    _use_pandas_for_html_repr = True  # toggle use of pandas for html repr

    def __init__(self, path=None):
        if path is None:
            path = os.getcwd()
        if not os.path.isfile(_get_project_config_fn(path)):
            _raise_if_older_schema(path)
            raise LookupError(
                f"Unable to find project at path '{os.path.abspath(path)}'."
            )

        # Project constructor does not search upward, so the provided path must
        # be a project directory.
        config = _load_config(path)
        self._config = _ProjectConfig(config)
        self._lock = RLock()

        # Ensure that the project's data schema is supported.
        self._check_schema_compatibility()

        # Prepare project document
        self._document = None

        # Prepare project H5StoreManager
        self._stores = None

        # Prepare project and workspace directories.
        # os.path is used instead of pathlib.Path for performance.
        self._path = os.path.abspath(path)
        self._workspace = os.path.join(self._path, "workspace")

        # Prepare workspace directory.
        if not os.path.isdir(self.workspace):
            try:
                _mkdir_p(self.workspace)
            except OSError:
                logger.error(
                    "Error occurred while trying to create workspace directory for project at path "
                    f"{self.path}."
                )
                raise

        # Internal state point cache
        # Note that the state point cache is a superset of the jobs in the
        # project, and its contents cannot be invalidated. The cached mapping
        # of "id: statepoint" is valid even after a job has been removed, and
        # can be used to re-open a job by id as long as that id remains in the
        # cache.
        self._sp_cache = {}
        self._sp_cache_read = False
        self._sp_cache_misses = 0
        self._sp_cache_warned = False
        self._sp_cache_miss_warning_threshold = self.config.get(
            "statepoint_cache_miss_warning_threshold", 500
        )

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self.path)})"

    def _repr_html_(self):
        """Project details in HTML format for use in IPython environment.

        Returns
        -------
        str
            HTML containing project details.

        """
        return (
            "<p>"
            + f"<strong>Project:</strong> {self.path}<br>"
            + f"<strong>Workspace:</strong> {self.workspace}<br>"
            + f"<strong>Size:</strong> {len(self)}"
            + "</p>"
            + self.find_jobs()._repr_html_jobs()
        )

    def __eq__(self, other):
        return repr(self) == repr(other)

    @property
    def config(self):
        """Get project's configuration.

        The configuration is immutable once the Project is constructed. To
        modify a project configuration, use the command line or edit the
        configuration file directly.

        See :ref:`signac config <signac-cli-config>` for related command line tools.

        Returns
        -------
        :class:`~signac.project._ProjectConfig`
            Dictionary containing project's configuration.

        """
        return self._config

    @property
    def path(self):
        """str: The path to the project directory."""
        return self._path

    @property
    def workspace(self):
        """str: The project's workspace directory."""
        return self._workspace

    def _check_schema_compatibility(self):
        """Check whether this project's data schema is compatible with this version.

        Raises
        ------
        :class:`~signac.errors.IncompatibleSchemaVersion`
            If the schema version is incompatible.

        """
        schema_version = SCHEMA_VERSION
        config_schema_version = int(self.config["schema_version"])
        if config_schema_version > schema_version:
            # Project config schema version is newer and therefore not supported.
            raise IncompatibleSchemaVersion(
                "The signac schema version used by this project is '{}', but signac {} "
                "only supports up to schema version '{}'. Try updating signac.".format(
                    config_schema_version, __version__, schema_version
                )
            )
        elif config_schema_version < schema_version:
            raise IncompatibleSchemaVersion(
                "The signac schema version used by this project is '{}', but signac {} "
                "requires schema version '{}'. Please use 'python -m signac migrate' to "
                "irreversibly migrate this project's schema to the supported "
                "version.".format(config_schema_version, __version__, schema_version)
            )
        else:  # identical and therefore compatible
            logger.debug(
                f"The project's schema version {config_schema_version} is supported."
            )

    def min_len_unique_id(self):
        """Determine the minimum length required for a job id to be unique.

        This method's runtime scales with the number of jobs in the
        workspace.

        Returns
        -------
        int
            Minimum string length of a unique job identifier.

        """
        job_ids = list(self._find_job_ids())
        tmp = set()
        for i in range(JOB_ID_LENGTH):
            tmp.clear()
            for id_ in job_ids:
                if id_[:i] in tmp:
                    break
                else:
                    tmp.add(id_[:i])
            else:
                break
        return i

    def fn(self, filename):
        """Prepend a filename with the project path.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        str
            The absolute path of the file.

        """
        return os.path.join(self.path, filename)

    def isfile(self, filename):
        """Check if a filename exists in the project path.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        bool
            True if filename exists in the project path.

        """
        return os.path.isfile(self.fn(filename))

    @property
    def document(self):
        """Get document associated with this project.

        Returns
        -------
        MutableMapping
            The project document. Supports attribute-based access to dict keys.

        """
        with self._lock:
            if self._document is None:
                fn_doc = os.path.join(self.path, self.FN_DOCUMENT)
                self._document = BufferedJSONAttrDict(
                    filename=fn_doc, write_concern=True
                )
        return self._document

    @document.setter
    def document(self, new_doc):
        """Setter method for document associated with this project.

        Parameters
        ----------
        new_doc : dict
            The new project document.

        """
        with self._lock:
            self.document.reset(new_doc)

    @property
    def doc(self):
        """Get document associated with this project.

        Alias for :meth:`~signac.Project.document`.

        Returns
        -------
        MutableMapping
            The project document. Supports attribute-based access to dict keys.

        """
        return self.document

    @doc.setter
    def doc(self, new_doc):
        """Setter method for document associated with this project.

        Parameters
        ----------
        new_doc : dict
            The new project document.

        """
        self.document = new_doc

    @property
    def stores(self):
        """Get HDF5 stores associated with this project.

        Use this property to access an HDF5 file within the project
        directory using the :py:class:`~.H5Store` dict-like interface.

        This is an example for accessing an HDF5 file called ``'my_data.h5'``
        within the project directory:

        .. code-block:: python

            project.stores['my_data']['array'] = np.random((32, 4))

        This is equivalent to:

        .. code-block:: python

            H5Store(project.fn('my_data.h5'))['array'] = np.random((32, 4))

        Both the `project.stores` and the `H5Store` itself support attribute
        access. The above example could therefore also be expressed as:

        .. code-block:: python

            project.stores.my_data.array = np.random((32, 4))

        Returns
        -------
        :class:`~signac.H5StoreManager`
            The HDF5 store manager for this project.

        """
        with self._lock:
            if self._stores is None:
                self._stores = H5StoreManager(self.path)
        return self._stores

    @property
    def data(self):
        """Get data associated with this project.

        This property should be used for large array-like data, which can't be
        stored efficiently in the project document. For examples and usage, see
        `Centralized Project Data
        <https://docs.signac.io/en/latest/projects.html#centralized-project-data>`_.

        Equivalent to:

        .. code-block:: python

            return project.stores['signac_data']

        See Also
        --------
        :class:`~signac.H5Store` : Usage examples.

        Returns
        -------
        :class:`~signac.H5Store`
            An HDF5-backed datastore.

        """
        return self.stores[self.KEY_DATA]

    @data.setter
    def data(self, new_data):
        """Setter method for data associated with this project.

        Parameters
        ----------
        new_data : :class:`~signac.H5Store`
            An HDF5-backed datastore.

        """
        self.stores[self.KEY_DATA] = new_data

    def open_job(self, statepoint=None, id=None):
        """Get a job handle associated with a state point.

        This method returns the job instance associated with
        the given state point or job id.
        Opening a job by a valid state point never fails.
        Opening a job by id requires a lookup of the state point
        from the job id, which may fail if the job was not
        previously initialized.

        Parameters
        ----------
        statepoint : dict, optional
            The job's unique set of state point parameters (Default value = None).
        id : str, optional
            The job id (Default value = None).

        Returns
        -------
        :class:`~signac.job.Job`
            The job instance.

        Raises
        ------
        KeyError
            If the attempt to open the job by id fails.
        LookupError
            If the attempt to open the job by an abbreviated id returns more
            than one match.

        """
        if not self._sp_cache_read:
            # Read the cache from disk on the first call.
            self._read_cache()
            self._sp_cache_read = True

        if statepoint is None and id is None:
            raise ValueError("Must provide statepoint or id.")
        elif statepoint is not None and id is not None:
            raise ValueError("Either statepoint or id must be provided, but not both.")
        elif statepoint is not None:
            # Second best case (Job will update self._sp_cache on init)
            return Job(project=self, statepoint=deepcopy(statepoint))
        try:
            # Optimal case (id is in the state point cache)
            return Job(project=self, statepoint=self._sp_cache[id], id_=id)
        except KeyError:
            # Worst case: no state point was provided and the state point cache
            # missed. The Job will register itself in self._sp_cache when the
            # state point is accessed.
            if len(id) < JOB_ID_LENGTH:
                # Resolve partial job ids (first few characters) into a full job id
                job_ids = self._find_job_ids()
                matches = [id_ for id_ in job_ids if id_.startswith(id)]
                if len(matches) == 1:
                    id = matches[0]
                elif len(matches) > 1:
                    raise LookupError(id)
                else:
                    # By elimination, len(matches) == 0
                    raise KeyError(id)
            elif not self._contains_job_id(id):
                # id does not exist in the project data space
                raise KeyError(id)
            return Job(project=self, id_=id, directory_known=True)

    def _job_dirs(self):
        """Generate ids of jobs in the workspace.

        Yields
        ------
        str
            Job id.

        """
        try:
            for d in os.listdir(self.workspace):
                if JOB_ID_REGEX.match(d):
                    yield d
        except OSError as error:
            if error.errno == errno.ENOENT:
                if os.path.islink(self.workspace):
                    raise WorkspaceError(
                        f"The link '{self.workspace}' pointing to the workspace is broken."
                    )
                elif not os.path.isdir(os.path.dirname(self.workspace)):
                    logger.warning(
                        "The path to the workspace directory "
                        "('{}') does not exist.".format(os.path.dirname(self.workspace))
                    )
                else:
                    logger.info(
                        f"The workspace directory '{self.workspace}' does not exist!"
                    )
            else:
                logger.error(
                    f"Unable to access the workspace directory '{self.workspace}'."
                )
                raise WorkspaceError(error)

    def __len__(self):
        # We simply count the the number of valid directories and avoid building a list
        # for improved performance.
        i = 0
        for i, _ in enumerate(self._job_dirs(), 1):
            pass
        return i

    def _contains_job_id(self, job_id):
        """Determine whether a job id is in the project's data space.

        Parameters
        ----------
        job_id : str
            The job id to test for initialization.

        Returns
        -------
        bool
            True if the job id is initialized for this project.

        """
        # Performance-critical path. We can rely on the project workspace and
        # job id to be well-formed, so just use str.join with os.sep instead of
        # os.path.join for speed.
        return os.path.exists(os.sep.join((self.workspace, job_id)))

    def __contains__(self, job):
        """Determine whether a job is in the project's data space.

        Parameters
        ----------
        job : :class:`~signac.job.Job`
            The job to test for initialization.

        Returns
        -------
        bool
            True if the job is initialized for this project.

        """
        return self._contains_job_id(job.id)

    def detect_schema(self, exclude_const=False, subset=None):
        """Detect the project's state point schema.

        See :ref:`signac schema <signac-cli-schema>` for the command line equivalent.

        Parameters
        ----------
        exclude_const : bool, optional
            Exclude all state point keys that are shared by all jobs within this project
            (Default value = False).
        subset : sequence[Job or str], optional
            A sequence of jobs or job ids specifying a subset over which the state point
            schema should be detected (Default value = None).

        Returns
        -------
        :class:`~signac.schema.ProjectSchema`
            The detected project schema.

        """
        from .schema import _build_job_statepoint_index

        index = _SearchIndexer(self._build_index(include_job_document=False))
        if subset is not None:
            subset = {str(s) for s in subset}.intersection(index.keys())
            index = _SearchIndexer((id_, index[id_]) for id_ in subset)
        statepoint_index = _build_job_statepoint_index(
            exclude_const=exclude_const, index=index
        )

        def _collect_by_type(values):
            """Construct a mapping of types to a set of elements drawn from the input values."""
            values_by_type = defaultdict(set)
            for v in values:
                values_by_type[type(v)].add(v)
            return values_by_type

        return ProjectSchema(
            {key: _collect_by_type(value) for key, value in statepoint_index}
        )

    def _find_job_ids(self, filter=None):
        """Find the job ids of all jobs matching the filter.

        The filter argument must be a JSON-serializable Mapping of key-value
        pairs. The ``filter`` argument can search against both job state points
        and job documents. See
        https://docs.signac.io/en/latest/query.html#query-namespaces
        for a description of supported queries.

        Parameters
        ----------
        filter : Mapping, optional
            A mapping of key-value pairs used for the query (Default value =
            None).

        Returns
        -------
        list
            The ids of all jobs matching the filter.

        Raises
        ------
        TypeError
            If the filters are not JSON serializable.
        ValueError
            If the filters are invalid.

        """
        if not filter:
            return list(self._job_dirs())
        filter = dict(parse_filter(_add_prefix(filter)))
        index = _SearchIndexer(
            self._build_index(include_job_document="doc" in _root_keys(filter))
        )
        return list(index.find(filter))

    def find_jobs(self, filter=None):
        """Find all jobs in the project's workspace.

        The filter argument must be a JSON-serializable Mapping of key-value
        pairs. The ``filter`` argument can search against both job state points
        and job documents. See
        https://docs.signac.io/en/latest/query.html#query-namespaces
        for a description of supported queries.

        See :ref:`signac find <signac-cli-find>` for the command line equivalent.

        .. tip::

            To find a single job given a state point, use `open_job` with O(1) cost.

        .. tip::

            To find many groups of jobs, use your own code to loop through the project
            once and build multiple matching lists.

        .. warning::

            `find_jobs` costs O(N) each time it is called. It applies the filter to
            every job in the workspace.

        Parameters
        ----------
        filter : Mapping, optional
            A mapping of key-value pairs used for the query (Default value =
            None).

        Returns
        -------
        :class:`~signac.project.JobsCursor`
            JobsCursor of jobs matching the provided filter.

        Raises
        ------
        TypeError
            If the filters are not JSON serializable.
        ValueError
            If the filters are invalid.

        """
        if not filter:
            filter = {}
        return JobsCursor(self, dict(parse_filter(filter)))

    def __iter__(self):
        return iter(self.find_jobs())

    def groupby(self, key=None, default=None):
        """Group jobs according to one or more state point or document parameters.

        Prepend the key with 'sp.' or 'doc.' to specify the query namespace. If no prefix
        is specified, group by state point key.

        This method can be called on any :class:`~signac.project.JobsCursor` such as
        the one returned by :meth:`~signac.Project.find_jobs` or by iterating over a
        project.

        Examples
        --------
        .. code-block:: python

            # Group jobs by state point parameter 'a'.
            for key, group in project.groupby('a'):
                print(key, list(group))

            # Group jobs by document value 'a'.
            for key, group in project.groupby('doc.a'):
                print(key, list(group))

            # Group jobs by jobs.sp['a'] and job.document['b']
            for key, group in project.groupby(('a', 'doc.b')):
                print(key, list(group))

            # Find jobs where job.sp['a'] is 1 and group them
            # by job.sp['b'] and job.sp['c'].
            for key, group in project.find_jobs({'a': 1}).groupby(('b', 'c')):
                print(key, list(group))

            # Group by job.sp['d'] and job.document['count'] using a lambda.
            for key, group in project.groupby(
                lambda job: (job.sp['d'], job.document['count'])
            ):
                print(key, list(group))

        If `key` is None, jobs are grouped by id, placing one job into each group.

        If `default` is None, only jobs with the `key` defined will be grouped.
        Jobs without the `key` will be filtered out and not included in any
        group.

        Parameters
        ----------
        key : str, iterable, or callable, optional
            The grouping key(s) passed as a string,
            iterable of strings, or a callable that will be passed one
            argument, the job (Default value = None).
        default : object, optional
            A default value to be used when a given key is not
            present. The value must be sortable and is only used if not None
            (Default value = None).

        Yields
        ------
        key :
            Key identifying this group.
        group : iterable of Jobs
            Iterable of `Job` instances matching this group.

        """
        yield from self.find_jobs().groupby(key, default=default)

    def to_dataframe(self, *args, **kwargs):
        r"""Export the project metadata to a pandas :class:`~pandas.DataFrame`.

        The arguments to this function are forwarded to
        :meth:`~signac.project.JobsCursor.to_dataframe`.

        Parameters
        ----------
        \*args :
            Forwarded to :meth:`~signac.project.JobsCursor.to_dataframe`.
        \*\*kwargs :
            Forwarded to :meth:`~signac.project.JobsCursor.to_dataframe`.

        Returns
        -------
        :class:`~pandas.DataFrame`

        """
        return self.find_jobs().to_dataframe(*args, **kwargs)

    def _register(self, id_, statepoint):
        """Register the job state point in the project state point cache.

        Parameters
        ----------
        id_ : str
            A job identifier.
        statepoint : dict
            A validated job state point.

        """
        self._sp_cache[id_] = statepoint

    def _get_statepoint_from_workspace(self, job_id, validate=True):
        """Attempt to read the state point from the workspace.

        Parameters
        ----------
        job_id : str
            Identifier of the job.
        validate : bool
            When True, validate that any state point read from disk matches the job_id.

        Raises
        ------
        :class:`signac.errors.JobsCorruptedError`
            When one or more jobs are identified as corrupted.

        """
        # Performance-critical path. We can rely on the project workspace, job
        # id, and state point file name to be well-formed, so just use str.join
        # with os.sep instead of os.path.join for speed.
        fn_statepoint = os.sep.join((self.workspace, job_id, Job.FN_STATE_POINT))
        try:
            with open(fn_statepoint, "rb") as statepoint_file:
                statepoint = json.loads(statepoint_file.read().decode())
                if validate and calc_id(statepoint) != job_id:
                    raise JobsCorruptedError([job_id])

                return statepoint
        except (OSError, ValueError) as error:
            if os.path.isdir(os.sep.join((self.workspace, job_id))):
                logger.error(
                    "Error while trying to access state point file of job '{}': '{}'.".format(
                        job_id, error
                    )
                )
                raise JobsCorruptedError([job_id])
            raise KeyError(job_id)

    def _get_statepoint(self, job_id, validate=True):
        """Get the state point associated with a job id.

        The state point is retrieved from the internal cache, from
        the workspace or from a state points file.

        Parameters
        ----------
        job_id : str
            A job id to get the state point for.
        validate : bool
            When True, validate that any state point read from disk matches the job_id.


        Returns
        -------
        dict
            The state point corresponding to job_id.

        Raises
        ------
        KeyError
            If the state point associated with job_id could not be found.
        :class:`signac.errors.JobsCorruptedError`
            If the state point file corresponding to job_id is inaccessible or
            corrupted.

        """
        if not self._sp_cache_read:
            # Read the cache from disk on the first call.
            self._read_cache()
            self._sp_cache_read = True
        try:
            # State point cache hit
            return self._sp_cache[job_id]
        except KeyError:
            # State point cache missed
            self._sp_cache_misses += 1
            if (
                not self._sp_cache_warned
                and self._sp_cache_misses > self._sp_cache_miss_warning_threshold
            ):
                logger.debug(
                    "High number of state point cache misses. Consider "
                    "updating the cache by running `signac update-cache`."
                )
                self._sp_cache_warned = True
            statepoint = self._get_statepoint_from_workspace(job_id, validate)
            # Update the project's state point cache from this cache miss
            self._sp_cache[job_id] = statepoint
        return statepoint

    def create_linked_view(self, prefix=None, job_ids=None, path=None):
        """Create or update a persistent linked view of the selected data space.

        Similar to :meth:`~signac.Project.export_to`, this function expands the data space
        for the selected jobs, but instead of copying data will create symbolic links to the
        individual job directories. This is primarily useful for browsing through
        the data space using a file-browser with human-interpretable directory paths.

        By default, the paths of the view will be based on variable state point keys as part
        of the *implicit* schema of the selected jobs that we create the view for. For example,
        creating a linked view for a data space with schema

        .. code-block:: python

            >>> print(project.detect_schema())
            {
             'foo': 'int([0, 1, 2, ..., 8, 9], 10)',
            }

        by calling ``project.create_linked_view('my_view')`` will look similar to:

        .. code-block:: bash

            my_view/foo/0/job -> workspace/b8fcc6b8f99c56509eb65568922e88b8
            my_view/foo/1/job -> workspace/b6cd26b873ae3624653c9268deff4485
            ...

        It is possible to control the paths using the ``path`` argument, which behaves in
        the exact same manner as the equivalent argument for :meth:`~signac.Project.export_to`.

        .. note::
            The behavior of this function is almost equivalent to
            ``project.export_to('my_view', copytree=os.symlink)`` with the
            major difference that view hierarchies are actually *updated*,
            meaning that invalid links are automatically removed.

        See :ref:`signac view <signac-cli-view>` for the command line equivalent.

        Parameters
        ----------
        prefix : str, optional
            The path where the linked view will be created or updated (Default value = None).
        job_ids : iterable, optional
            If None (the default), create the view for the complete data space,
            otherwise only for this iterable of job ids.
        path : str or callable, optional
            The path (function) used to structure the linked data space (Default value = None).

        Returns
        -------
        dict
            A dictionary that maps the source directory paths to the linked
            directory paths.

        """
        from .linked_view import create_linked_view

        return create_linked_view(self, prefix, job_ids, path)

    def clone(self, job, copytree=None):
        """Clone job into this project.

        Create an identical copy of job within this project.

        See :ref:`signac clone <signac-cli-clone>` for the command line equivalent.

        Parameters
        ----------
        job : :class:`~signac.job.Job`
            The job to copy into this project.
        copytree : callable, optional
            The function used for copying directory tree structures. Uses
            :func:`shutil.copytree` if ``None`` (Default value = None). The function
            requires that the target is a directory.

        Returns
        -------
        :class:`~signac.job.Job`
            The job instance corresponding to the copied job.

        Raises
        ------
        :class:`~signac.errors.DestinationExistsError`
            In case that a job with the same id is already
            initialized within this project.

        """
        if copytree is None:
            copytree = shutil.copytree
        dst = self.open_job(job.statepoint())
        try:
            copytree(job.path, dst.path)
        except OSError as error:
            if error.errno == errno.EEXIST:
                raise DestinationExistsError(dst)
            elif error.errno == errno.ENOENT:
                raise ValueError("Source job not initialized.")
            else:
                raise
        return dst

    def sync(
        self,
        other,
        strategy=None,
        exclude=None,
        doc_sync=None,
        selection=None,
        **kwargs,
    ):
        r"""Synchronize this project with the other project.

        Try to clone all jobs from the other project to this project.
        If a job is already part of this project, try to synchronize the job
        using the optionally specified strategies.

        See :ref:`signac sync <signac-cli-sync>` for the command line equivalent.

        Parameters
        ----------
        other : :class:`~signac.Project`
            The other project to synchronize this project with.
        strategy : callable, optional
            A synchronization strategy for file conflicts. If no strategy is provided, a
            :class:`~signac.errors.SyncConflict` exception will be raised upon conflict
            (Default value = None).
        exclude : str, optional
            A filename exclude pattern. All files matching this pattern will be
            excluded from synchronization (Default value = None).
        doc_sync : attribute or callable from :py:class:`~signac.sync.DocSync`, optional
            A synchronization strategy for document keys. If this argument is None, by default
            no keys will be synchronized upon conflict (Default value = None).
        selection : sequence of :class:`~signac.job.Job` or job ids (str), optional
            Only synchronize the given selection of jobs (Default value = None).
        \*\*kwargs :
            This method also accepts the same keyword arguments as the
            :meth:`~signac.sync.sync_projects` function.

        Raises
        ------
        :class:`~signac.errors.DocumentSyncConflict`
            If there are conflicting keys within the project or job documents that cannot
            be resolved with the given strategy or if there is no strategy provided.
        :class:`~signac.errors.FileSyncConflict`
            If there are differing files that cannot be resolved with the given strategy
            or if no strategy is provided.
        :class:`~signac.errors.SchemaSyncConflict`
            In case that the check_schema argument is True and the detected state point
            schema of this and the other project differ.

        """
        return sync_projects(
            source=other,
            destination=self,
            strategy=strategy,
            exclude=exclude,
            doc_sync=doc_sync,
            selection=selection,
            **kwargs,
        )

    def export_to(self, target, path=None, copytree=None):
        """Export all jobs to a target location, such as a directory or a (compressed) archive file.

        Use this function in combination with :meth:`~signac.Project.find_jobs` to export only a
        select number of jobs, for example:

        .. code-block:: python

            project.find_jobs({'foo': 0}).export_to('foo_0.tar')

        The ``path`` argument enables users to control how exactly the exported data space is to be
        expanded. By default, the path-function will be based on the *implicit* schema of the
        exported jobs. For example, exporting jobs that all differ by a state point key *foo* with
        ``project.export_to('data/')``, the exported directory structure could look like this:

        .. code-block:: bash

            data/foo/0
            data/foo/1
            ...

        That would be equivalent to specifying ``path=lambda job: os.path.join('foo', job.sp.foo)``.

        Instead of a function, we can also provide a string, where fields for state point keys
        are automatically formatted. For example, the following two path arguments are equivalent:
        "foo/{foo}" and "foo/{job.sp.foo}".

        Any attribute of job can be used as a field here, so ``job.doc.bar``,
        ``job.id``, and ``job.ws`` can also be used as path fields.

        A special ``{{auto}}`` field allows us to expand the path automatically with state point
        keys that have not been specified explicitly. So, for example, one can provide
        ``path="foo/{foo}/{{auto}}"`` to specify that the path shall begin with ``foo/{foo}/``,
        but is then automatically expanded with all other state point key-value pairs. How
        key-value pairs are concatenated can be controlled *via* the format-specifier, so for
        example, ``path="{{auto:_}}"`` will generate a structure such as

        .. code-block:: bash

            data/foo_0
            data/foo_1
            ...

        Finally, providing ``path=False`` is equivalent to ``path="{job.id}"``.

        See Also
        --------
        :meth:`~signac.Project.import_from` :
            Previously exported or non-signac data spaces can be imported.

        :ref:`signac export <signac-cli-export>` :
            See signac export for the command line equivalent.

        Parameters
        ----------
        target : str
            A path to a directory to export to. The target can not already exist.
            Besides directories, possible targets are tar files (`.tar`), gzipped tar files
            (`.tar.gz`), zip files (`.zip`), bzip2-compressed files (`.bz2`),
            and xz-compressed files (`.xz`).
        path : str or callable, optional
            The path (function) used to structure the exported data space.
            This argument must either be a callable which returns a path (str) as a function
            of `job`, a string where fields are replaced using the job-state point dictionary,
            or `False`, which means that we just use the job-id as path.
            Defaults to the equivalent of ``{{auto}}``.
        copytree : callable, optional
            The function used for copying directory tree structures. Uses
            :func:`shutil.copytree` if ``None`` (Default value = None). The function
            requires that the target is a directory.

        Returns
        -------
        dict
            A dict that maps the source directory paths, to the target
            directory paths.

        """
        return self.find_jobs().export_to(target=target, path=path, copytree=copytree)

    def import_from(self, origin=None, schema=None, sync=None, copytree=None):
        """Import the data space located at origin into this project.

        This function will walk through the data space located at origin and will try to identify
        data space paths that can be imported as a job workspace into this project.

        The ``schema`` argument expects a function that takes a path argument and returns a state
        point dictionary. A default function is used when no argument is provided.
        The default schema function will simply look for state point files -- usually named
        ``signac_statepoint.json`` -- and then import all data located within that path into the job
        workspace corresponding to the specified state point.

        Alternatively the schema argument may be a string, that is converted into a schema function,
        for example: Providing ``foo/{foo:int}`` as schema argument means that all directories under
        ``foo/`` will be imported and their names will be interpreted as the value for ``foo``
        within the state point.

        .. tip::

            Use ``copytree=os.replace`` or ``copytree=shutil.move`` to move dataspaces on import
            instead of copying them.

            Warning: Imports can fail due to conflicts. Moving data instead of copying may
            therefore lead to inconsistent states and users are advised to apply caution.

        See Also
        --------
        :meth:`~signac.Project.export_to` : Export the project data space.

        :ref:`signac import <signac-cli-import>` :
            See signac import for the command line equivalent.

        Parameters
        ----------
        origin : str, optional
            The path to the data space origin, which is to be imported. This may be a path to
            a directory, a zip file, or a tarball archive (Default value = None).
        schema : callable, optional
            An optional schema function, which is either a string or a function that accepts a
            path as its first and only argument and returns the corresponding state point as dict.
            (Default value = None).
        sync : bool or dict, optional
            If ``True``, the project will be synchronized with the imported data space. If a
            dict of keyword arguments is provided, the arguments will be used for
            :meth:`~signac.Project.sync` (Default value = None).
        copytree : callable, optional
            The function used for copying directory tree structures. Uses
            :func:`shutil.copytree` if ``None`` (Default value = None). The function
            requires that the target is a directory.

        Returns
        -------
        dict
            A dict that maps the source directory paths to the target directory paths.

        """
        from .import_export import import_into_project

        if sync:
            with self.temporary_project() as tmp_project:
                ret = tmp_project.import_from(origin=origin, schema=schema)
                if sync is True:
                    self.sync(other=tmp_project)
                else:
                    self.sync(other=tmp_project, **sync)
                return ret

        return dict(
            import_into_project(
                origin=origin, project=self, schema=schema, copytree=copytree
            )
        )

    def check(self):
        """Check the project's workspace for corruption.

        Raises
        ------
        :class:`signac.errors.JobsCorruptedError`
            When one or more jobs are identified as corrupted.

        """
        corrupted = []
        logger.info("Checking workspace for corruption...")
        for job_id in self._find_job_ids():
            try:
                self._get_statepoint_from_workspace(job_id)
            except JobsCorruptedError as error:
                corrupted.extend(error.job_ids)
        if corrupted:
            logger.error(
                "At least one job appears to be corrupted. Call Project.repair() "
                "to try to fix errors."
            )
            raise JobsCorruptedError(corrupted)

    def repair(self, job_ids=None):
        """Attempt to repair the workspace after it got corrupted.

        This method will attempt to repair lost or corrupted job state point
        files using a state point cache.

        Parameters
        ----------
        job_ids : iterable[str], optional
            An iterable of job ids that should get repaired. Defaults to all jobs.

        Raises
        ------
        :class:`signac.errors.JobsCorruptedError`
            When one or more corrupted job could not be repaired.

        """
        if job_ids is None:
            job_ids = self._find_job_ids()

        # Load internal cache from all available external sources.
        self._read_cache()
        corrupted = []
        for job_id in job_ids:
            try:
                # First, check if we can look up the state point.
                statepoint = self._get_statepoint(job_id, validate=False)
                # Check if state point and id correspond.
                correct_id = calc_id(statepoint)
                if correct_id != job_id:
                    logger.warning(
                        "The job id of job '{}' is incorrect; "
                        "it should be '{}'.".format(job_id, correct_id)
                    )
                    invalid_wd = os.path.join(self.workspace, job_id)
                    correct_wd = os.path.join(self.workspace, correct_id)
                    try:
                        os.replace(invalid_wd, correct_wd)
                    except OSError as error:
                        logger.critical(
                            "Unable to fix location of job with "
                            " id '{}': '{}'.".format(job_id, error)
                        )
                        corrupted.append(job_id)
                        continue
                    else:
                        logger.info("Moved job to correct workspace.")

                job = self.open_job(statepoint)
            except KeyError:
                logger.critical(
                    f"Unable to look up state point for job with id '{job_id}'."
                )
                corrupted.append(job_id)
            else:
                try:
                    # Try to reinitialize the job (triggers state point file check).
                    job.init()
                except Exception as error:
                    logger.error(
                        "Error during initialization of job with "
                        "id '{}': '{}'.".format(job_id, error)
                    )
                    try:  # Attempt to fix the job state point file.
                        job.init(force=True)
                    except Exception as error2:
                        logger.critical(
                            f"Unable to force init job with id '{job_id}': '{error2}'."
                        )
                        corrupted.append(job_id)
        if corrupted:
            raise JobsCorruptedError(corrupted)

    def _build_index(self, include_job_document=False):
        """Generate a basic state point index.

        Parameters
        ----------
        include_job_document : bool, optional
            Whether to include the job document in the index (Default value =
            False).

        Yields
        ------
        job_id : str
            The job id.
        doc : dict
            Dictionary with keys ``sp`` containing the state point and ``doc``
            containing the job document if requested.

        """
        for job_id in self._find_job_ids():
            doc = {"sp": self._get_statepoint(job_id)}
            if include_job_document:
                try:
                    # Performance-critical path. We can rely on the project
                    # workspace, job id, and document file name to be
                    # well-formed, so just use str.join with os.sep instead of
                    # os.path.join for speed.
                    fn_document = os.sep.join((self.workspace, job_id, Job.FN_DOCUMENT))
                    with open(fn_document, "rb") as file:
                        doc["doc"] = json.loads(file.read().decode())
                except OSError as error:
                    if error.errno != errno.ENOENT:
                        raise
            yield job_id, doc

    def _update_in_memory_cache(self):
        """Update the in-memory state point cache to reflect the workspace."""
        logger.debug("Updating in-memory cache...")
        start = time.time()
        job_ids = set(self._job_dirs())
        cached_ids = set(self._sp_cache)
        to_add = job_ids.difference(cached_ids)
        to_remove = cached_ids.difference(job_ids)
        if to_add or to_remove:
            for id_ in to_remove:
                del self._sp_cache[id_]

            def _add(id_):
                self._sp_cache[id_] = self._get_statepoint_from_workspace(id_)

            to_add_chunks = _split_and_print_progress(
                iterable=list(to_add),
                num_chunks=max(1, min(100, int(len(to_add) / 1000))),
                write=logger.info,
                desc="Read metadata: ",
            )

            with ThreadPool() as pool:
                for chunk in to_add_chunks:
                    pool.map(_add, chunk)

            delta = time.time() - start
            logger.debug(f"Updated in-memory cache in {delta:.3f} seconds.")
            return to_add, to_remove
        else:
            logger.debug("In-memory cache is up to date.")

    def _remove_persistent_cache_file(self):
        """Remove the persistent cache file (if it exists)."""
        try:
            os.remove(self.fn(self.FN_CACHE))
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise error

    def update_cache(self):
        """Update the persistent state point cache.

        This function updates a persistent state point cache, which
        is stored in the project directory. Most data space operations,
        including iteration and filtering or selection are expected
        to be significantly faster after calling this function, especially
        for large data spaces.
        """
        logger.info("Update cache...")
        start = time.time()
        cache = self._read_cache()
        cached_ids = set(self._sp_cache)
        self._update_in_memory_cache()
        if cache is None or set(cache) != cached_ids:
            fn_cache = self.fn(self.FN_CACHE)
            fn_cache_tmp = fn_cache + "~"
            try:
                with gzip.open(fn_cache_tmp, "wb") as cachefile:
                    cachefile.write(json.dumps(self._sp_cache).encode())
            except OSError:  # clean-up
                try:
                    os.remove(fn_cache_tmp)
                except OSError:
                    pass
                raise
            else:
                os.replace(fn_cache_tmp, fn_cache)
            delta = time.time() - start
            logger.info(f"Updated cache in {delta:.3f} seconds.")
            return len(self._sp_cache)
        else:
            logger.info("Cache is up to date.")

    def _read_cache(self):
        """Read the persistent state point cache (if available)."""
        logger.debug("Reading cache...")
        start = time.time()
        try:
            with gzip.open(self.fn(self.FN_CACHE), "rb") as cachefile:
                cache = json.loads(cachefile.read().decode())
            self._sp_cache.update(cache)
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise
            logger.debug("No cache file found.")
        else:
            delta = time.time() - start
            logger.debug(f"Read cache in {delta:.3f} seconds.")
            return cache

    @contextmanager
    def temporary_project(self, dir=None):
        """Context manager for the initialization of a temporary project.

        The temporary project is by default created within the parent project's
        workspace to ensure that they share the same file system. This is an example
        for how this method can be used for the import and synchronization of
        external data spaces.

        .. code-block:: python

            with project.temporary_project() as tmp_project:
                tmp_project.import_from('/data')
                project.sync(tmp_project)

        Parameters
        ----------
        dir : str, optional
            Optionally specify where the temporary project directory is to be
            created. Defaults to the project's workspace directory.

        Returns
        -------
        :class:`~signac.Project`
            An instance of :class:`~signac.Project`.

        """
        if dir is None:
            dir = self.workspace
        _mkdir_p(self.workspace)  # ensure workspace exists
        with TemporaryProject(cls=type(self), dir=dir) as tmp_project:
            yield tmp_project

    @classmethod
    def init_project(cls, path=None):
        """Initialize a project in the provided directory.

        It is safe to call this function multiple times with the same
        arguments. However, a `RuntimeError` is raised if an existing project
        configuration would conflict with the provided initialization
        parameters.

        See :ref:`signac init <signac-cli-init>` for the command line equivalent.

        Parameters
        ----------
        path : str, optional
            The directory for the project.
            Defaults to the current working directory.

        Returns
        -------
        :class:`~signac.Project`
            Initialized project, an instance of :class:`~signac.Project`.
        """
        if path is None:
            path = os.getcwd()

        try:
            project = cls.get_project(path=path, search=False)
        except LookupError:
            _raise_if_older_schema(path)
            fn_config = _get_project_config_fn(path)
            _mkdir_p(os.path.dirname(fn_config))
            config = _read_config_file(fn_config)
            config["schema_version"] = SCHEMA_VERSION
            config.write()
            project = cls.get_project(path=path)
        return project

    @classmethod
    def get_project(cls, path=None, search=True, **kwargs):
        r"""Find a project configuration and return the associated project.

        Parameters
        ----------
        path : str, optional
            The starting point to search for a project. If None, the current
            working directory is used (Default value = None).
        search : bool, optional
            If True, search for project configurations inside and above the
            specified path, otherwise only return a project in the specified
            path (Default value = True).
        \*\*kwargs :
            Optional keyword arguments that are forwarded to the
            :class:`.Project` class constructor.

        Returns
        -------
        :class:`~signac.Project`
            An instance of :class:`~signac.Project`.

        Raises
        ------
        LookupError
            If no project configuration can be found.

        """
        if path is None:
            path = os.getcwd()

        if not os.path.exists(path):
            raise LookupError(
                f"Unable to determine project id for nonexistent path '{os.path.abspath(path)}'."
            )

        old_path = path
        if not search and not os.path.isfile(_get_project_config_fn(path)):
            path = None
        else:
            path = _locate_config_dir(path)

        if not path:
            raise LookupError(
                f"Unable to find project at path '{os.path.abspath(old_path)}'."
            )

        return cls(path=path, **kwargs)

    @classmethod
    def get_job(cls, path=None):
        """Find a Job in or above the current working directory (or provided path).

        Parameters
        ----------
        path : str, optional
            The starting point to search for a job. If None, the current
            working directory is used (Default value = None).

        Returns
        -------
        :class:`~signac.job.Job`
            The first job found in or above the provided path.

        Raises
        ------
        LookupError
            If a job cannot be found.

        """
        if path is None:
            path = os.getcwd()
        path = os.path.abspath(path)

        # Ensure the path exists, which is not guaranteed by the regex match
        if not os.path.exists(path):
            raise LookupError(f"Path does not exist: '{path}'.")

        # Find the last match instance of a job id
        results = list(re.finditer(JOB_ID_REGEX, path))
        if len(results) == 0:
            raise LookupError(f"Could not find a job id in path '{path}'.")
        match = results[-1]
        job_id = match.group(0)
        job_path = path[: match.end()]

        # Find a project *above* the path (avoid finding nested projects)
        project = cls.get_project(os.path.join(job_path, os.pardir))

        # Return the matched job id from the found project
        return Job(project=project, id_=job_id, directory_known=True)

    def __getstate__(self):
        state = dict(self.__dict__)
        # Locks are not pickleable and must be removed from the state
        del state["_lock"]
        return state

    def __setstate__(self, state):
        # Locks are not pickleable and must be added back to the state
        state["_lock"] = RLock()
        self.__dict__.update(state)


@contextmanager
def TemporaryProject(cls=None, **kwargs):
    r"""Context manager for the generation of a temporary project.

    This is a factory function that creates a Project within a temporary directory
    and must be used as context manager, for example like this:

    .. code-block:: python

        with TemporaryProject() as tmp_project:
            tmp_project.import_from('/data')

    Parameters
    ----------
    cls : object, optional
        The class of the temporary project.
        Defaults to :class:`~signac.Project`.
    \*\*kwargs :
        Optional keyword arguments that are forwarded to the
        TemporaryDirectory class constructor, which is used to create a
        temporary project directory.

    Yields
    ------
    :class:`~signac.Project`
        An instance of :class:`~signac.Project`.

    """
    if cls is None:
        cls = Project
    with TemporaryDirectory(**kwargs) as tmp_dir:
        yield cls.init_project(path=tmp_dir)


class _JobsCursorIterator:
    """Iterator for JobsCursor."""

    def __init__(self, project, ids):
        self._project = project
        self._ids = ids
        self._ids_iterator = iter(ids)

    def __next__(self):
        return Job(
            project=self._project, id_=next(self._ids_iterator), directory_known=True
        )

    def __iter__(self):
        return type(self)(self._project, self._ids)


class JobsCursor:
    """An iterator over a search query result.

    Application developers should not directly instantiate this class, but
    use :meth:`~signac.Project.find_jobs` instead.

    Enables simple iteration and grouping operations.

    .. warning::

        `JobsCursor` caches the jobs that match the filter. Call `Project.find_jobs`
        again to update the search after making changes to jobs or the workspace
        that would change the result of the search.

    Parameters
    ----------
    project : :class:`~signac.Project`
        Project handle.
    filter : Mapping
        A mapping of key-value pairs used for the query (Default value = None).

    """

    _use_pandas_for_html_repr = True  # toggle use of pandas for html repr

    def __init__(self, project, filter=None):
        self._project = project
        self._filter = filter

        # Replace empty filters with None for performance
        if self._filter == {}:
            self._filter = None

        # Cache for matching ids.
        self._id_cache = None
        self._id_set_cache = None

    @property
    def _ids(self):
        """List of job ids that match the filter.

        Populated on first use, then cached in subsequent calls.

        Returns
        -------
        list[str]
            Job ids that match the filter.
        """
        if self._id_cache is None:
            self._id_cache = self._project._find_job_ids(self._filter)

        return self._id_cache

    @property
    def _id_set(self):
        """Set of job ids that match the filter.

        Populated on first use, then cached in subsequent calls.

        Returns
        -------
        set[str]
            Job ids that match the filter.
        """
        if self._id_set_cache is None:
            self._id_set_cache = set(self._ids)

        return self._id_set_cache

    def __eq__(self, other):
        return self._project == other._project and self._filter == other._filter

    def __len__(self):
        # Highly performance critical code path!!
        if self._filter:
            # We use the standard function for determining job ids if and only if
            # any of the two filter is provided.
            return len(self._ids)
        else:
            # Without filters, we can simply return the length of the whole project.
            return len(self._project)

    def __contains__(self, job):
        """Determine whether a job is in this cursor.

        Parameters
        ----------
        job : :class:`~signac.job.Job`
            The job to check.

        Returns
        -------
        bool
            True if the job matches the filter criteria and is initialized for this project.

        """
        if self._filter:
            return job.id in self._id_set

        return job in self._project

    def __iter__(self):
        # Code duplication here for improved performance.
        return _JobsCursorIterator(self._project, self._ids)

    def groupby(self, key=None, default=None):
        """Group jobs according to one or more state point or document parameters.

        Prepend the key with 'sp.' or 'doc.' to specify the query namespace. If no prefix
        is specified, group by state point key.

        This method can be called on any :class:`~signac.project.JobsCursor` such as
        the one returned by :meth:`~signac.Project.find_jobs` or by iterating over a
        project.

        Examples
        --------
        .. code-block:: python

            # Group jobs by state point parameter 'a'.
            for key, group in project.groupby('a'):
                print(key, list(group))

            # Group jobs by document value 'a'.
            for key, group in project.groupby('doc.a'):
                print(key, list(group))

            # Group jobs by jobs.sp['a'] and job.document['b']
            for key, group in project.groupby(('a', 'doc.b')):
                print(key, list(group))

            # Find jobs where job.sp['a'] is 1 and group them
            # by job.sp['b'] and job.sp['c'].
            for key, group in project.find_jobs({'a': 1}).groupby(('b', 'c')):
                print(key, list(group))

            # Group by job.sp['d'] and job.document['count'] using a lambda.
            for key, group in project.groupby(
                lambda job: (job.sp['d'], job.document['count'])
            ):
                print(key, list(group))

        If `key` is None, jobs are grouped by id, placing one job into each group.

        If `default` is None, only jobs with the `key` defined will be grouped.
        Jobs without the `key` will be filtered out and not included in any
        group.

        Parameters
        ----------
        key : str, iterable, or callable, optional
            The grouping key(s) passed as a string,
            iterable of strings, or a callable that will be passed one
            argument, the job (Default value = None).
        default : object, optional
            A default value to be used when a given key is not
            present. The value must be sortable and is only used if not None
            (Default value = None).

        Yields
        ------
        key :
            Key identifying this group.
        group : iterable of Jobs
            Iterable of `Job` instances matching this group.
        """
        _filter = self._filter

        if default is not None and not isinstance(key, (str, Iterable)):
            warnings.warn(
                "The default parameter is ignored for grouping except "
                "when grouping by a (list of) string key(s)."
            )

        def _strip_prefix(key):
            """Strip the prefix, if it is present.

            Implicit and explicit sp prefixes are equivalent and can be treated
            identically for this purpose.
            """
            return key.split(".", 1)[-1]

        def _is_doc_key(key):
            """Check if a key is a document key."""
            return "." in key and key.split(".", 1)[0] == "doc"

        if isinstance(key, str):
            stripped_key = _strip_prefix(key)

            if default is None:
                if _filter is None:
                    _filter = {key: {"$exists": True}}
                else:
                    _filter = {"$and": [{key: {"$exists": True}}, _filter]}

                if _is_doc_key(key):

                    def keyfunction(job):
                        return job.document[stripped_key]

                else:

                    def keyfunction(job):
                        return job.cached_statepoint[stripped_key]

            else:
                if _is_doc_key(key):

                    def keyfunction(job):
                        return job.document.get(stripped_key, default)

                else:

                    def keyfunction(job):
                        return job.cached_statepoint.get(stripped_key, default)

        elif isinstance(key, Iterable):
            sp_keys = []
            doc_keys = []
            for k in key:
                if _is_doc_key(k):
                    doc_keys.append(_strip_prefix(k))
                else:
                    sp_keys.append(_strip_prefix(k))

            if default is None:
                if _filter is None:
                    _filter = {k: {"$exists": True} for k in key}
                else:
                    _filter = {"$and": [{k: {"$exists": True} for k in key}, _filter]}

                def keyfunction(job):
                    return tuple(
                        [job.cached_statepoint[k] for k in sp_keys]
                        + [job.document[k] for k in doc_keys]
                    )

            else:

                def keyfunction(job):
                    return tuple(
                        [job.cached_statepoint.get(k, default) for k in sp_keys]
                        + [job.document.get(k, default) for k in doc_keys]
                    )

        elif key is None:
            # Must return a type that can be ordered with <, >
            def keyfunction(job):
                return job.id

        else:
            # Pass the job document to a callable
            keyfunction = key

        yield from groupby(
            sorted(
                iter(self._project.find_jobs(_filter)),
                key=keyfunction,
            ),
            key=keyfunction,
        )

    def export_to(self, target, path=None, copytree=None):
        """Export all jobs to a target location, such as a directory or a (zipped) archive file.

        See Also
        --------
        :meth:`~signac.Project.export_to` : For full details on how to use this function.

        Parameters
        ----------
        target : str
            A path to a directory or archive file to export to.
        path : str or callable
            The path (function) used to structure the exported data space
            (Default value = None).
        copytree : callable, optional
            The function used for copying directory tree structures. Uses
            :func:`shutil.copytree` if ``None`` (Default value = None). The function
            requires that the target is a directory.

        Returns
        -------
        dict
            A dictionary that maps the source directory paths to the target
            directory paths.

        """
        from .import_export import export_jobs

        return dict(
            export_jobs(jobs=list(self), target=target, path=path, copytree=copytree)
        )

    def to_dataframe(
        self, sp_prefix="sp.", doc_prefix="doc.", usecols=None, flatten=False
    ):
        """Convert the selection of jobs to a pandas :class:`~pandas.DataFrame`.

        This function exports the job metadata to a
        :py:class:`pandas.DataFrame`. All state point and document keys are
        prefixed by default to be able to distinguish them.

        Parameters
        ----------
        sp_prefix : str, optional
            Prefix state point keys with the given string. Defaults to "sp.".
        doc_prefix : str, optional
            Prefix document keys with the given string. Defaults to "doc.".
        usecols : list-like or callable, optional
            Used to select a subset of columns. If list-like, must contain
            strings corresponding to the column names that should be included.
            For example, ``['sp.a', 'doc.notes']``. If callable, the column
            will be included if the function called on the column name returns
            True. For example, ``lambda x: 'sp.' in x``. Defaults to ``None``,
            which uses all columns from the state point and document. Note
            that this filter is applied *after* the doc and sp prefixes are
            added to the column names.
        flatten : bool, optional
            Whether nested state points or document keys should be flattened.
            If True, ``{'a': {'b': 'c'}}`` becomes a column named ``a.b`` with
            value ``c``. If False, it becomes a column named ``a`` with value
            ``{'b': 'c'}``. Defaults to ``False``.

        Returns
        -------
        :class:`~pandas.DataFrame`
            A pandas DataFrame with all job metadata.

        """
        import pandas

        if usecols is None:

            def usecols(column):
                return True

        elif not callable(usecols):
            included_columns = set(usecols)

            def usecols(column):
                return column in included_columns

        def _flatten(d):
            return dict(_nested_dicts_to_dotted_keys(d)) if flatten else d

        def _export_sp_and_doc(job):
            """Prefix and filter state point and document keys.

            Parameters
            ----------
            job : :class:`~signac.job.Job`
                The job instance.

            Yields
            ------
            tuple
                tuple with prefixed state point or document key and values.

            """
            for key, value in _flatten(job.cached_statepoint).items():
                prefixed_key = sp_prefix + key
                if usecols(prefixed_key):
                    yield prefixed_key, value
            for key, value in _flatten(job.doc).items():
                prefixed_key = doc_prefix + key
                if usecols(prefixed_key):
                    yield prefixed_key, value

        return pandas.DataFrame.from_dict(
            data={job.id: dict(_export_sp_and_doc(job)) for job in self},
            orient="index",
        ).infer_objects()

    def __repr__(self):
        return "{type}(project={project}, filter={filter})".format(
            type=self.__class__.__name__,
            project=repr(self._project),
            filter=repr(self._filter),
        )

    def _repr_html_jobs(self):
        """Jobs representation as HTML.

        Returns
        -------
        str
            HTML representation of jobs.

        """
        html = ""
        len_self = len(self)
        try:
            if len_self > 100:
                raise RuntimeError  # too large
            if self._use_pandas_for_html_repr:
                import pandas
            else:
                raise RuntimeError
        except ImportError:
            warnings.warn("Install pandas for a pretty representation of jobs.")
            html += f"<br/><strong>{len_self}</strong> job(s) found"
        except RuntimeError:
            html += f"<br/><strong>{len_self}</strong> job(s) found"
        else:
            with pandas.option_context("display.max_rows", 20):
                html += self.to_dataframe()._repr_html_()
        return html

    def _repr_html_(self):
        """Return an HTML representation of JobsCursor.

        Returns
        -------
        str
            HTML representation of jobs.

        """
        return repr(self) + self._repr_html_jobs()


def init_project(path=None):
    """Initialize a project.

    It is safe to call this function multiple times with the same arguments.
    However, a `RuntimeError` is raised if an existing project configuration
    would conflict with the provided initialization parameters.

    Parameters
    ----------
    path : str, optional
        The directory for the project.
        Defaults to the current working directory.

    Returns
    -------
    :class:`~signac.Project`
        The initialized project instance.

    Raises
    ------
    RuntimeError
        If the project path already contains a conflicting project
        configuration.

    """
    return Project.init_project(path=path)


def get_project(path=None, search=True, **kwargs):
    r"""Find a project configuration and return the associated project.

    Parameters
    ----------
    path : str, optional
        The starting point to search for a project. If None, the current
        working directory is used (Default value = None).
    search : bool, optional
        If True, search for project configurations inside and above the
        specified path, otherwise only return a project in the specified
        path (Default value = True).
    \*\*kwargs :
        Optional keyword arguments that are forwarded to
        :meth:`~signac.Project.get_project`.

    Returns
    -------
    :class:`~signac.Project`
        An instance of :class:`~signac.Project`.

    Raises
    ------
    LookupError
        If no project configuration can be found.

    """
    return Project.get_project(path=path, search=search, **kwargs)


def get_job(path=None):
    """Find a Job in or above the provided path (or the current working directory).

    Parameters
    ----------
    path : str, optional
        The starting point to search for a job. If None, the current
        working directory is used (Default value = None).

    Returns
    -------
    :class:`~signac.job.Job`
        The first job found in or above the provided path.

    Raises
    ------
    LookupError
        If a job cannot be found.

    Examples
    --------
    When the current directory is a job directory:

    >>> signac.get_job()
    signac.job.Job(project=..., statepoint={...})

    """
    return Project.get_job(path=path)
