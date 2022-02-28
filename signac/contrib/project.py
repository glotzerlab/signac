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
from collections.abc import Iterable
from contextlib import contextmanager
from itertools import groupby
from multiprocessing.pool import ThreadPool
from tempfile import TemporaryDirectory
from threading import RLock

from packaging import version

from ..common.config import Config, load_config, read_config_file
from ..core.h5store import H5StoreManager
from ..sync import sync_projects
from ..synced_collections.backends.collection_json import BufferedJSONAttrDict
from ..version import SCHEMA_VERSION, __version__
from ._index import _SearchIndexer
from .errors import (
    DestinationExistsError,
    IncompatibleSchemaVersion,
    JobsCorruptedError,
    WorkspaceError,
)
from .filterparse import _add_prefix, _root_keys, parse_filter
from .hashing import calc_id
from .job import Job
from .schema import ProjectSchema
from .utility import _mkdir_p, _nested_dicts_to_dotted_keys, _split_and_print_progress

logger = logging.getLogger(__name__)

JOB_ID_LENGTH = 32
JOB_ID_REGEX = re.compile(f"[a-f0-9]{{{JOB_ID_LENGTH}}}")

# The warning used for doc filter deprecation everywhere. Don't use
# triple-quoted multi-line string to avoid inserting newlines.
# TODO: In signac 2.0, remove all docstrings for doc_filter parameters. The
# doc_filter parameters will only be preserved for backwards compatibility but
# not advertised as part of the API in signac 2.0.
DOC_FILTER_WARNING = (
    "The doc_filter argument is deprecated as of version 1.7 and will be removed "
    "in version 3.0. Users should instead use a filter with a 'doc.' prefix. For "
    "example, `doc_filter={'foo': 'bar'}` is equivalent to `filter={'doc.foo': 'bar'}`. "
    "See https://docs.signac.io/en/latest/query.html#query-namespaces for more "
    "information."
)

# Temporary default for project names until they are removed entirely in signac 2.0
_DEFAULT_PROJECT_NAME = None


class _ProjectConfig(Config):
    r"""Extends the project config to make it immutable.

    Parameters
    ----------
    \*args :
        Forwarded to :class:`~signac.common.config.Config` constructor.
    \*\*kwargs :
        Forwarded to :class:`~signac.common.config.Config` constructor.

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

    Application developers should usually not need to
    directly instantiate this class, but use
    :meth:`~signac.get_project` instead.

    Parameters
    ----------
    config :
        The project configuration to use. By default, it loads the first signac
        project configuration found while searching upward from the current
        working directory (Default value = None).

    """

    Job = Job

    FN_DOCUMENT = "signac_project_document.json"
    "The project's document filename."

    KEY_DATA = "signac_data"
    "The project's datastore key."

    FN_CACHE = ".signac_sp_cache.json.gz"
    "The default filename for the state point cache file."

    _use_pandas_for_html_repr = True  # toggle use of pandas for html repr

    def __init__(self, config=None):
        if config is None:
            config = load_config()
        self._config = _ProjectConfig(config)
        self._lock = RLock()

        # Ensure that the project id is configured.
        try:
            self._id = str(self.config["project"])
        except KeyError:
            raise LookupError(
                "Unable to determine project id. "
                "Please verify that '{}' is a signac project path.".format(
                    os.path.abspath(self.config.get("project_dir", os.getcwd()))
                )
            )

        # Ensure that the project's data schema is supported.
        self._check_schema_compatibility()

        # Prepare project document
        self._document = None

        # Prepare project H5StoreManager
        self._stores = None

        # Prepare root directory and workspace paths.
        # os.path is used instead of pathlib.Path for performance.
        self._root_directory = self.config["project_dir"]
        self._workspace = os.path.expandvars(
            self.config.get("workspace_dir", "workspace")
        )
        if not os.path.isabs(self._workspace):
            self._workspace = os.path.join(self._root_directory, self._workspace)

        # Prepare workspace directory.
        if not os.path.isdir(self.workspace()):
            try:
                _mkdir_p(self.workspace())
            except OSError:
                logger.error(
                    "Error occurred while trying to create "
                    "workspace directory for project {}.".format(self.id)
                )
                raise

        # Internal state point cache
        # Note that the state point cache is a superset of the jobs in the
        # project, and its contents cannot be invalidated. The cached mapping
        # of "id: statepoint" is valid even after a job has been removed, and
        # can be used to re-open a job by id as long as that id remains in the
        # cache.
        self._sp_cache = {}
        self._sp_cache_misses = 0
        self._sp_cache_warned = False
        self._sp_cache_miss_warning_threshold = self.config.get(
            "statepoint_cache_miss_warning_threshold", 500
        )

    def __str__(self):
        """Return the project's id."""
        return str(self.id)

    def __repr__(self):
        return "{type}.get_project({root})".format(
            type=self.__class__.__name__, root=repr(self.root_directory())
        )

    def _repr_html_(self):
        """Project details in HTML format for use in IPython environment.

        Returns
        -------
        str
            HTML containing project details.

        """
        return (
            "<p>"
            + f"<strong>Project:</strong> {self.id}<br>"
            + f"<strong>Root:</strong> {self.root_directory()}<br>"
            + f"<strong>Workspace:</strong> {self.workspace()}<br>"
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
        :class:`~signac.contrib.project._ProjectConfig`
            Dictionary containing project's configuration.

        """
        return self._config

    def root_directory(self):
        """Return the project's root directory.

        Returns
        -------
        str
            Path of project directory.

        """
        return self._root_directory

    def workspace(self):
        """Return the project's workspace directory.

        The workspace defaults to `project_root/workspace`. Configure this
        directory with the ``'workspace_dir'`` configuration option. A relative
        path is assumed to be relative to the project's root directory.

        .. note::
            The configuration will respect environment variables,
            such as ``$HOME``.

        See :ref:`signac project -w <signac-cli-project>` for the command line equivalent.

        Returns
        -------
        str
            Path of workspace directory.

        """
        return self._workspace

    @property
    def id(self):
        """Get the project identifier.

        Returns
        -------
        str
            The project id.

        """
        return self._id

    def _check_schema_compatibility(self):
        """Check whether this project's data schema is compatible with this version.

        Raises
        ------
        :class:`~signac.errors.IncompatibleSchemaVersion`
            If the schema version is incompatible.

        """
        schema_version = version.parse(SCHEMA_VERSION)
        config_schema_version = version.parse(self.config["schema_version"])
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
                "requires schema version '{}'. Please use '$ signac migrate' to "
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
            for _id in job_ids:
                if _id[:i] in tmp:
                    break
                else:
                    tmp.add(_id[:i])
            else:
                break
        return i

    def fn(self, filename):
        """Prepend a filename with the project's root directory path.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        str
            The joined path of project root directory and filename.

        """
        return os.path.join(self.root_directory(), filename)

    def isfile(self, filename):
        """Check if a filename exists in the project's root directory.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        bool
            True if filename exists in the project's root directory.

        """
        return os.path.isfile(self.fn(filename))

    def _reset_document(self, new_doc):
        """Reset document to new document passed.

        Parameters
        ----------
        new_doc : dict
            The new project document.

        """
        with self._lock:
            self.document.reset(new_doc)

    @property
    def document(self):
        """Get document associated with this project.

        Returns
        -------
        :class:`~signac.synced_collections.backends.collection_json.BufferedJSONAttrDict`
            The project document.

        """
        with self._lock:
            if self._document is None:
                fn_doc = os.path.join(self.root_directory(), self.FN_DOCUMENT)
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
        self._reset_document(new_doc)

    @property
    def doc(self):
        """Get document associated with this project.

        Alias for :meth:`~signac.Project.document`.

        Returns
        -------
        :class:`~signac.synced_collections.backends.collection_json.BufferedJSONAttrDict`
            The project document.

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
        """Get HDF5-stores associated with this project.

        Use this property to access an HDF5 file within the project's root
        directory using the H5Store dict-like interface.

        This is an example for accessing an HDF5 file called ``'my_data.h5'``
        within the project's root directory:

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
            The HDF5-Store manager for this project.

        """
        with self._lock:
            if self._stores is None:
                self._stores = H5StoreManager(self.root_directory())
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
        statepoint : dict
            The job's unique set of state point parameters (Default value = None).
        id : str
            The job id (Default value = None).

        Returns
        -------
        :class:`~signac.contrib.job.Job`
            The job instance.

        Raises
        ------
        KeyError
            If the attempt to open the job by id fails.
        LookupError
            If the attempt to open the job by an abbreviated id returns more
            than one match.

        """
        if (statepoint is None) == (id is None):
            raise ValueError("Either statepoint or id must be provided, but not both.")
        if id is None:
            # Second best case (Job will update self._sp_cache on init)
            return self.Job(project=self, statepoint=statepoint)
        try:
            # Optimal case (id is in the state point cache)
            return self.Job(project=self, statepoint=self._sp_cache[id], _id=id)
        except KeyError:
            # Worst case: no state point was provided and the state point cache
            # missed. The Job will register itself in self._sp_cache when the
            # state point is accessed.
            if len(id) < JOB_ID_LENGTH:
                # Resolve partial job ids (first few characters) into a full job id
                job_ids = self._find_job_ids()
                matches = [_id for _id in job_ids if _id.startswith(id)]
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
            return self.Job(project=self, _id=id)

    def _job_dirs(self):
        """Generate ids of jobs in the workspace.

        Yields
        ------
        str
            Job id.

        """
        try:
            for d in os.listdir(self.workspace()):
                if JOB_ID_REGEX.match(d):
                    yield d
        except OSError as error:
            if error.errno == errno.ENOENT:
                if os.path.islink(self.workspace()):
                    raise WorkspaceError(
                        f"The link '{self.workspace()}' pointing to the workspace is broken."
                    )
                elif not os.path.isdir(os.path.dirname(self.workspace())):
                    logger.warning(
                        "The path to the workspace directory "
                        "('{}') does not exist.".format(
                            os.path.dirname(self.workspace())
                        )
                    )
                else:
                    logger.info(
                        f"The workspace directory '{self.workspace()}' does not exist!"
                    )
            else:
                logger.error(
                    f"Unable to access the workspace directory '{self.workspace()}'."
                )
                raise WorkspaceError(error)

    def num_jobs(self):
        """Return the number of initialized jobs.

        Returns
        -------
        int
            Count of initialized jobs.

        """
        # We simply count the the number of valid directories and avoid building a list
        # for improved performance.
        i = 0
        for i, _ in enumerate(self._job_dirs(), 1):
            pass
        return i

    __len__ = num_jobs

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
        return os.path.exists(os.sep.join((self.workspace(), job_id)))

    def __contains__(self, job):
        """Determine whether a job is in the project's data space.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
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
        exclude_const : bool
            Exclude all state point keys that are shared by all jobs within this project
            (Default value = False).
        subset :
            A sequence of jobs or job ids specifying a subset over which the state point
            schema should be detected (Default value = None).

        Returns
        -------
        :class:`~signac.contrib.schema.ProjectSchema`
            The detected project schema.

        """
        from .schema import _build_job_statepoint_index

        index = _SearchIndexer(self._build_index(include_job_document=False))
        if subset is not None:
            subset = {str(s) for s in subset}.intersection(index.keys())
            index = _SearchIndexer((_id, index[_id]) for _id in subset)
        statepoint_index = _build_job_statepoint_index(
            exclude_const=exclude_const, index=index
        )
        return ProjectSchema.detect(statepoint_index)

    def _find_job_ids(self, filter=None):
        """Find the job ids of all jobs matching the filter.

        The filter argument must be a JSON-serializable Mapping of key-value
        pairs. The ``filter`` argument can search against both job state points
        and job documents. See
        https://docs.signac.io/en/latest/query.html#query-namespaces
        for a description of supported queries.

        Parameters
        ----------
        filter : Mapping
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
        filter = dict(parse_filter(_add_prefix("sp.", filter)))
        index = _SearchIndexer(
            self._build_index(include_job_document="doc" in _root_keys(filter))
        )
        return list(index.find(filter))

    def find_jobs(self, filter=None, *args, **kwargs):
        """Find all jobs in the project's workspace.

        The filter argument must be a JSON-serializable Mapping of key-value
        pairs. The ``filter`` argument can search against both job state points
        and job documents. See
        https://docs.signac.io/en/latest/query.html#query-namespaces
        for a description of supported queries.

        See :ref:`signac find <signac-cli-find>` for the command line equivalent.

        Parameters
        ----------
        filter : Mapping
            A mapping of key-value pairs used for the query (Default value =
            None).

        Returns
        -------
        :class:`~signac.contrib.project.JobsCursor`
            JobsCursor of jobs matching the provided filter.

        Raises
        ------
        TypeError
            If the filters are not JSON serializable.
        ValueError
            If the filters are invalid.

        """
        doc_filter = next(iter(args), None) or kwargs.pop("doc_filter", None)
        if len(args) > 1 or len(kwargs):
            raise TypeError("Unsupported arguments were provided.")
        filter = dict(parse_filter(_add_prefix("sp.", filter)))
        if doc_filter:
            warnings.warn(DOC_FILTER_WARNING, FutureWarning)
            filter.update(parse_filter(_add_prefix("doc.", doc_filter)))
        return JobsCursor(self, filter)

    def __iter__(self):
        return iter(self.find_jobs())

    def groupby(self, key=None, default=None):
        """Group jobs according to one or more state point parameters.

        This method can be called on any :class:`~signac.contrib.project.JobsCursor` such as
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

        Parameters
        ----------
        key : str, iterable, or callable
            The state point grouping parameter(s) passed as a string,
            iterable of strings, or a callable that will be passed one
            argument, the job (Default value = None).
        default :
            A default value to be used when a given state point key is not
            present. The value must be sortable and is only used if not None
            (Default value = None).

        Yields
        ------
        key :
            Key identifying this group.
        group : iterable of Jobs
            Iterable of `Job` instances matching this group.

        """
        return self.find_jobs().groupby(key, default=default)

    def to_dataframe(self, *args, **kwargs):
        r"""Export the project metadata to a pandas :class:`~pandas.DataFrame`.

        The arguments to this function are forwarded to
        :meth:`~signac.contrib.project.JobsCursor.to_dataframe`.

        Parameters
        ----------
        \*args :
            Forwarded to :meth:`~signac.contrib.project.JobsCursor.to_dataframe`.
        \*\*kwargs :
            Forwarded to :meth:`~signac.contrib.project.JobsCursor.to_dataframe`.

        Returns
        -------
        :class:`~pandas.DataFrame`

        """
        return self.find_jobs().to_dataframe(*args, **kwargs)

    def _register(self, _id, statepoint):
        """Register the job state point in the project state point cache.

        Parameters
        ----------
        _id : str
            A job identifier.
        statepoint : dict
            A validated job state point.

        """
        self._sp_cache[_id] = statepoint

    def _get_statepoint_from_workspace(self, job_id):
        """Attempt to read the state point from the workspace.

        Parameters
        ----------
        job_id : str
            Identifier of the job.

        """
        # Performance-critical path. We can rely on the project workspace, job
        # id, and state point file name to be well-formed, so just use str.join
        # with os.sep instead of os.path.join for speed.
        fn_statepoint = os.sep.join((self.workspace(), job_id, self.Job.FN_STATE_POINT))
        try:
            with open(fn_statepoint, "rb") as statepoint_file:
                return json.loads(statepoint_file.read().decode())
        except (OSError, ValueError) as error:
            if os.path.isdir(os.sep.join((self.workspace(), job_id))):
                logger.error(
                    "Error while trying to access state point file of job '{}': '{}'.".format(
                        job_id, error
                    )
                )
                raise JobsCorruptedError([job_id])
            raise KeyError(job_id)

    def _get_statepoint(self, job_id):
        """Get the state point associated with a job id.

        The state point is retrieved from the internal cache, from
        the workspace or from a state points file.

        Parameters
        ----------
        job_id : str
            A job id to get the state point for.

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
        if not self._sp_cache:
            # Triggers if no state points have been added to the cache, and all
            # the values are None.
            self._read_cache()
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
                    "to update cache with the Project.update_cache() method."
                )
                self._sp_cache_warned = True
            statepoint = self._get_statepoint_from_workspace(job_id)
            # Update the project's state point cache from this cache miss
            self._sp_cache[job_id] = statepoint
        return statepoint

    def create_linked_view(self, prefix=None, job_ids=None, path=None):
        """Create or update a persistent linked view of the selected data space.

        Similar to :meth:`~signac.Project.export_to`, this function expands the data space
        for the selected jobs, but instead of copying data will create symbolic links to the
        individual job workspace directories. This is primarily useful for browsing through
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
        prefix : str
            The path where the linked view will be created or updated (Default value = None).
        job_ids : iterable
            If None (the default), create the view for the complete data space,
            otherwise only for this iterable of job ids.
        path :
            The path (function) used to structure the linked data space (Default value = None).

        Returns
        -------
        dict
            A dictionary that maps the source directory paths to the linked
            directory paths.

        """
        from .linked_view import create_linked_view

        return create_linked_view(self, prefix, job_ids, path)

    def clone(self, job, copytree=shutil.copytree):
        """Clone job into this project.

        Create an identical copy of job within this project.

        See :ref:`signac clone <signac-cli-clone>` for the command line equivalent.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            The job to copy into this project.
        copytree :
             (Default value = :func:`shutil.copytree`)

        Returns
        -------
        :class:`~signac.contrib.job.Job`
            The job instance corresponding to the copied job.

        Raises
        ------
        :class:`~signac.errors.DestinationExistsError`
            In case that a job with the same id is already
            initialized within this project.

        """
        dst = self.open_job(job.statepoint())
        try:
            copytree(job.workspace(), dst.workspace())
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
        strategy :
            A file synchronization strategy (Default value = None).
        exclude :
            Files with names matching the given pattern will be excluded
            from the synchronization (Default value = None).
        doc_sync :
            The function applied for synchronizing documents (Default value = None).
        selection :
            Only sync the given jobs (Default value = None).
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
        target :
            A path to a directory to export to. The target can not already exist.
            Besides directories, possible targets are tar files (`.tar`), gzipped tar files
            (`.tar.gz`), zip files (`.zip`), bzip2-compressed files (`.bz2`),
            and xz-compressed files (`.xz`).
        path :
            The path (function) used to structure the exported data space.
            This argument must either be a callable which returns a path (str) as a function
            of `job`, a string where fields are replaced using the job-state point dictionary,
            or `False`, which means that we just use the job-id as path.
            Defaults to the equivalent of ``{{auto}}``.
        copytree :
            The function used for the actual copying of directory tree
            structures. Defaults to :func:`shutil.copytree`.
            Can only be used when the target is a directory.

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
        origin :
            The path to the data space origin, which is to be imported. This may be a path to
            a directory, a zip file, or a tarball archive (Default value = None).
        schema :
            An optional schema function, which is either a string or a function that accepts a
            path as its first and only argument and returns the corresponding state point as dict.
            (Default value = None).
        sync :
            If ``True``, the project will be synchronized with the imported data space. If a
            dict of keyword arguments is provided, the arguments will be used for
            :meth:`~signac.Project.sync` (Default value = None).
        copytree :
            Specify which exact function to use for the actual copytree operation.
            Defaults to :func:`shutil.copytree`.

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
                statepoint = self._get_statepoint(job_id)
                if calc_id(statepoint) != job_id:
                    corrupted.append(job_id)
                else:
                    self.open_job(statepoint).init()
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
        job_ids :
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
                statepoint = self._get_statepoint(job_id)
                # Check if state point and id correspond.
                correct_id = calc_id(statepoint)
                if correct_id != job_id:
                    logger.warning(
                        "The job id of job '{}' is incorrect; "
                        "it should be '{}'.".format(job_id, correct_id)
                    )
                    invalid_wd = os.path.join(self.workspace(), job_id)
                    correct_wd = os.path.join(self.workspace(), correct_id)
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
        include_job_document : bool
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
                    fn_document = os.sep.join(
                        (self.workspace(), job_id, self.Job.FN_DOCUMENT)
                    )
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
            for _id in to_remove:
                del self._sp_cache[_id]

            def _add(_id):
                self._sp_cache[_id] = self._get_statepoint_from_workspace(_id)

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
        is stored in the project root directory. Most data space operations,
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

        The temporary project is by default created within the root project's
        workspace to ensure that they share the same file system. This is an example
        for how this method can be used for the import and synchronization of
        external data spaces.

        .. code-block:: python

            with project.temporary_project() as tmp_project:
                tmp_project.import_from('/data')
                project.sync(tmp_project)

        Parameters
        ----------
        dir : str
            Optionally specify where the temporary project root directory is to be
            created. Defaults to the project's workspace directory.

        Returns
        -------
        :class:`~signac.Project`
            An instance of :class:`~signac.Project`.

        """
        if dir is None:
            dir = self.workspace()
        _mkdir_p(self.workspace())  # ensure workspace exists
        with TemporaryProject(cls=type(self), dir=dir) as tmp_project:
            yield tmp_project

    @classmethod
    def init_project(cls, *args, root=None, workspace=None, make_dir=True, **kwargs):
        """Initialize a project in the provided root directory.

        It is safe to call this function multiple times with the same
        arguments. However, a `RuntimeError` is raised if an existing project
        configuration would conflict with the provided initialization
        parameters.

        See :ref:`signac init <signac-cli-init>` for the command line equivalent.

        Parameters
        ----------
        root : str, optional
            The root directory for the project.
            Defaults to the current working directory.
        workspace : str, optional
            The workspace directory for the project.
            Defaults to a subdirectory ``workspace`` in the project root.
        make_dir : bool, optional
            Create the project root directory if it does not exist yet
            (Default value = True).

        Returns
        -------
        :class:`~signac.Project`
            Initialized project, an instance of :class:`~signac.Project`.

        Raises
        ------
        RuntimeError
            If the project root path already contains a conflicting project
            configuration.

        """
        # TODO: Remove both the `if args` and `if kwargs` blocks in version 3.0
        # when we remove backwards compatibility for project name APIs.
        name = None
        # The key used to store project names in the project document.
        name_key = "signac_project_name"
        if args:
            num_args = len(args)
            if num_args == 1:
                name = args[0]
            else:
                # Match the usual error from misusing keyword-only args.
                raise TypeError(
                    f"init_project() takes 0 positional arguments but {num_args} were given"
                )
        if kwargs:
            name = kwargs.pop("name", None)
            if kwargs:
                # Match the usual error from extra keyword args.
                raise TypeError(
                    f"init_project() got an unexpected keyword argument '{next(iter(kwargs))}'"
                )

        if name is not None:
            assert version.parse(__version__) < version.parse("3.0.0")
            warnings.warn(
                "Project names were removed in signac 2.0. If you intended to call "
                "`init_project` with a root directory as the sole positional argument, please "
                f"provide it as a keyword argument: `init_project(root={name})`. If your "
                "project name contains important information, consider storing it in the "
                "project document instead. The name provided will be stored in the project "
                f"document with the key `{name_key}`. Calling `init_project` with a name will "
                "become an error in signac 3.0.",
                FutureWarning,
            )

        if root is None:
            root = os.getcwd()

        if name is not None:
            warnings.warn(
                "Project names are deprecated and will be removed in signac 2.0 in favor of using "
                "the project root directory to identify projects. The name argument to "
                "init_project should be removed.",
                FutureWarning,
            )
        else:
            name = _DEFAULT_PROJECT_NAME
        try:
            project = cls.get_project(root=root, search=False)
            existing_name = project.doc.get(name_key)
            if name is not None and name != existing_name:
                raise ValueError(
                    "The name provided to `init_project` does not match the existing "
                    f"project document in which {name_key}={existing_name}."
                )
        except LookupError:
            fn_config = os.path.join(root, "signac.rc")
            if make_dir:
                _mkdir_p(os.path.dirname(fn_config))
            config = read_config_file(fn_config)
            config["project"] = name
            if workspace is not None:
                config["workspace_dir"] = workspace
            config["schema_version"] = SCHEMA_VERSION
            config.write()
            project = cls.get_project(root=root)
            if name is not None:
                project.doc[name_key] = name
            assert project.id == str(name)
            return project
        else:
            if workspace is not None and os.path.realpath(
                workspace
            ) != os.path.realpath(project.workspace()):
                raise RuntimeError(
                    f"Failed to initialize project. Path '{os.path.abspath(root)}' already "
                    "contains a conflicting project configuration."
                )
            return project

    @classmethod
    def get_project(cls, root=None, search=True, **kwargs):
        r"""Find a project configuration and return the associated project.

        Parameters
        ----------
        root : str
            The starting point to search for a project, defaults to the
            current working directory.
        search : bool
            If True, search for project configurations inside and above
            the specified root directory, otherwise only return projects
            with a root directory identical to the specified root argument
            (Default value = True).
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
            When project configuration cannot be found.

        """
        if root is None:
            root = os.getcwd()
        config = load_config(root=root, local=False)
        if "project" not in config or (
            not search
            and os.path.realpath(config["project_dir"]) != os.path.realpath(root)
        ):
            raise LookupError(
                "Unable to determine project id for path '{}'.".format(
                    os.path.abspath(root)
                )
            )

        return cls(config=config, **kwargs)

    @classmethod
    def get_job(cls, root=None):
        """Find a Job in or above the current working directory (or provided path).

        Parameters
        ----------
        root : str
            The job root directory.
            If no root directory is given, the current working directory is
            assumed to be the job directory (Default value = None).

        Returns
        -------
        :class:`~signac.contrib.job.Job`
            The job instance.

        Raises
        ------
        LookupError
            When job cannot be found.

        """
        if root is None:
            root = os.getcwd()
        root = os.path.abspath(root)

        # Ensure the root path exists, which is not guaranteed by the regex match
        if not os.path.exists(root):
            raise LookupError(f"Path does not exist: '{root}'.")

        # Find the last match instance of a job id
        results = list(re.finditer(JOB_ID_REGEX, root))
        if len(results) == 0:
            raise LookupError(f"Could not find a job id in path '{root}'.")
        match = results[-1]
        job_id = match.group(0)
        job_root = root[: match.end()]

        # Find a project *above* the root directory (avoid finding nested projects)
        project = cls.get_project(os.path.join(job_root, os.pardir))

        # Return the matched job id from the found project
        return project.open_job(id=job_id)

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
    cls :
        The class of the temporary project.
        Defaults to :class:`~signac.Project`.
    \*\*kwargs :
        Optional keyword arguments that are forwarded to the TemporaryDirectory class
        constructor, which is used to create a temporary root directory.

    Yields
    ------
    :class:`~signac.Project`
        An instance of :class:`~signac.Project`.

    """
    if cls is None:
        cls = Project
    with TemporaryDirectory(**kwargs) as tmp_dir:
        yield cls.init_project(root=tmp_dir)


def _skip_errors(iterable, log=print):
    """Skip errors.

    Parameters
    ----------
    iterable : dict
        An iterable.
    log : callable
        The function to call when logging errors (Default value = print)

    Yields
    ------
    Elements from the iterable, with exceptions ignored.

    """
    while True:
        try:
            yield next(iterable)
        except StopIteration:
            return
        except Exception as error:
            log(error)


class _JobsCursorIterator:
    """Iterator for JobsCursor."""

    def __init__(self, project, ids):
        self._project = project
        self._ids = ids
        self._ids_iterator = iter(ids)

    def __next__(self):
        return self._project.open_job(id=next(self._ids_iterator))

    def __iter__(self):
        return type(self)(self._project, self._ids)


class JobsCursor:
    """An iterator over a search query result.

    Application developers should not directly instantiate this class, but
    use :meth:`~signac.Project.find_jobs` instead.

    Enables simple iteration and grouping operations.

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

        # This private attribute allows us to implement the deprecated
        # next() method for this class.
        self._next_iter = None

    def __eq__(self, other):
        return self._project == other._project and self._filter == other._filter

    def __len__(self):
        # Highly performance critical code path!!
        if self._filter:
            # We use the standard function for determining job ids if and only if
            # any of the two filter is provided.
            return len(self._project._find_job_ids(self._filter))
        else:
            # Without filters, we can simply return the length of the whole project.
            return len(self._project)

    def __contains__(self, job):
        """Determine whether a job is in this cursor.

        Parameters
        ----------
        job : :class:`~signac.contrib.job.Job`
            The job to check.

        Returns
        -------
        bool
            True if the job matches the filter criteria and is initialized for this project.

        """
        if job not in self._project:
            # Exit early if the job is not in the project. This is O(1).
            return False
        if self._filter:
            # We use the standard function for determining job ids if a filter
            # is provided. This is O(N) and could be optimized by caching the
            # ids of state points that match a state point filter. Caching the
            # matches for a document filter is not safe because the document
            # can change.
            return job.id in self._project._find_job_ids(self._filter)
        # Without filters, we can simply check if the job is in the project.
        # By the early-exit condition, we know the job must be contained.
        return True

    def __iter__(self):
        # Code duplication here for improved performance.
        return _JobsCursorIterator(
            self._project, self._project._find_job_ids(self._filter)
        )

    def groupby(self, key=None, default=None):
        """Group jobs according to one or more state point parameters.

        This method can be called on any :class:`~signac.contrib.project.JobsCursor` such as
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

        Parameters
        ----------
        key : str, iterable, or callable
            The state point grouping parameter(s) passed as a string,
            iterable of strings, or a callable that will be passed one
            argument, the job (Default value = None).
        default :
            A default value to be used when a given state point key is not
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
                        return job.sp[stripped_key]

            else:
                if _is_doc_key(key):

                    def keyfunction(job):
                        return job.document.get(stripped_key, default)

                else:

                    def keyfunction(job):
                        return job.sp.get(stripped_key, default)

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
                        [job.sp[k] for k in sp_keys]
                        + [job.document[k] for k in doc_keys]
                    )

            else:

                def keyfunction(job):
                    return tuple(
                        [job.sp.get(k, default) for k in sp_keys]
                        + [job.document.get(k, default) for k in doc_keys]
                    )

        elif key is None:
            # Must return a type that can be ordered with <, >
            def keyfunction(job):
                return str(job)

        else:
            # Pass the job document to a callable
            keyfunction = key

        return groupby(
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
        copytree : callable
            The function used for copying of directory tree structures.
            Defaults to :func:`shutil.copytree`. Can only be used when the
            target is a directory (Default value = None).

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
            job : :class:`~signac.contrib.job.Job`
                The job instance.

            Yields
            ------
            tuple
                tuple with prefixed state point or document key and values.

            """
            for key, value in _flatten(job.statepoint).items():
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


def init_project(*args, root=None, workspace=None, make_dir=True, **kwargs):
    """Initialize a project.

    It is safe to call this function multiple times with the same arguments.
    However, a `RuntimeError` is raised if an existing project configuration
    would conflict with the provided initialization parameters.

    Parameters
    ----------
    root : str, optional
        The root directory for the project.
        Defaults to the current working directory.
    workspace : str, optional
        The workspace directory for the project.
        Defaults to a subdirectory ``workspace`` in the project root.
    make_dir : bool, optional
        Create the project root directory, if it does not exist yet (Default
        value = True).

    Returns
    -------
    :class:`~signac.Project`
        The initialized project instance.

    Raises
    ------
    RuntimeError
        If the project root path already contains a conflicting project
        configuration.

    """
    return Project.init_project(
        *args, root=root, workspace=workspace, make_dir=make_dir, **kwargs
    )


def get_project(root=None, search=True, **kwargs):
    r"""Find a project configuration and return the associated project.

    Parameters
    ----------
    root : str
        The starting point to search for a project, defaults to the current
        working directory.
    search : bool
        If True, search for project configurations inside and above the
        specified root directory, otherwise only return projects with a root
        directory identical to the specified root argument (Default value =
        True).
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
    return Project.get_project(root=root, search=search, **kwargs)


def get_job(root=None):
    """Find a Job in or above the current working directory (or provided path).

    Parameters
    ----------
    root : str
        The job root directory.
        If no root directory is given, the current working directory is
        assumed to be within the current job workspace directory (Default value = None).

    Returns
    -------
    :class:`~signac.contrib.job.Job`
        Job handle.

    Raises
    ------
    LookupError
        If this job cannot be found.

    Examples
    --------
    When the current directory is a job workspace directory:

    >>> signac.get_job()
    signac.contrib.job.Job(project=..., statepoint={...})

    """
    return Project.get_job(root=root)
