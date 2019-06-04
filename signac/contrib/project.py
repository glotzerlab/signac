# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import os
import stat
import re
import logging
import warnings
import errno
import uuid
import gzip
import time
from contextlib import contextmanager
from itertools import groupby
from multiprocessing.pool import ThreadPool

from .. import syncutil
from ..core import json
from ..core.jsondict import JSONDict
from ..core.h5store import H5StoreManager
from .collection import Collection
from ..common import six
from ..common.config import load_config
from ..common.tempdir import TemporaryDirectory
from ..sync import sync_projects
from .job import Job
from .hashing import calc_id
from .indexing import SignacProjectCrawler
from .indexing import MasterCrawler
from .utility import _mkdir_p, split_and_print_progress
from .schema import ProjectSchema
from .errors import WorkspaceError
from .errors import DestinationExistsError
from .errors import JobsCorruptedError
if six.PY2:
    from collections import Mapping, Iterable
else:
    from collections.abc import Mapping, Iterable

logger = logging.getLogger(__name__)

JOB_ID_REGEX = re.compile('[a-f0-9]{32}')

ACCESS_MODULE_MINIMAL = """import signac

def get_indexes(root):
    yield signac.get_project(root).index()
"""

ACCESS_MODULE_MASTER = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
import signac

def get_indexes(root):
    yield signac.get_project(root).index()

if __name__ == '__main__':
    with signac.Collection.open('index.txt') as index:
        signac.export(signac.index(), index, update=True)
"""


class JobSearchIndex(object):
    """Search for specific jobs with filters.

    The JobSearchIndex allows to search for job_ids,
    that are part of an index, which match specific
    statepoint filters or job document filters.

    :param index: A document index.
    """

    def __init__(self, index, _trust=False):
        self._collection = Collection(index, _trust=_trust)

    def __len__(self):
        return len(self._collection)

    def _resolve_statepoint_filter(self, q):
        for k, v in q.items():
            if k in ('$and', '$or'):
                if not isinstance(v, list) or isinstance(v, tuple):
                    raise ValueError(
                        "The argument to a logical operator must be a sequence (e.g. a list)!")
                yield k, [dict(self._resolve_statepoint_filter(i)) for i in v]
            else:
                yield 'statepoint.{}'.format(k), v

    def find_job_ids(self, filter=None, doc_filter=None):
        """Find the job_ids of all jobs matching the filters.

        The optional filter arguments must be a Mapping of key-value
        pairs and JSON serializable.

        :param filter: A mapping of key-value pairs that all
            indexed job statepoints are compared against.
        :type filter: Mapping
        :param doc_filter: A mapping of key-value pairs that all
            indexed job documents are compared against.
        :yields: The ids of all indexed jobs matching both filters.
        :raise TypeError: If the filters are not JSON serializable.
        :raises ValueError: If the filters are invalid.
        :raises RuntimeError: If the filters are not supported
            by the index.
        """
        if filter:
            filter = dict(self._resolve_statepoint_filter(filter))
            if doc_filter:
                filter.update(doc_filter)
        elif doc_filter:
            filter = doc_filter
        return self._collection._find(filter)


class Project(object):
    """The handle on a signac project.

    Application developers should usually not need to
    directly instantiate this class, but use
    :func:`signac.get_project` instead."""
    Job = Job

    FN_DOCUMENT = 'signac_project_document.json'
    "The project's document filename."

    KEY_DATA = 'signac_data'
    "The project's datastore key."

    FN_STATEPOINTS = 'signac_statepoints.json'
    "The default filename to read from and write statepoints to."

    FN_CACHE = '.signac_sp_cache.json.gz'
    "The default filename for the state point cache file."

    _use_pandas_for_html_repr = True  # toggle use of pandas for html repr

    def __init__(self, config=None):
        if config is None:
            config = load_config()
        self._config = config

        # Ensure that the project id is configured.
        self.get_id()

        # Prepare project document
        self._fn_doc = os.path.join(self._rd, self.FN_DOCUMENT)
        self._document = None

        # Internal caches
        self._index_cache = dict()
        self._sp_cache = dict()
        self._sp_cache_misses = 0
        self._sp_cache_warned = False
        self._sp_cache_miss_warning_threshold = self._config.get(
            'statepoint_cache_miss_warning_threshold', 500)

    def __str__(self):
        "Returns the project's id."
        return str(self.get_id())

    def __repr__(self):
        return "{type}({{'project': '{id}', 'project_dir': '{rd}',"\
               " 'workspace_dir': '{wd}'}})".format(
                   type=self.__class__.__module__ + '.' + self.__class__.__name__,
                   id=self.get_id(), rd=self.root_directory(), wd=self.workspace())

    def _repr_html_(self):
        return repr(self) + self.find_jobs()._repr_html_jobs()

    def __eq__(self, other):
        return repr(self) == repr(other)

    @property
    def config(self):
        "The project's configuration."
        return self._config

    @property
    def _rd(self):
        "The project root directory."
        return self._config['project_dir']

    @property
    def _wd(self):
        wd = os.path.expandvars(self._config.get('workspace_dir', 'workspace'))
        if os.path.isabs(wd):
            return wd
        else:
            return os.path.join(self._rd, wd)

    def root_directory(self):
        "Returns the project's root directory."
        return self._rd

    def workspace(self):
        """Returns the project's workspace directory.

        The workspace defaults to `project_root/workspace`.
        Configure this directory with the 'workspace_dir'
        attribute.
        If the specified directory is a relative path,
        the absolute path is relative from the project's
        root directory.

        .. note::
            The configuration will respect environment variables,
            such as $HOME."""
        return self._wd

    def get_id(self):
        """Get the project identifier.

        :return: The project id.
        :rtype: str
        :raises LookupError: If no project id could be determined.
        """
        try:
            return str(self.config['project'])
        except KeyError:
            raise LookupError(
                "Unable to determine project id ."
                "Are you sure '{}' is a signac project path?".format(
                    os.path.abspath(self.config.get('project_dir', os.getcwd()))))

    def min_len_unique_id(self):
        "Determine the minimum length required for an id to be unique."
        job_ids = list(self.find_job_ids())
        tmp = set()
        for i in range(32):
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

        :param filename: The filename of the file.
        :type filename: str
        :return: The joined path of project root directory and filename.
        """
        return os.path.join(self.root_directory(), filename)

    def isfile(self, filename):
        """True if a file with filename exists in the project's root directory.

        :param filename: The filename of the file.
        :type filename: str
        :return: True if a file with filename exists in the project's root
            directory.
        :rtype: bool
        """
        return os.path.isfile(self.fn(filename))

    def _reset_document(self, new_doc):
        if not isinstance(new_doc, Mapping):
            raise ValueError("The document must be a mapping.")
        dirname, filename = os.path.split(self._fn_doc)
        fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
            uid=uuid.uuid4(), fn=filename))
        with open(fn_tmp, 'wb') as tmpfile:
            tmpfile.write(json.dumps(new_doc).encode())
        if six.PY2:
            os.rename(fn_tmp, self._fn_doc)
        else:
            os.replace(fn_tmp, self._fn_doc)

    @property
    def document(self):
        """The document associated with this project.

        :return: The project document handle.
        :rtype: :class:`~.JSONDict`
        """
        if self._document is None:
            self._document = JSONDict(filename=self._fn_doc, write_concern=True)
        return self._document

    @document.setter
    def document(self, new_doc):
        self._reset_document(new_doc)

    @property
    def doc(self):
        """The document associated with this project.

        Alias for :attr:`~signac.Project.document`.

        :return: The project document handle.
        :rtype: :class:`~.JSONDict`
        """
        return self.document

    @doc.setter
    def doc(self, new_doc):
        self.document = new_doc

    @property
    def stores(self):
        """Access HDF5-stores associated with this project.

        Use this property to access an HDF5 file within the project's root
        directory using the H5Store dict-like interface.

        This is an example for accessing an HDF5 file called 'my_data.h5' within
        the project's root directory:

            project.stores['my_data']['array'] = np.random((32, 4))

        This is equivalent to:

            H5Store(project.fn('my_data.h5'))['array'] = np.random((32, 4))

        Both the `project.stores` and the `H5Store` itself support attribute
        access. The above example could therefore also be expressed as

            project.stores.my_data.array = np.random((32, 4))

        :return: The HDF5-Store manager for this project.
        :rtype: :class:`~..core.h5store.H5StoreManager
        """
        return H5StoreManager(self._rd)

    @property
    def data(self):
        """The data associated with this project.

        Equivalent to:

            return project.store['signac_data']

        :return: An HDF5-backed datastore.
        :rtype: :class:`~..core.h5store.H5Store`
        """
        return self.stores[self.KEY_DATA]

    @data.setter
    def data(self, new_data):
        self.stores[self.KEY_DATA] = new_data

    def open_job(self, statepoint=None, id=None):
        """Get a job handle associated with a statepoint.

        This method returns the job instance associated with
        the given statepoint or job id.
        Opening a job by a valid statepoint never fails.
        Opening a job by id requires a lookup of the statepoint
        from the job id, which may fail if the job was not
        previously initialized.

        :param statepoint: The job's unique set of parameters.
        :type statepoint: mapping
        :param id: The job id.
        :type id: str
        :return: The job instance.
        :rtype: :class:`~.Job`
        :raises KeyError:
            If the attempt to open the job by id fails.
        :raises LookupError: If the attempt to open the job by an
            abbreviated id returns more than one match.
        """
        if (id is None) == (statepoint is None):
            raise ValueError(
                "You need to either provide the statepoint or the id.")
        if id is None:
            # second best case
            job = self.Job(project=self, statepoint=statepoint)
            if job._id not in self._sp_cache:
                self._sp_cache[job._id] = dict(job._statepoint)
            return job
        elif id in self._sp_cache:
            # optimal case
            return self.Job(project=self, statepoint=self._sp_cache[id], _id=id)
        else:
            # worst case (no statepoint and cache miss)
            if len(id) < 32:
                job_ids = self.find_job_ids()
                matches = [_id for _id in job_ids if _id.startswith(id)]
                if len(matches) == 1:
                    id = matches[0]
                elif len(matches) > 1:
                    raise LookupError(id)
            return self.Job(project=self, statepoint=self.get_statepoint(id), _id=id)

    def _job_dirs(self):
        try:
            for d in os.listdir(self._wd):
                if JOB_ID_REGEX.match(d):
                    yield d
        except OSError as error:
            if error.errno == errno.ENOENT:
                if os.path.islink(self._wd):
                    raise WorkspaceError(
                        "The link '{}' pointing to the workspace is broken.".format(self._wd))
                elif not os.path.isdir(os.path.dirname(self._wd)):
                    logger.warning(
                        "The path to the workspace directory "
                        "('{}') does not exist.".format(os.path.dirname(self._wd)))
                else:
                    logger.info("The workspace directory '{}' does not exist!".format(self._wd))
            else:
                logger.error("Unable to access the workspace directory '{}'.".format(self._wd))
                raise WorkspaceError(error)

    def num_jobs(self):
        "Return the number of initialized jobs."
        # We simply count the the number of valid directories and avoid building a list
        # for improved performance.
        i = 0
        for i, _ in enumerate(self._job_dirs(), 1):
            pass
        return i

    __len__ = num_jobs

    def __contains__(self, job):
        """Determine whether job is in the project's data space.

        :param job: The job to test for initialization.
        :type job: :py:class:`~.Job`
        :returns: True when the job is initialized for this project.
        :rtype: bool
        """
        return job.get_id() in self.find_job_ids()

    def build_job_search_index(self, index, _trust=False):
        """Build a job search index.

        :param index: A document index.
        :type index: list
        :returns: A job search index based on the provided index.
        :rtype: :class:`~.JobSearchIndex`
        """
        return JobSearchIndex(index=index, _trust=_trust)

    def build_job_statepoint_index(self, exclude_const=False, index=None):
        """Build a statepoint index to identify jobs with specific parameters.

        This method generates pairs of state point keys and mappings of values
        to a set of all corresponding job ids. The pairs are ordered by the number
        of different values.
        Since state point keys may be nested, they are represented as a tuple.
        For example:

        .. code-block:: python

            >>> for i in range(4):
            ...     project.open_job({'a': i, 'b': {'c': i % 2}}).init()
            ...
            >>> for key, value in project.build_job_statepoint_index():
            ...     print(key)
            ...     pprint.pprint(value)
            ...
            ('b', 'c')
            defaultdict(<class 'set'>,
                        {0: {'3a530c13bfaf57517b4e81ecab6aec7f',
                             '4e9a45a922eae6bb5d144b36d82526e4'},
                         1: {'d49c6609da84251ab096654971115d0c',
                             '5c2658722218d48a5eb1e0ef7c26240b'}})
            ('a',)
            defaultdict(<class 'set'>,
                        {0: {'4e9a45a922eae6bb5d144b36d82526e4'},
                         1: {'d49c6609da84251ab096654971115d0c'},
                         2: {'3a530c13bfaf57517b4e81ecab6aec7f'},
                         3: {'5c2658722218d48a5eb1e0ef7c26240b'}})

        Values that are constant over the complete data space can be optionally
        ignored with the exclude_const argument set to True.

        :param exclude_const: Exclude entries that are shared by all jobs
            that are part of the index.
        :type exclude_const: bool
        :param index: A document index.
        :yields: Pairs of state point keys and mappings of values to a set of all
            corresponding job ids.
        """
        from .schema import _build_job_statepoint_index
        if index is None:
            index = [{'_id': job._id, 'statepoint': job.sp()} for job in self]
        for x in _build_job_statepoint_index(jobs=self, exclude_const=exclude_const, index=index):
            yield x

    def detect_schema(self, exclude_const=False, subset=None, index=None):
        """Detect the project's state point schema.

        :param exclude_const:
            Exclude all state point keys that are shared by all jobs within this project.
        :type exclude_const:
            bool
        :param subset:
            A sequence of jobs or job ids specifying a subset over which the state point
            schema should be detected.
        :param index:
            A document index.
        :returns:
            The detected project schema.
        :rtype:
            `signac.contrib.schema.ProjectSchema`
        """
        if index is None:
            index = self.index(include_job_document=False)
        if subset is not None:
            subset = {str(s) for s in subset}
            index = [doc for doc in index if doc['_id'] in subset]
        statepoint_index = self.build_job_statepoint_index(exclude_const=exclude_const, index=index)
        return ProjectSchema.detect(statepoint_index)

    def find_job_ids(self, filter=None, doc_filter=None, index=None):
        """Find the job_ids of all jobs matching the filters.

        The optional filter arguments must be a Mapping of key-value
        pairs and JSON serializable.

        .. note::
            Providing a pre-calculated index may vastly increase the
            performance of this function.

        :param filter: A mapping of key-value pairs that all
            indexed job statepoints are compared against.
        :type filter: Mapping
        :param doc_filter: A mapping of key-value pairs that all
            indexed job documents are compared against.
        :param index: A document index.
        :yields: The ids of all indexed jobs matching both filters.
        :raise TypeError: If the filters are not JSON serializable.
        :raises ValueError: If the filters are invalid.
        :raises RuntimeError: If the filters are not supported
            by the index.
        """
        if filter is None and doc_filter is None and index is None:
            return list(self._job_dirs())
        if index is None:
            if doc_filter is None:
                index = self._sp_index()
            else:
                index = self.index(include_job_document=True)
            search_index = JobSearchIndex(index, _trust=True)
        else:
            search_index = JobSearchIndex(index)
        return search_index.find_job_ids(filter=filter, doc_filter=doc_filter)

    def find_jobs(self, filter=None, doc_filter=None):
        """Find all jobs in the project's workspace.

        The optional filter arguments must be a Mapping of key-value pairs and
        JSON serializable. The `filter` argument is used to search against job
        statepoints, whereas the `doc_filter` argument compares against job
        document keys.

        :param filter: A mapping of key-value pairs that all
            indexed job statepoints are compared against.
        :type filter: Mapping
        :param doc_filter: A mapping of key-value pairs that all
            indexed job documents are compared against.
        :type doc_filter: Mapping
        :yields: Instances of :class:`~signac.contrib.job.Job`
        :raise TypeError: If the filters are not JSON serializable.
        :raises ValueError: If the filters are invalid.
        :raises RuntimeError: If the filters are not supported
            by the index.
        """
        return JobsCursor(self, filter, doc_filter)

    def __iter__(self):
        return iter(self.find_jobs())

    def groupby(self, key=None, default=None):
        """Groups jobs according to one or more statepoint parameters.
        This method can be called on any :class:`~.JobsCursor` such as
        the one returned by :meth:`find_jobs` or by iterating over a
        project. Examples:

        .. code-block:: python

            # Group jobs by statepoint parameter 'a'.
            for key, group in project.groupby('a'):
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

        If `key` is None, jobs are grouped by identity (by id), placing one job
        into each group.

        :param key:
            The statepoint grouping parameter(s) passed as a string, iterable of strings,
            or a function that will be passed one argument, the job.
        :type key:
            str, iterable, or function
        :param default:
            A default value to be used when a given state point key is not present (must
            be sortable).
        """
        return self.find_jobs().groupby(key, default=default)

    def groupbydoc(self, key=None, default=None):
        """Groups jobs according to one or more document values.
        This method can be called on any :class:`~.JobsCursor` such as
        the one returned by :meth:`find_jobs` or by iterating over a
        project. Examples:

        .. code-block:: python

            # Group jobs by document value 'a'.
            for key, group in project.groupbydoc('a'):
                print(key, list(group))

            # Find jobs where job.sp['a'] is 1 and group them
            # by job.document['b'] and job.document['c'].
            for key, group in project.find_jobs({'a': 1}).groupbydoc(('b', 'c')):
                print(key, list(group))

            # Group by whether 'd' is a field in the job.document using a lambda.
            for key, group in project.groupbydoc(lambda doc: 'd' in doc):
                print(key, list(group))

        If `key` is None, jobs are grouped by identity (by id), placing one job
        into each group.

        :param key:
            The statepoint grouping parameter(s) passed as a string, iterable of strings,
            or a function that will be passed one argument, :attr:`Job.document`.
        :type key:
            str, iterable, or function
        :param default:
            A default value to be used when a given state point key is not present (must
            be sortable).
        """
        return self.find_jobs().groupbydoc(key, default=default)

    def to_dataframe(self, *args, **kwargs):
        """Export the project metadata to a pandas dataframe.

        The arguments to this function are forwarded to :py:meth:`.JobsCursor.to_dataframe`.
        """
        return self.find_jobs().to_dataframe(*args, **kwargs)

    def read_statepoints(self, fn=None):
        """Read all statepoints from a file.

        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.Project.FN_STATEPOINTS`.
        :type fn: str

        See also :meth:`dump_statepoints` and :meth:`write_statepoints`.
        """
        if fn is None:
            fn = self.fn(self.FN_STATEPOINTS)
        # See comment in write statepoints.
        with open(fn, 'r') as file:
            return json.loads(file.read())

    def dump_statepoints(self, statepoints):
        """Dump the statepoints and associated job ids.

        Equivalent to:

        .. code-block:: python

            {project.open_job(sp).get_id(): sp for sp in statepoints}

        :param statepoints: A list of statepoints.
        :type statepoints: iterable
        :return: A mapping, where the key is the job id
                 and the value is the statepoint.
        :rtype: dict
        """
        return {calc_id(sp): sp for sp in statepoints}

    def write_statepoints(self, statepoints=None, fn=None, indent=2):
        """Dump statepoints to a file.

        If the file already contains statepoints, all new statepoints
        will be appended, while the old ones are preserved.

        :param statepoints: A list of statepoints,
            defaults to all statepoints which are defined in the workspace.
        :type statepoints: iterable
        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str
        :param indent: Specify the indentation of the json file.
        :type indent: int

        See also :meth:`dump_statepoints`.
        """
        if fn is None:
            fn = self.fn(self.FN_STATEPOINTS)
        try:
            tmp = self.read_statepoints(fn=fn)
        except IOError as error:
            if not error.errno == errno.ENOENT:
                raise
            tmp = dict()
        if statepoints is None:
            job_ids = self._job_dirs()
            _cache = {_id: self.get_statepoint(_id) for _id in job_ids}
        else:
            _cache = {calc_id(sp): sp for sp in statepoints}

        tmp.update(_cache)
        logger.debug("Writing state points file with {} entries.".format(len(tmp)))
        with open(fn, 'w') as file:
            file.write(json.dumps(tmp, indent=indent))

    def _register(self, job):
        "Register the job within the local index."
        self._sp_cache[job._id] = dict(job._statepoint)

    def _get_statepoint_from_workspace(self, jobid):
        "Attempt to read the statepoint from the workspace."
        fn_manifest = os.path.join(self._wd, jobid, self.Job.FN_MANIFEST)
        try:
            with open(fn_manifest, 'rb') as manifest:
                return json.loads(manifest.read().decode())
        except (IOError, ValueError) as error:
            if os.path.isdir(os.path.join(self._wd, jobid)):
                logger.error(
                    "Error while trying to access state "
                    "point manifest file of job '{}': '{}'.".format(jobid, error))
                raise JobsCorruptedError([jobid])
            raise KeyError(jobid)

    def get_statepoint(self, jobid, fn=None):
        """Get the statepoint associated with a job id.

        The state point is retrieved from the internal cache, from
        the workspace or from a state points file.

        :param jobid:
            A job id to get the statepoint for.
        :type jobid:
            str
        :param fn:
            The filename of the file containing the statepoints, defaults
            to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn:
            str
        :return:
            The state point corresponding to jobid.
        :rtype:
            dict
        :raises KeyError:
            If the state point associated with jobid could not be found.
        :raises JobsCorruptedError:
            If the state point manifest file corresponding to jobid is
            inaccessible or corrupted.
        """
        if not self._sp_cache:
            self._read_cache()
        try:
            if jobid in self._sp_cache:
                return self._sp_cache[jobid]
            else:
                self._sp_cache_misses += 1
                if not self._sp_cache_warned and\
                        self._sp_cache_misses > self._sp_cache_miss_warning_threshold:
                    logger.debug(
                        "High number of state point cache misses. Consider "
                        "to update cache with the Project.update_cache() method.")
                    self._sp_cache_warned = True
                sp = self._get_statepoint_from_workspace(jobid)
        except KeyError as error:
            try:
                sp = self.read_statepoints(fn=fn)[jobid]
            except IOError as io_error:
                if io_error.errno != errno.ENOENT:
                    raise io_error
                else:
                    raise error
        self._sp_cache[jobid] = sp
        return sp

    def create_linked_view(self, prefix=None, job_ids=None, index=None, path=None):
        """Create or update a persistent linked view of the selected data space.

        Similar to :meth:`~.export_to`, this function expands the data space for the selected
        jobs, but instead of copying data will create symbolic links to the individual job
        workspace directories. This is primarily useful for browsing through the data
        space using a file-browser with human-interpretable directory paths.

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
        the exact same manner as the equivalent argument for :meth:`~.Project.export_to`.

        .. note::

            The behavior of this function is almost equivalent to
            ``project.export_to('my_view', copytree=os.symlink)`` with the major difference,
            that view hierarchies are actually *updated*, that means no longer valid links
            are automatically removed.

        :param prefix:
            The path where the linked view will be created or updated.
        :type prefix:
            str
        :param job_ids:
            If None (the default), create the view for the complete data space,
            otherwise only for the sub space constituted by the provided job ids.
        :param index:
            A document index.
        :param path:
            The path (function) used to structure the linked data space.
        :returns:
            A dict that maps the source directory paths, to the linked
            directory paths.
        """
        from .linked_view import create_linked_view
        return create_linked_view(self, prefix, job_ids, index, path)

    def reset_statepoint(self, job, new_statepoint):
        """Reset the state point of job.

        .. danger::

            Use this function with caution! Resetting a job's state point,
            may sometimes be necessary, but can possibly lead to incoherent
            data spaces.

        :param job: The job, that should be reset to a new state point.
        :type job: :class:`~.contrib.job.Job`
        :param new_statepoint: The job's new state point.
        :type new_statepoint: mapping
        :raises DestinationExistsError:
            If a job associated with the new state point is already initialized.
        :raises OSError:
            If the move failed due to an unknown system related error.
        """
        job.reset_statepoint(new_statepoint=new_statepoint)

    def update_statepoint(self, job, update, overwrite=False):
        """Update the statepoint of this job.

        .. warning::

            While appending to a job's state point is generally safe,
            modifying existing parameters may lead to data
            inconsistency. Use the overwrite argument with caution!

        :param job: The job, whose statepoint shall be updated.
        :type job: :class:`~.contrib.job.Job`
        :param update: A mapping used for the statepoint update.
        :type update: mapping
        :param overwrite:
            Set to true, to ignore whether this update overwrites parameters,
            which are currently part of the job's state point. Use with caution!
        :raises KeyError:
            If the update contains keys, which are already part of the job's
            state point and overwrite is False.
        :raises DestinationExistsError:
            If a job associated with the new state point is already initialized.
        :raises OSError:
            If the move failed due to an unknown system related error.
        """
        job.update_statepoint(update=update, overwrite=overwrite)

    def clone(self, job, copytree=syncutil.copytree):
        """Clone job into this project.

        Create an identical copy of job within this project.

        :param job: The job to copy into this project.
        :type job: :py:class:`~.Job`
        :returns: The job instance corresponding to the copied job.
        :rtype: :class:`~.Job`
        :raises DestinationExistsError:
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
                raise ValueError("Source job not initalized.")
            else:
                raise
        return dst

    def sync(self, other, strategy=None, exclude=None, doc_sync=None, selection=None, **kwargs):
        """Synchronize this project with the other project.

        Try to clone all jobs from the other project to this project.
        If a job is already part of this project, try to synchronize the job
        using the optionally specified strategies.

        :param other:
            The other project to synchronize this project with.
        :type other:
            :py:class:`~.Project`
        :param strategy:
            A file synchronization strategy.
        :param exclude:
            Files with names matching the given pattern will be excluded
            from the synchronization.
        :param doc_sync:
            The function applied for synchronizing documents.
        :param selection:
            Only sync the given jobs.
        :param kwargs:
            This method accepts the same keyword arguments as the :func:`~.sync.sync_projects`
            function.
        :raises DocumentSyncConflict:
            If there are conflicting keys within the project or job documents that cannot
            be resolved with the given strategy or if there is no strategy provided.
        :raises FileSyncConflict:
            If there are differing files that cannot be resolved with the given strategy
            or if no strategy is provided.
        :raises SyncSchemaConflict:
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
            **kwargs)

    def export_to(self, target, path=None, copytree=None):
        """Export all jobs to a target location, such as a directory or a (compressed) archive file.

        Use this function in combination with :meth:`~.find_jobs` to export only a select number
        of jobs, for example:

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
        ``job._id``, and ``job.ws`` can also be used as path fields.

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

        Finally, providing ``path=False`` is equivalent to ``path="{job._id}"``.

        .. seealso::

            Previously exported or non-signac data spaces can be imported
            with :meth:`~.import_from`.

        :param target:
            A path to a directory to export to. The target can not already exist.
            Besides directories, possible targets are tar files (`.tar`), gzipped tar files
            (`.tar.gz`), zip files (`.zip`), bzip2-compressed files (`.bz2`),
            and xz-compressed files (`.xz`).
        :param path:
            The path (function) used to structure the exported data space.
            This argument must either be a callable which returns a path (str) as a function
            of `job`, a string where fields are replaced using the job-state point dictionary,
            or `False`, which means that we just use the job-id as path.
            Defaults to the equivalent of ``{{auto}}``.
        :param copytree:
            The function used for the actual copying of directory tree
            structures. Defaults to :func:`shutil.copytree`.
            Can only be used when the target is a directory.
        :returns:
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
        The default schema function will simply look for state point manifest files--usually named
        ``signac_statepoint.json``--and then import all data located within that path into the job
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

        .. seealso:: Export the project data space with :meth:`~.export_to`.

        :param origin:
            The path to the data space origin, which is to be imported. This may be a path to
            a directory, a zip file, or a tarball archive.
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
        from .import_export import import_into_project
        if sync:
            with self.temporary_project() as tmp_project:
                ret = tmp_project.import_from(origin=origin, schema=schema)
                if sync is True:
                    self.sync(other=tmp_project)
                else:
                    self.sync(other=tmp_project, **sync)
                return ret

        paths = dict(import_into_project(
            origin=origin, project=self, schema=schema, copytree=copytree))
        return paths

    def check(self, job_ids=None):
        """Check the project's workspace for corruption.

        :param job_ids:
            The ids of jobs to check, defaults to all jobs.
        :raises JobsCorruptedError:
            When one or more jobs are identified as corrupted.
        """
        corrupted = []
        logger.info("Checking workspace for corruption...")
        for job_id in self.find_job_ids():
            try:
                sp = self.get_statepoint(job_id)
                if calc_id(sp) != job_id:
                    corrupted.append(job_id)
                else:
                    self.open_job(sp).init()
            except JobsCorruptedError as error:
                corrupted.extend(error.job_ids)
        if corrupted:
            logger.error(
                "At least one job appears to be corrupted. Call Project.repair() "
                "to try to fix errors.".format(len(corrupted)))
            raise JobsCorruptedError(corrupted)

    def repair(self, fn_statepoints=None, index=None, job_ids=None):
        """Attempt to repair the workspace after it got corrupted.

        This method will attempt to repair lost or corrupted job state point
        manifest files using a state points file or a document index or both.

        :param fn_statepoints:
            The filename of the file containing the statepoints, defaults
            to :const:`~signac.contrib.project.Project.FN_STATEPOINTS`.
        :type fn_statepoints:
            str
        :param index:
            A document index
        :param job_ids:
            An iterable of job ids that should get repaired. Defaults to all jobs.
        :raises JobsCorruptedError:
            When one or more corrupted job could not be repaired.
        """
        if job_ids is None:
            job_ids = self.find_job_ids()

        # Load internal cache from all available external sources.
        self._read_cache()
        try:
            self._sp_cache.update(self.read_statepoints(fn=fn_statepoints))
        except IOError as error:
            if error.errno != errno.ENOENT or fn_statepoints is not None:
                raise
        if index is not None:
            for doc in index:
                self._sp_cache[doc['signac_id']] = doc['statepoint']

        corrupted = []
        for job_id in job_ids:
            try:
                # First, check if we can look up the state point.
                sp = self.get_statepoint(job_id)
                # Check if state point and id correspond.
                correct_id = calc_id(sp)
                if correct_id != job_id:
                    logger.warning(
                        "The job id of job '{}' is incorrect; "
                        "it should be '{}'.".format(job_id, correct_id))
                    invalid_wd = os.path.join(self.workspace(), job_id)
                    correct_wd = os.path.join(self.workspace(), correct_id)
                    try:
                        os.rename(invalid_wd, correct_wd)
                    except OSError as error:
                        logger.critical(
                            "Unable to fix location of job with "
                            " id '{}': '{}'.".format(job_id, error))
                        corrupted.append(job_id)
                        continue
                    else:
                        logger.info("Moved job to correct workspace.")

                job = self.open_job(sp)
            except KeyError:
                logger.critical("Unable to lookup state point for job with id '{}'.".format(job_id))
                corrupted.append(job_id)
            else:
                try:
                    # Try to reinit the job (triggers state point manifest file check).
                    job.init()
                except Exception as error:
                    logger.error(
                        "Error during initalization of job with "
                        "id '{}': '{}'.".format(job_id, error))
                    try:    # Attempt to fix the job manifest file.
                        job.init(force=True)
                    except Exception as error2:
                        logger.critical(
                            "Unable to force init job with id "
                            "'{}': '{}'.".format(job_id, error2))
                        corrupted.append(job_id)
        if corrupted:
            raise JobsCorruptedError(corrupted)

    def _sp_index(self):
        job_ids = set(self._job_dirs())
        to_add = job_ids.difference(self._index_cache)
        to_remove = set(self._index_cache).difference(job_ids)
        for _id in to_remove:
            del self._index_cache[_id]
        for _id in to_add:
            self._index_cache[_id] = dict(statepoint=self.get_statepoint(_id), _id=_id)
        return self._index_cache.values()

    def _build_index(self, include_job_document=False):
        "Return a basic state point index."
        wd = self.workspace() if self.Job is Job else None
        for _id in self.find_job_ids():
            doc = dict(_id=_id, statepoint=self.get_statepoint(_id))
            if include_job_document:
                if wd is None:
                    doc.update(self.open_job(id=_id).document)
                else:   # use optimized path
                    try:
                        with open(os.path.join(wd, _id, self.Job.FN_DOCUMENT), 'rb') as file:
                            doc.update(json.loads(file.read().decode()))
                    except IOError as error:
                        if error.errno != errno.ENOENT:
                            raise
            yield doc

    def _update_in_memory_cache(self):
        "Update the in-memory state point cache to reflect the workspace."
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

            to_add_chunks = split_and_print_progress(
                iterable=list(to_add),
                num_chunks=max(1, min(100, int(len(to_add) / 1000))),
                write=logger.info,
                desc="Read metadata: ")

            if six.PY2:
                pool = ThreadPool()
                for chunk in to_add_chunks:
                    pool.map(_add, chunk)
            else:
                with ThreadPool() as pool:
                    for chunk in to_add_chunks:
                        pool.map(_add, chunk)

            delta = time.time() - start
            logger.debug("Updated in-memory cache in {:.3f} seconds.".format(delta))
            return to_add, to_remove
        else:
            logger.debug("In-memory cache is up to date.")

    def _remove_persistent_cache_file(self):
        "Remove the persistent cache file (if it exists)."
        try:
            os.remove(self.fn(self.FN_CACHE))
        except (OSError, IOError) as error:
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
        logger.info('Update cache...')
        start = time.time()
        cache = self._read_cache()
        self._update_in_memory_cache()
        if cache is None or set(cache) != set(self._sp_cache):
            fn_cache = self.fn(self.FN_CACHE)
            fn_cache_tmp = fn_cache + '~'
            try:
                with gzip.open(fn_cache_tmp, 'wb') as cachefile:
                    cachefile.write(json.dumps(self._sp_cache).encode())
            except OSError:  # clean-up
                try:
                    os.remove(fn_cache_tmp)
                except (OSError, IOError):
                    pass
                raise
            else:
                if six.PY2:
                    os.rename(fn_cache_tmp, fn_cache)
                else:
                    os.replace(fn_cache_tmp, fn_cache)
            delta = time.time() - start
            logger.info("Updated cache in {:.3f} seconds.".format(delta))
            return len(self._sp_cache)
        else:
            logger.info("Cache is up to date.")

    def _read_cache(self):
        "Read the persistent state point cache (if available)."
        logger.debug("Reading cache...")
        start = time.time()
        try:
            with gzip.open(self.fn(self.FN_CACHE), 'rb') as cachefile:
                cache = json.loads(cachefile.read().decode())
            self._sp_cache.update(cache)
        except IOError as error:
            if not error.errno == errno.ENOENT:
                raise
            logger.debug("No cache file found.")
        else:
            delta = time.time() - start
            logger.debug("Read cache in {:.3f} seconds.".format(delta))
            return cache

    def index(self, formats=None, depth=0,
              skip_errors=False, include_job_document=True):
        r"""Generate an index of the project's workspace.

        This generator function indexes every file in the project's
        workspace until the specified `depth`.
        The job document if it exists, is always indexed, other
        files need to be specified with the formats argument.

        .. code-block:: python

            for doc in project.index({r'.*\.txt', 'TextFile'}):
                print(doc)

        :param formats: The format definitions as mapping.
        :type formats: dict
        :param depth: Specifies the crawling depth.
            A value of 0 (default) means no limit.
        :type depth: int
        :param skip_errors: Skip all errors which occur during indexing.
            This is useful when trying to repair a broken workspace.
        :type skip_errors: bool
        :param include_job_document: Include the contents of job
            documents.
        :type include_job_document: bool
        :yields: index documents"""
        if formats is None:
            root = self.workspace()

            def _full_doc(doc):
                doc['signac_id'] = doc['_id']
                doc['root'] = root
                return doc

            docs = self._build_index(include_job_document=include_job_document)
            docs = map(_full_doc, docs)
        else:
            if six.PY2:
                if isinstance(formats, basestring):  # noqa
                    formats = {formats: 'File'}
            else:
                if isinstance(formats, str):
                    formats = {formats: 'File'}

            class Crawler(SignacProjectCrawler):
                pass
            for pattern, fmt in formats.items():
                Crawler.define(pattern, fmt)
            crawler = Crawler(self.root_directory())
            docs = crawler.crawl(depth=depth)
        if skip_errors:
            docs = _skip_errors(docs, logger.critical)
        for doc in docs:
            yield doc

    def create_access_module(self, filename=None, master=True):
        """Create the access module for indexing

        This method generates the access module required to make
        this project's index part of a master index.

        :param filename: The name of the access module file.
            Defaults to the standard name and should usually
            not be changed.
        :type filename: str
        :param master: If True, add directives for the compilation
            of a master index when executing the module.
        :type master: bool
        :returns: The name of the created access module.
        :rtype: str
        """
        if filename is None:
            filename = os.path.join(
                self.root_directory(),
                MasterCrawler.FN_ACCESS_MODULE)
        with open(filename, 'wx' if six.PY2 else 'x') as file:
            if master:
                file.write(ACCESS_MODULE_MASTER)
            else:
                file.write(ACCESS_MODULE_MINIMAL)
        if master:
            mode = os.stat(filename).st_mode | stat.S_IEXEC
            os.chmod(filename, mode)
        logger.info("Created access module file '{}'.".format(filename))
        return filename

    @contextmanager
    def temporary_project(self, name=None, dir=None):
        """Context manager for the initialization of a temporary project.

        The temporary project is by default created within the root project's
        workspace to ensure that they share the same file system. This is an example
        for how this method can be used for the import and synchronization of
        external data spaces.

        .. code-block:: python

            with project.temporary_project() as tmp_project:
                tmp_project.import_from('/data')
                project.sync(tmp_project)

        :param name:
            An optional name for the temporary project.
            Defaults to a unique random string.
        :param dir:
            Optionally specify where the temporary project root directory is to be
            created. Defaults to the project's workspace directory.
        :returns:
            An instance of :class:`.Project`.
        """
        if name is None:
            name = os.path.join(self.get_id(), str(uuid.uuid4()))
        if dir is None:
            dir = self.workspace()
        _mkdir_p(self.workspace())  # ensure workspace exists
        with TemporaryProject(name=name, cls=type(self), dir=dir) as tmp_project:
            yield tmp_project

    @classmethod
    def init_project(cls, name, root=None, workspace=None, make_dir=True):
        """Initialize a project with the given name.

        It is safe to call this function multiple times with
        the same arguments.
        However, a :class:`RuntimeError` is raised in case where an
        existing project configuration would conflict with
        the provided initialization parameters.

        :param name: The name of the project to initialize.
        :type name: str
        :param root: The root directory for the project.
            Defaults to the current working directory.
        :type root: str
        :param workspace: The workspace directory for the project.
            Defaults to `$project_root/workspace`.
        :type workspace: str
        :param make_dir: Create the project root directory, if
            it does not exist yet.
        :type make_dir: bool
        :returns: The project handle of the initialized project.
        :rtype: :py:class:`~.Project`
        :raises RuntimeError: If the project root path already
            contains a conflicting project configuration."""
        if root is None:
            root = os.getcwd()
        try:
            project = cls.get_project(root=root, search=False)
        except LookupError:
            fn_config = os.path.join(root, 'signac.rc')
            if make_dir:
                _mkdir_p(os.path.dirname(fn_config))
            with open(fn_config, 'a') as config_file:
                config_file.write('project={}\n'.format(name))
                if workspace is not None:
                    config_file.write('workspace_dir={}\n'.format(workspace))
            project = cls.get_project(root=root)
            assert project.get_id() == str(name)
            return project
        else:
            try:
                assert project.get_id() == str(name)
                if workspace is not None:
                    assert os.path.realpath(workspace) \
                        == os.path.realpath(project.workspace())
                return project
            except AssertionError:
                raise RuntimeError(
                    "Failed to initialize project '{}'. Path '{}' already "
                    "contains a conflicting project configuration.".format(
                        name, os.path.abspath(root)))

    @classmethod
    def get_project(cls, root=None, search=True):
        """Find a project configuration and return the associated project.

        :param root:
            The starting point to search for a project, defaults to the
            current working directory.
        :type root: str
        :param search:
            If True, search for project configurations inside and above
            the specified root directory, otherwise only return projects
            with a root directory identical to the specified root argument.
        :type search: bool
        :returns: The project handle.
        :raises LookupError: If no project configuration can be found.
        """
        if root is None:
            root = os.getcwd()
        config = load_config(root=root, local=False)
        if 'project' not in config or \
                (not search and os.path.realpath(config['project_dir']) != os.path.realpath(root)):
            raise LookupError(
                "Unable to determine project id for path '{}'.".format(os.path.abspath(root)))
        return cls(config=config)

    @classmethod
    def get_job(cls, root=None):
        """Find a Job in or above the current working directory (or provided path).

        :param root: The job root directory.
            If no root directory is given, the current working directory is
            assumed to be the job directory.
        :type root: str
        :returns: The job handle.
        :raises LookupError: If this job cannot be found."""
        if root is None:
            root = os.getcwd()
        root = os.path.abspath(root)

        # Ensure the root path exists, which is not guaranteed by the regex match
        if not os.path.exists(root):
            raise LookupError("Path does not exist: '{}'.".format(root))

        # Find the last match instance of a job id
        results = list(re.finditer(JOB_ID_REGEX, root))
        if len(results) == 0:
            raise LookupError("Could not find a job id in path '{}'.".format(root))
        match = results[-1]
        job_id = match.group(0)
        job_root = root[:match.end()]

        # Find a project *above* the root directory (avoid finding nested projects)
        project = cls.get_project(os.path.join(job_root, os.pardir))

        # Return the matched job id from the found project
        return project.open_job(id=job_id)


@contextmanager
def TemporaryProject(name=None, cls=None, **kwargs):
    """Context manager for the generation of a temporary project.

    This is a factory function that creates a Project within a temporary directory
    and must be used as context manager, for example like this:

    .. code-block:: python

        with TemporaryProject() as tmp_project:
            tmp_project.import_from('/data')

    :param name:
        An optional name for the temporary project.
        Defaults to a unique random string.
    :param cls:
        The class of the temporary project.
        Defaults to :class:`.Project`.
    :param kwargs:
        Optional key-word arguments that are forwarded to the TemporaryDirectory class
        constructor, which is used to create a temporary root directory.
    :returns:
        An instance of :class:`.Project`.
    """
    if name is None:
        name = str(uuid.uuid4())
    if cls is None:
        cls = Project
    with TemporaryDirectory(**kwargs) as tmp_dir:
        yield cls.init_project(name=name, root=tmp_dir)


def _skip_errors(iterable, log=print):
    while True:
        try:
            yield next(iterable)
        except StopIteration:
            return
        except Exception as error:
            log(error)


class _JobsCursorIterator(object):

    def __init__(self, project, ids):
        self._project = project
        self._ids = ids
        self._ids_iterator = iter(ids)

    def __next__(self):
        return self._project.open_job(id=next(self._ids_iterator))

    next = __next__  # Python 2.7 compatibility

    def __iter__(self):
        return type(self)(self._project, self._ids)


class JobsCursor(object):
    """An iterator over a search query result, enabling simple iteration and
    grouping operations.
    """
    _use_pandas_for_html_repr = True  # toggle use of pandas for html repr

    def __init__(self, project, filter, doc_filter):
        self._project = project
        self._filter = filter
        self._doc_filter = doc_filter

        # This private attribute allows us to implement the deprecated
        # next() method for this class.
        self._next_iter = None

    def __len__(self):
        # Highly performance critical code path!!
        if self._filter or self._doc_filter:
            # We use the standard function for determining job ids if and only if
            # any of the two filter is provided.
            return len(self._project.find_job_ids(self._filter, self._doc_filter))
        else:
            # Without filter we can simply return the length of the whole project.
            return self._project.__len__()

    def __iter__(self):
        # Code duplication here for improved performance.
        return _JobsCursorIterator(
            self._project,
            self._project.find_job_ids(self._filter, self._doc_filter),
            )

    def next(self):
        """Return the next element.

        This function is deprecated, users should use iter(..).next() instead!

        .. deprecated:: 0.9.6
        """
        warnings.warn("Calling next() directly on a JobsCursor is deprecated!", DeprecationWarning)
        if self._next_iter is None:
            self._next_iter = iter(self)
        try:
            return self._next_iter.next()
        except StopIteration:
            self._next_iter = None
            raise

    def groupby(self, key=None, default=None):
        """Groups jobs according to one or more statepoint parameters.
        This method can be called on any :class:`~.JobsCursor` such as
        the one returned by :meth:`find_jobs` or by iterating over a
        project. Examples:

        .. code-block:: python

            # Group jobs by statepoint parameter 'a'.
            for key, group in project.groupby('a'):
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

        If `key` is None, jobs are grouped by identity (by id), placing one job
        into each group.

        :param key:
            The statepoint grouping parameter(s) passed as a string, iterable of strings,
            or a function that will be passed one argument, the job.
        :type key:
            str, iterable, or function
        :param default:
            A default value to be used when a given state point key is not present (must
            be sortable).
        """
        if isinstance(key, six.string_types):
            if default is None:
                def keyfunction(job):
                    return job.sp[key]
            else:
                def keyfunction(job):
                    return job.sp.get(key, default)

        elif isinstance(key, Iterable):
            if default is None:
                def keyfunction(job):
                    return tuple(job.sp[k] for k in key)
            else:
                def keyfunction(job):
                    return tuple(job.sp.get(k, default) for k in key)

        elif key is None:
            # Must return a type that can be ordered with <, >
            def keyfunction(job):
                return str(job)

        else:
            keyfunction = key

        return groupby(sorted(iter(self), key=keyfunction), key=keyfunction)

    def groupbydoc(self, key=None, default=None):
        """Groups jobs according to one or more document values.
        This method can be called on any :class:`~.JobsCursor` such as
        the one returned by :meth:`find_jobs` or by iterating over a
        project. Examples:

        .. code-block:: python

            # Group jobs by document value 'a'.
            for key, group in project.groupbydoc('a'):
                print(key, list(group))

            # Find jobs where job.sp['a'] is 1 and group them
            # by job.document['b'] and job.document['c'].
            for key, group in project.find_jobs({'a': 1}).groupbydoc(('b', 'c')):
                print(key, list(group))

            # Group by whether 'd' is a field in the job.document using a lambda.
            for key, group in project.groupbydoc(lambda doc: 'd' in doc):
                print(key, list(group))

        If `key` is None, jobs are grouped by identity (by id), placing one job
        into each group.

        :param key:
            The statepoint grouping parameter(s) passed as a string, iterable of strings,
            or a function that will be passed one argument, :attr:`job.document`.
        :type key:
            str, iterable, or function
        :param default:
            A default value to be used when a given state point key is not present (must
            be sortable).
        """
        if isinstance(key, six.string_types):
            if default is None:
                def keyfunction(job):
                    return job.document[key]
            else:
                def keyfunction(job):
                    return job.document.get(key, default)
        elif isinstance(key, Iterable):
            if default is None:
                def keyfunction(job):
                    return tuple(job.document[k] for k in key)
            else:
                def keyfunction(job):
                    return tuple(job.document.get(k, default) for k in key)
        elif key is None:
            # Must return a type that can be ordered with <, >
            def keyfunction(job):
                return str(job)
        else:
            # Pass the job document to lambda functions
            def keyfunction(job):
                return key(job.document)
        return groupby(sorted(iter(self), key=keyfunction), key=keyfunction)

    def export_to(self, target, path=None, copytree=None):
        """Export all jobs to a target location, such as a directory or a (zipped) archive file.

        See help(signac.Project.export_to) for full details on how to use this function.
        """
        from .import_export import export_jobs
        return dict(export_jobs(jobs=list(self), target=target,
                                path=path, copytree=copytree))

    def to_dataframe(self, sp_prefix='sp.', doc_prefix='doc.'):
        """Convert the selection of jobs to a pandas dataframe.

        This function exports the job metadata to a :py:class:`pandas.DataFrame`.
        All state point and document keys are prefixed by default to be able to distinguish them.

        :param sp_prefix:
            Prefix state point keys with the given string. Defaults to "sp.".
        :type sp_prefix:
            str
        :param doc_prefix:
            Prefix document keys with the given string. Defaults to "doc.".
        :type doc_prefix:
            str
        :returns:
            A pandas dataframe with all job metadata.
        :rtype:
            :py:class:`pandas.DataFrame`
        """
        import pandas

        def _export_sp_and_doc(job):
            for key, value in job.sp.items():
                yield sp_prefix + key, value
            for key, value in job.doc.items():
                yield doc_prefix + key, value

        return pandas.DataFrame.from_dict(
            data={job._id: dict(_export_sp_and_doc(job)) for job in self},
            orient='index').infer_objects()

    def __repr__(self):
        return "{type}({{'project': '{project}', 'filter': '{filter}',"\
               " 'docfilter': '{doc_filter}'}})".format(
                   type=self.__class__.__module__ + '.' + self.__class__.__name__,
                   project=self._project,
                   filter=self._filter,
                   doc_filter=self._doc_filter)

    def _repr_html_jobs(self):
        html = ''
        len_self = len(self)
        try:
            if len_self > 100:
                raise RuntimeError  # too large
            if self._use_pandas_for_html_repr:
                import pandas
            else:
                raise RuntimeError
        except ImportError:
            warnings.warn('Install pandas for a pretty representation of jobs.')
            html += '<br/><strong>{}</strong> job(s) found'.format(len_self)
        except RuntimeError:
            html += '<br/><strong>{}</strong> job(s) found'.format(len_self)
        else:
            with pandas.option_context("display.max_rows", 20):
                html += self.to_dataframe()._repr_html_()
        return html

    def _repr_html_(self):
        """Returns an HTML representation of JobsCursor."""
        return repr(self) + self._repr_html_jobs()


def init_project(name, root=None, workspace=None, make_dir=True):
    """Initialize a project with the given name.

    It is safe to call this function multiple times with
    the same arguments.
    However, a :class:`RuntimeError` is raised in case where an
    existing project configuration would conflict with
    the provided initialization parameters.

    :param name: The name of the project to initialize.
    :type name: str
    :param root: The root directory for the project.
        Defaults to the current working directory.
    :type root: str
    :param workspace: The workspace directory for the project.
        Defaults to `$project_root/workspace`.
    :type workspace: str
    :param make_dir: Create the project root directory, if
        it does not exist yet.
    :type make_dir: bool
    :returns: The project handle of the initialized project.
    :rtype: :py:class:`~.Project`
    :raises RuntimeError: If the project root path already
        contains a conflicting project configuration."""
    return Project.init_project(name=name, root=root, workspace=workspace, make_dir=make_dir)


def get_project(root=None, search=True):
    """Find a project configuration and return the associated project.

    :param root:
        The starting point to search for a project, defaults to the
        current working directory.
    :type root: str
    :param search:
        If True, search for project configurations inside and above
        the specified root directory, otherwise only return projects
        with a root directory identical to the specified root argument.
    :type search: bool
    :returns: The project handle.
    :rtype: :py:class:`~.Project`
    :raises LookupError: If no project configuration can be found.
    """
    return Project.get_project(root=root, search=search)


def get_job(root=None):
    """Find a Job in or above the current working directory (or provided path).

    :param root: The job root directory.
        If no root directory is given, the current working directory is
        assumed to be within the current job workspace directory.
    :type root: str
    :returns: The job handle.
    :raises LookupError: If this job cannot be found.

    For example, when the current directory is a job workspace directory:

    .. code-block:: python

        >>> signac.get_job()
        signac.contrib.job.Job(project=..., statepoint={...})

    """
    return Project.get_job(root=root)
