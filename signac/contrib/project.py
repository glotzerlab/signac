# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import os
import stat
import re
import logging
import errno
import collections
import shutil
from itertools import chain

from ..core.json import json
from .collection import Collection
from ..common import six
from ..common.config import load_config
from .job import Job
from .hashing import calc_id
from .indexing import _index_signac_project_workspace
from .indexing import SignacProjectCrawler
from .indexing import MasterCrawler
from .utility import _mkdir_p
from .errors import DestinationExistsError

logger = logging.getLogger(__name__)

#: The default filename to read from and write statepoints to.
FN_STATEPOINTS = 'signac_statepoints.json'

ACCESS_MODULE_MINIMAL = """import signac

def get_indexes(root):
    yield signac.get_project(root).index()
"""

ACCESS_MODULE_MASTER = """#!/usr/bin/env python
# -*- condig: utf-8 -*-
import signac

def get_indexes(root):
    yield signac.get_project(root).index()

if __name__ == '__main__':
    with signac.Collection.open('index.txt') as index:
        signac.export(signac.index(), index, update=True)
"""


class JobSearchIndex(object):
    """Search for sepcific jobs with filters.

    The JobSearchIndex allows to search for job_ids,
    that are part of an index, which match specific
    statepoint filters or job document filters.

    :param index: A document index.
    """

    def __init__(self, index):
        self._collection = Collection(index)

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
        if filter is None:
            f = dict()
        else:
            f = dict(self._resolve_statepoint_filter(filter))
        if doc_filter is not None:
            f.update(doc_filter)
        return self._collection._find(f)


class Project(object):
    """The handle on a signac project.

    Application developers should usually not need to
    directly instantiate this class, but use
    :func:`signac.get_project` instead."""
    Job = Job

    def __init__(self, config=None):
        if config is None:
            config = load_config()
        self._config = config
        self.get_id()

    def __str__(self):
        "Returns the project's id."
        return str(self.get_id())

    def __repr__(self):
        return "{type}({{'project': '{id}', 'project_dir': '{rd}',"\
               " 'workspace_dir': '{wd}'}})".format(
                    type=self.__class__.__module__ + '.' + self.__class__.__name__,
                    id=self.get_id(),
                    rd=self.root_directory(),
                    wd=self.workspace())

    def __eq__(self, other):
        return repr(self) == repr(other)

    @property
    def config(self):
        "The project's configuration."
        return self._config

    def root_directory(self):
        "Returns the project's root directory."
        return self._config['project_dir']

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
        wd = os.path.expandvars(self._config.get('workspace_dir', 'workspace'))
        if os.path.isabs(wd):
            return wd
        else:
            return os.path.join(self.root_directory(), wd)

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

    def open_job(self, statepoint=None, id=None):
        """Get a job handle associated with a statepoint.

        This method returns the job instance associated with
        the given statepoint or job id.
        Opening a job by a valid statepoint never fails.
        Opening a job by id, requires a lookup of the statepoint
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
            return self.Job(project=self, statepoint=statepoint)
        else:
            if len(id) < 32:
                job_ids = self.find_job_ids()
                matches = [_id for _id in job_ids if _id.startswith(id)]
                if len(matches) == 1:
                    id = matches[0]
                elif len(matches) > 1:
                    raise LookupError(id)
            return self.Job(project=self, statepoint=self.get_statepoint(id))

    def _job_dirs(self):
        wd = self.workspace()
        m = re.compile('[a-f0-9]{32}')
        try:
            for d in os.listdir(wd):
                if m.match(d):
                    yield d
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise

    def num_jobs(self):
        "Return the number of initialized jobs."
        return len(list(self._job_dirs()))

    __len__ = num_jobs

    def __contains__(self, job):
        """Determine whether job is in the project's data space.

        :param job: The job to test for initialization.
        :type job: :py:class:`~.Job`
        :returns: True when the job is initialized for this project.
        :rtype: bool
        """
        return job.get_id() in self.find_job_ids()

    def build_job_search_index(self, index):
        """Build a job search index.

        :param index: A document index.
        :type index: list
        :returns: A job search index based on the provided index.
        :rtype: :class:`~.JobSearchIndex`
        """
        return JobSearchIndex(index=index)

    def build_job_statepoint_index(self, exclude_const=False, index=None):
        """Build a statepoint index to identify jobs with specific parameters.

        This method generates unordered key-value pairs, with complete
        statepoint paths as keys, encoded in JSON, and a set of job ids
        of all corresponding jobs, e.g.:

        .. code-block:: python

            >>> project.open_job({'a': 0, 'b': {'c': 'const'}}).init()
            >>> project.open_job({'a': 1, 'b': {'c': 'const'}}).init()
            >>> for k, v in project.job_statepoint_index():
            ...     print(k, v)
            ...
            ["a", 1] {'b7568fa73881d27cbf24bf58d226d80e'}
            ["a", 0] {'54b61a7adbe004b30b39aa399d04f483'}
            ["b", "c", "abc"] {'b7568fa73881d27cbf24bf58d226d80e', '54b61a7adbe004b30b...

        :param exclude_const: Exclude entries that are shared by all jobs
            that are part of the index.
        :type exclude_const: bool
        :param index: A document index.
        :yields: Key-value pairs of JSON-encoded statepoint parameters and
            and a set of corresponding job ids.
        """
        from .collection import _traverse_filter
        if index is None:
            index = self.index(include_job_document=False)
        collection = Collection(index)
        for doc in collection.find():
            for key, _ in _traverse_filter(doc):
                if key == '_id' or key.split('.')[0] != 'statepoint':
                    continue
                collection.index(key, build=True)
        tmp = collection._indexes
        for k in sorted(tmp, key=lambda k: len(tmp[k])):
            if exclude_const and len(tmp[k]) == 1 \
                    and len(tmp[k][list(tmp[k].keys())[0]]) == len(collection):
                continue
            yield tuple(k.split('.')[1:]), tmp[k]

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
            index = self.index(include_job_document=doc_filter is not None)
        search_index = self.build_job_search_index(index)
        return search_index.find_job_ids(filter=filter, doc_filter=doc_filter)

    def find_jobs(self, filter=None, doc_filter=None, index=None):
        """Find all jobs in the project's workspace.

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
        :yields: Instances of :class:`~signac.contrib.job.Job`
        :raise TypeError: If the filters are not JSON serializable.
        :raises ValueError: If the filters are invalid.
        :raises RuntimeError: If the filters are not supported
            by the index.
        """
        return _JobsIterator(self, self.find_job_ids(filter, doc_filter, index))

    def __iter__(self):
        return self.find_jobs()

    def find_statepoints(self, filter=None, doc_filter=None, index=None, skip_errors=False):
        """Find all statepoints in the project's workspace.

        :param filter: If not None, only yield statepoints matching the filter.
        :type filter: mapping
        :param skip_errors: Show, but otherwise ignore errors while
            iterating over the workspace. Use this argument to repair
            a corrupted workspace.
        :type skip_errors: bool
        :yields: statepoints as dict"""
        if index is None:
            index = self.index(include_job_document=False)
        if skip_errors:
            index = _skip_errors(index, logger.critical)
        jobs = self.find_jobs(filter, doc_filter, index)
        if skip_errors:
            jobs = _skip_errors(jobs, logger.critical)
        for job in jobs:
            yield job.statepoint()

    def read_statepoints(self, fn=None):
        """Read all statepoints from a file.

        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str

        See also :meth:`dump_statepoints` and :meth:`write_statepoints`.
        """
        if fn is None:
            fn = os.path.join(self.root_directory(), FN_STATEPOINTS)
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
            fn = os.path.join(self.root_directory(), FN_STATEPOINTS)
        try:
            tmp = self.read_statepoints(fn=fn)
        # except FileNotFoundError:
        except IOError as error:
            if not error.errno == errno.ENOENT:
                raise
            tmp = dict()
        if statepoints is None:
            statepoints = self.find_statepoints()
        tmp.update(self.dump_statepoints(statepoints))
        with open(fn, 'w') as file:
            file.write(json.dumps(tmp, indent=indent))

    def _get_statepoint_from_workspace(self, jobid):
        fn_manifest = os.path.join(self.workspace(), jobid, self.Job.FN_MANIFEST)
        try:
            with open(fn_manifest, 'r') as manifest:
                return json.loads(manifest.read())
        except (IOError, ValueError) as error:
            if os.path.isfile(fn_manifest):
                msg = "Error while trying to access manifest file: "\
                      "'{}'. Error: '{}'.".format(fn_manifest, error)
                logger.critical(msg)
            raise KeyError(jobid)

    def get_statepoint(self, jobid, fn=None):
        """Get the statepoint associated with a job id.

        The statepoint is retrieved from the workspace or
        from the statepoints file if the former attempt fails.

        :param jobid: A job id to get the statepoint for.
        :type jobid: str
        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str
        :return: The statepoint.
        :rtype: dict
        :raises KeyError: If the statepoint associated with \
            jobid could not be found.

        See also :meth:`dump_statepoints`.
        """
        try:
            statepoint = self._get_statepoint_from_workspace(jobid)
        except KeyError:
            try:
                statepoint = self.read_statepoints(fn=fn)[jobid]
            except IOError as error:
                if not error.errno == errno.ENOENT:
                    raise
                raise KeyError(jobid)
        assert statepoint is not None
        assert str(self.open_job(statepoint)) == jobid
        return statepoint

    def create_linked_view(self, prefix=None, job_ids=None, index=None):
        """Create or update a persistent linked view of the selected data space.

        This method determines unique paths for each job based on the job's
        statepoint and creates symbolic links to the associated workspace
        directories. This is useful for browsing through the data space in a
        human-readable manner.

        Assuming that the parameter space is

            * a=0, b=0
            * a=1, b=0
            * a=2, b=0
            * ...,

        where *b* does not vary over all statepoints, this method will create
        the following *symbolic links* within the specified view prefix:

        .. code-block:: bash

            view/a/0/job -> /path/to/workspace/7f9fb369851609ce9cb91404549393f3
            view/a/1/job -> /path/to/workspace/017d53deb17a290d8b0d2ae02fa8bd9d
            ...

        .. note::

            To maximize the compactness of each view path, *b* which does not
            vary over the selected data space, is ignored.

        :param prefix:
            The path where the linked view will be created or updated.
        :type prefix:
            str
        :param job_ids:
            If None (the default), create the view for the complete data space,
            otherwise only for the sub space constituted by the provided job ids.
        :param index:
            A document index.
        """
        if prefix is None:
            prefix = 'view'
        if index is None:
            index = self.index(include_job_document=False)
        if job_ids is not None:
            if not isinstance(job_ids, set):
                job_ids = set(job_ids)
            index = [doc for doc in index if doc['_id'] in job_ids]
            if not job_ids.issubset({doc['_id'] for doc in index}):
                raise ValueError("Insufficient index for selected data space.")

        jsi = self.build_job_statepoint_index(exclude_const=True, index=index)
        sp_index = dict(jsi)
        tmp = collections.defaultdict(list)
        for key, values in sp_index.items():
            for value, group in values.items():
                p = '_'.join(str(_) for _ in (key + (value, )))
                for job_id in group:
                    tmp[job_id].append(p)
        links = dict()
        for job_id, p in tmp.items():
            path = os.path.join(* p + ['job'])
            links[path] = self.open_job(id=job_id).workspace()
        if not links:   # data space contains less than two elements
            for job in self.find_jobs():
                links['./job'] = job.workspace()
            assert len(links) < 2
        _update_view(prefix, links)

    def find_job_documents(self, filter=None):
        """Find all job documents in the project's workspace.

        This method iterates through all jobs or all jobs matching
        the filter and yields each job's document as a dict.
        Each dict additionally contains a field 'statepoint',
        with the job's statepoint and a field '_id', which is
        the job's id.

        :param filter: If not None,
            only find job documents matching filter.
        :type filter: mapping
        :yields: Instances of dict.
        :raises KeyError: If the job document already contains the fields
            '_id' or 'statepoint'."""
        for job in self.find_jobs(filter=filter):
            doc = dict(job.document)
            if '_id' in doc:
                raise KeyError(
                    "The job document already contains a field '_id'!")
            if 'statepoint' in doc:
                raise KeyError(
                    "The job document already contains a field 'statepoint'!")
            doc['_id'] = str(job)
            doc['statepoint'] = job.statepoint()
            yield doc

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

    def clone(self, job):
        """Clone job into this project.

        Create an identical copy of job within this project.

        :param job: The job to copy into this project.
        :type job: :py:class:`~.Job`
        :returns: The job instance corresponding to the copied job.
        :rtype: :py:class:`~.Job`
        :raises DestinationExistsError:
            In case that a job with the same id is already
            initialized within this project.
        """
        dst = self.open_job(job.statepoint())
        try:
            shutil.copytree(job.workspace(), dst.workspace())
        except OSError as error:
            if error.errno == errno.EEXIST:
                raise DestinationExistsError(dst)
            else:
                raise
        return dst

    def repair(self):
        "Attempt to repair the workspace after it got corrupted."
        for job_dir in self._job_dirs():
            jobid = os.path.split(job_dir)[-1]
            fn_manifest = os.path.join(job_dir, self.Job.FN_MANIFEST)
            try:
                with open(fn_manifest) as manifest:
                    statepoint = json.loads(manifest.read())
            except Exception as error:
                logger.warning(
                    "Encountered error while reading from '{}'. "
                    "Error: {}".format(fn_manifest, error))
                try:
                    logger.info("Attempt to recover statepoint from file.")
                    statepoint = self.get_statepoint(jobid)
                    self.open_job(statepoint)._create_directory(overwrite=True)
                except KeyError as error:
                    raise RuntimeWarning(
                        "Use write_statepoints() before attempting to repair!")
                except IOError as error:
                    if FN_STATEPOINTS in str(error):
                        raise RuntimeWarning(
                            "Use write_statepoints() before attempting to repair!")
                    raise
                except Exception:
                    logger.error("Attemp to repair job space failed.")
                    raise
                else:
                    logger.info("Successfully recovered state point.")

    def index(self, formats=None, depth=0,
              skip_errors=False, include_job_document=True):
        """Generate an index of the project's workspace.

        This generator function indexes every file in the project's
        workspace until the specified `depth`.
        The job document if it exists, is always indexed, other
        files need to be specified with the formats argument.

        .. code-block:: python

            for doc in project.index({'.*\.txt', 'TextFile'}):
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
            docs = _index_signac_project_workspace(
                root=self.workspace(),
                include_job_document=include_job_document,
                fn_statepoint=self.Job.FN_MANIFEST)
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
            Defaults to the standard name and should ususally
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

    @classmethod
    def init_project(cls, name, root=None, workspace=None, make_dir=True):
        """Initialize a project with the given name.

        It is safe to call this function multiple times with
        the same arguments.
        However, a RuntimeError is raised in case where an
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
            project = cls.get_project(root=root)
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
    def get_project(cls, root=None):
        """Find a project configuration and return the associated project.

        :param root: The project root directory.
            If no root directory is given, the next project found
            within or above the current working directory is returned.
        :type root: str
        :returns: The project handle.
        :raises LookupError: If no project configuration can be found."""
        config = load_config(root=root, local=root is not None)
        if 'project' not in config:
            raise LookupError(
                "Unable to determine project id for path '{}'.".format(
                    os.getcwd() if root is None else os.path.abspath(root)))
        return cls(config=config)


def _find_all_links(root, leaf='job'):
    for dirpath, dirnames, filenames in os.walk(root):
        for dirname in dirnames:
            if dirname == leaf:
                yield os.path.relpath(dirpath, root)
                break
        for filename in filenames:
            if filename == leaf:
                yield os.path.relpath(dirpath, root)
                break


class _Node(object):

    def __init__(self, name=None, value=None):
        self.name = name
        self.value = value
        self.children = dict()

    def get_child(self, name):
        return self.children.setdefault(name, type(self)(name))

    def __str__(self):
        return "_Node({}, {})".format(self.name, self.value)

    __repr__ = __str__


def _build_tree(paths):
    root = _Node()
    for path in paths:
        node = root
        for p in path.split(os.sep):
            node = node.get_child(p)
    return root


def _get_branches(root, branch=None):
    if branch is None:
        branch = list()
    else:
        branch = list(branch) + [root]
    if root.children:
        for child in root.children.values():
            for b in _get_branches(child, branch):
                yield b
    else:
        yield branch


def _color_path(root, path):
    root.value = True
    for name in path:
        root = root.get_child(name)
        root.value = True


def _find_dead_branches(root, branch=None):
    if branch is None:
        branch = list()
    else:
        branch = list(branch) + [root]
    if root.children:
        for child in root.children.values():
            for b in _find_dead_branches(child, branch):
                yield b
    if not root.value:
        yield branch


def _analyze_view(prefix, links, leaf='job'):
    logger.info("Analyzing view prefix '{}'...".format(prefix))
    existing_paths = {os.path.join(p, leaf) for p in _find_all_links(prefix, leaf)}
    existing_tree = _build_tree(existing_paths)
    for path in links:
        _color_path(existing_tree, path.split(os.sep))
    obsolete = []
    dead_branches = _find_dead_branches(existing_tree)
    for branch in reversed(sorted(dead_branches, key=lambda b: len(b))):
        if branch:
            obsolete.append(os.path.join(* (n.name for n in branch)))
    if '.' in obsolete:
        obsolete.remove('.')
    keep_or_update = existing_paths.intersection(links.keys())
    new = set(links.keys()).difference(keep_or_update)
    to_update = [p for p in keep_or_update if
                 os.path.realpath(os.path.join(prefix, p)) != links[p]]
    return obsolete, to_update, new


def _update_view(prefix, links, leaf='job'):
    obsolete, to_update, new = _analyze_view(prefix, links)
    num_ops = len(obsolete) + 2 * len(to_update) + len(new)
    if num_ops:
        logger.info("Generating current view in '{}' ({} operations)...".format(
            prefix, num_ops))
    else:
        logger.info("View in '{}' is up to date.".format(prefix))
        return
    logger.debug("Removing {} obsolete links.".format(len(obsolete)))
    for path in obsolete:
        p = os.path.join(prefix, path)
        try:
            os.unlink(p)
        except OSError:
            os.rmdir(p)
    logger.debug("Creating {} new and updating {} existing links.".format(
        len(new), len(to_update)))
    for path in to_update:
        os.unlink(os.path.join(prefix, path))
    for path in chain(new, to_update):
        dst = os.path.join(prefix, path)
        src = os.path.relpath(links[path], os.path.split(dst)[0])
        _make_link(src, dst)


def _make_link(src, dst):
    try:
        os.makedirs(os.path.dirname(dst))
    # except FileExistsError:
    except OSError as error:
        if error.errno != errno.EEXIST:
            raise
    try:
        if six.PY2:
            os.symlink(src, dst)
        else:
            os.symlink(src, dst, target_is_directory=True)
    except OSError as error:
        if error.errno == errno.EEXIST:
            if os.path.realpath(src) == os.path.realpath(dst):
                return
        raise


def _make_urls(statepoints, key_set):
    "Create unique URLs for all jobs matching filter."
    for statepoint in statepoints:
        url = []
        for keys in key_set:
            url.append('.'.join(keys))
            v = statepoint
            for key in keys:
                v = v.get(key)
                if v is None:
                    break
            url.append(str(v))
        if len(url):
            yield statepoint, os.path.join(*url)


def _skip_errors(iterable, log=print):
    while True:
        try:
            yield next(iterable)
        except StopIteration:
            return
        except Exception as error:
            log(error)


class _JobsIterator(object):

    def __init__(self, project, ids):
        self._project = project
        self._ids = ids
        self._ids_iterator = iter(ids)

    def __len__(self):
        return len(self._ids)

    def __iter__(self):
        return self

    def __next__(self):
        return self._project.open_job(id=next(self._ids_iterator))

    next = __next__  # python 2.7 compatibility


def init_project(name, root=None, workspace=None, make_dir=True):
    """Initialize a project with the given name.

    It is safe to call this function multiple times with
    the same arguments.
    However, a RuntimeError is raised in case where an
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


def get_project(root=None):
    """Find a project configuration and return the associated project.

    :param root: The project root directory.
        If no root directory is given, the next project found
        within or above the current working directory is returned.
    :type root: str
    :returns: The project handle.
    :rtype: :py:class:`~.Project`
    :raises LookupError: If no project configuration can be found."""
    return Project.get_project(root=root)
