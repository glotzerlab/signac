# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import os
import re
import logging
import json
import errno
import warnings
import collections

from ..core.search_engine import DocumentSearchEngine
from ..common import six
from ..common.config import load_config
from .job import Job
from .hashing import calc_id
from .indexing import _index_signac_project_workspace
from .indexing import SignacProjectCrawler
from .indexing import MasterCrawler
from .utility import _mkdir_p, is_string

if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

#: The default filename to read from and write statepoints to.
FN_STATEPOINTS = 'signac_statepoints.json'

ACCESS_MODULE_TEMPLATE = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from signac.contrib import SignacProjectCrawler
{imports}


class {crawlername}(SignacProjectCrawler):
    pass
{definitions}


def get_crawlers(root):
    return {{'main': {crawlername}(os.path.join(root, '{wd}'))}}
"""

ACCESS_MODULE_MC_TEMPLATE = """if __name__ == '__main__':
    master_crawler = MasterCrawler('.')
    for doc in master_crawler.crawl(depth={depth}):
        print(doc)
"""


class JobSearchIndex(object):
    """Search for sepcific jobs with filters.

    The JobSearchIndex allows to search for job_ids,
    that are part of an index, which match specific
    statepoint filters or job document filters.

    :param project: The project the jobs are associated with.
    :type project: :class:`~.Project`
    :param index: A document index.
    :param include: A mapping of keys that shall be
        included (True) or excluded (False).
    :type include: Mapping
    """

    def __init__(self, index, include=None, hash_=None):
        self._engine = DocumentSearchEngine(
            index, include=include, hash_=hash_)

    def __len__(self):
        return len(self._engine)

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
        f = dict()
        if filter is not None:
            f['statepoint'] = filter
        if doc_filter is not None:
            f.update(doc_filter)
        f = json.loads(json.dumps(f))  # Normalize
        for job_id in self._engine.find(filter=f):
            yield job_id


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
        return "{type}({{'project': '{id}', 'project_dir': '{rd}', 'workspace_dir': '{wd}'}})".format(
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

    def open_job(self, statepoint=None, id=None):
        """Get a job handle associated with a statepoint.

        This method returns the job instance associated with
        the given statepoint or job id.
        Opening a job by statepoint never fails.
        Opening a job by id, requires a lookup of the statepoint
        from the job id, which may fail if the job was not
        previously initialized.

        :param statepoint: The job's unique set of parameters.
        :type statepoint: mapping
        :param id: The job id.
        :type id: str
        :return: The job instance.
        :rtype: :class:`signac.contrib.job.Job`
        :raises KeyError: If the attempt to open the job by id fails.
        """
        if (id is None) == (statepoint is None):
            raise ValueError(
                "You need to either provide the statepoint or the id.")
        if id is None:
            return self.Job(project=self, statepoint=statepoint)
        else:
            return self.Job(project=self, statepoint=self.get_statepoint(id))

    def _job_dirs(self):
        wd = self.workspace()
        m = re.compile('[a-z0-9]{32}')
        try:
            for d in os.listdir(wd):
                if m.match(d):
                    yield d
        except IOError as error:
            if error.errno != errno.ENOENT:
                raise

    def num_jobs(self):
        "Return the number of initialized jobs."
        return len(list(self._job_dirs()))

    def build_job_search_index(self, index, include=None, hash_=None):
        """Build a job search index.

        :param index: A document index.
        :type index: list
        :param include: A mapping of keys that shall be
            included (True) or excluded (False).
        :type include: Mapping
        :returns: A job search index based on the provided index.
        :rtype: :class:`~.JobSearchIndex`
        """
        return JobSearchIndex(index=index, include=include, hash_=hash_)

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
            ["b", "c", "abc"] {'b7568fa73881d27cbf24bf58d226d80e', '54b61a7adbe004b30b39aa399d04f483'}

        :param exclude_const: Exclude entries that are shared by all jobs
            that are part of the index.
        :type exclude_const: bool
        :param index: A document index.
        :yields: Key-value pairs of JSON-encoded statepoint parameters and
            and a set of corresponding job ids.
        """
        if index is None:
            index = self.index(include_job_document=False)
        include = {'statepoint': True}
        search_index = self.build_job_search_index(
            index, include, hash_=json.dumps)
        tmp = search_index._engine.index
        N = len(search_index)
        for k in sorted(tmp, key=lambda k: len(tmp[k])):
            if exclude_const and len(tmp[k]) == N:
                continue
            yield json.dumps(json.loads(k)[1:]), tmp[k]

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
            for job_id in self._job_dirs():
                yield job_id
            return
        if index is None:
            index = self.index(include_job_document=doc_filter is not None)
        if doc_filter is None:
            include = {'statepoint': True}
        else:
            include = None
        search_index = self.build_job_search_index(index, include)
        for job_id in search_index.find_job_ids(filter=filter, doc_filter=doc_filter):
            yield job_id

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
        for job_id in self.find_job_ids(filter, doc_filter, index):
            yield self.open_job(id=job_id)

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

    def find_variable_parameters(self, statepoints=None):
        """Find all parameters which vary over the data space.

        .. warning::

            This method is deprecated.
            Please see :meth:`~.build_job_statepoint_index` for an
            alternative method.

        This method attempts to detect all parameters, which vary
        over the parameter space.
        The parameter sets are ordered decreasingly
        by data sub space size.

        .. warning::

            This method does not detect linear dependencies
            within the state points. Linear dependencies should
            generally be avoided.

        :param statepoints: The statepoints to consider.
            Defaults to all state points within the data space.
        :type statepoints: Iterable of parameter mappings.
        :return: A hierarchical list of variable parameters.
        :rtype: list"""
        warnings.warn(
            "The find_variable_parameters() method is deprecated, please use "
            "build_job_statepoint_index() instead.", DeprecationWarning)
        if statepoints is None:
            statepoints = self.find_statepoints()
        return list(_find_unique_keys(statepoints))

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
                return json.load(manifest)
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

    def create_linked_view(self, job_ids=None, prefix=None,
                           force=False, index=None):
        """Create a persistent linked view of the selected data space..

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
        """
        if prefix is None:
            prefix = 'view'
        if index is None:
            index = self.index(include_job_document=False)
        if not force and os.listdir(prefix):
            raise RuntimeError(
                "Failed to create persistent view in '{}', the directory "
                "is not empty! Use `force=True` to ignore this and create "
                "the view anyways.".format(prefix))

        if job_ids is not None:
            if not isinstance(job_ids, set):
                job_ids = set(job_ids)
            index = (doc for doc in index if doc['signac_id'] in job_ids)

        jsi = self.build_job_statepoint_index(exclude_const=True, index=index)
        no_link = True
        for path, job_id in _make_paths(jsi):
            if job_ids is not None and job_id not in job_ids:
                continue
            src = os.path.join(self.open_job(id=job_id).workspace())
            dst = os.path.join(prefix, path)
            logger.info(
                "Creating link {src} -> {dst}".format(src=src, dst=dst))
            _make_link(src, dst)
            no_link = False
        if no_link:
            raise RuntimeError(
                "The # of jobs selected for the creation of views must "
                "be greater or equal than 2!")

    def create_view(self, filter=None, prefix='view'):
        """Create a view of the workspace.

        .. warning::

            This method is deprecated.
            Please use :meth:`~.create_linked_view` instead.

        This method gathers all varying statepoint parameters
        and creates symbolic links to the workspace directories.
        This is useful for browsing through the workspace in a
        human-readable manner.

        Let's assume the parameter space is

            * a=0, b=0
            * a=1, b=0
            * a=2, b=0
            * ...,

        where *b* does not vary over all statepoints.

        Calling this method will generate the following *symbolic links* within
        the speciefied  view directory:

        .. code-block:: bash

            view/a/0 -> /path/to/workspace/7f9fb369851609ce9cb91404549393f3
            view/a/1 -> /path/to/workspace/017d53deb17a290d8b0d2ae02fa8bd9d
            ...

        .. note::

            As *b* does not vary over the whole parameter space it is not part
            of the view url.
            This maximizes the compactness of each view url.

        :param filter:  If not None,
            create view only for jobs matching filter.
        :type filter: mapping
        :param prefix: Specifies where to create the links."""
        warnings.warn(
            "The create_view() method is deprecated, please use "
            "create_linked_view() instead.", DeprecationWarning)
        statepoints = list(self.find_statepoints(filter=filter))
        if not len(statepoints):
            if filter is None:
                logger.warning("No state points found!")
            else:
                logger.warning("No state points matched the filter.")
        key_set = list(_find_unique_keys(statepoints))
        if filter is not None:
            key_set[:0] = [[key] for key in filter.keys()]
        for statepoint, url in _make_urls(statepoints, key_set):
            src = self.open_job(statepoint).workspace()
            dst = os.path.join(prefix, url)
            logger.info(
                "Creating link {src} -> {dst}".format(src=src, dst=dst))
            _make_link(src, dst)

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
        """Reset the statepoint of job.

        .. danger::

            Use this function with caution! Resetting a job's statepoint,
            may sometimes be necessary, but can possibly lead to incoherent
            data spaces.
            If you only want to *extend* your statepoint, consider to
            use :meth:`~.update_statepoint` instead.

        :param job: The job, that should be reset to a new state point.
        :type job: :class:`~.contrib.job.Job`
        :param new_statepoint: The job's new unique set of parameters.
        :type new_statepoint: mapping
        :raises RuntimeError: If a job associated with the new unique set
            of parameters already exists in the workspace."""
        dst = self.open_job(new_statepoint)
        _move_job(job, dst)
        logger.info(
            "Reset statepoint of job {}, moved to {}.".format(job, dst))

    def update_statepoint(self, job, update, overwrite=False):
        """Update the statepoint of job.

        .. warning::

            While appending to a job's statepoint is generally safe,
            modifying existing parameters may lead to data
            inconsistency. Use the overwrite argument with caution!

        :param job: The job, whose statepoint shall be updated.
        :type job: :class:`~.contrib.job.Job`
        :param update: A mapping used for the statepoint update.
        :type update: mapping
        :param overwrite: Set to true, to ignore whether this
            update overwrites parameters, which are currently
            part of the job's statepoint. Use with caution!
        :raises KeyError: If the update contains keys, which are
            already part of the job's statepoint and overwrite is False.
        :raises RuntimeError: If a job associated with the new unique set
            of parameters already exists in the workspace."""
        statepoint = dict(job.statepoint())
        if not overwrite:
            for key in update:
                if key in statepoint:
                    raise KeyError(key)
        statepoint.update(update)
        _move_job(job, self.open_job(statepoint))

    def repair(self):
        "Attempt to repair the workspace after it got corrupted."
        for job_dir in self._job_dirs():
            jobid = os.path.split(job_dir)[-1]
            fn_manifest = os.path.join(job_dir, self.Job.FN_MANIFEST)
            try:
                with open(fn_manifest) as manifest:
                    statepoint = json.load(manifest)
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

            for doc in project.index('.*\.txt', TextFile):
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
            class Crawler(SignacProjectCrawler):
                pass
            for pattern, fmt in formats.items():
                Crawler.define(pattern, fmt)
            crawler = Crawler(self.workspace())
            docs = crawler.crawl(depth=depth)
        if skip_errors:
            docs = _skip_errors(docs, logger.critical)
        for doc in docs:
            yield doc

    def create_access_module(self, formats=None, crawlername=None,
                             filename=None, master=True, depth=1):
        """Create the access module for indexing

        This method generates the acess module containing indexing
        directives for master crawlers.

        :param formats: The format definitions as mapping.
        :type formats: dict
        :param crawlername: Specify a name for the crawler class.
            Defaults to a name based on the project's name.
        :type crawlername: str
        :param filename: The name of the access module file.
            Defaults to the standard name and should ususally
            not be changed.
        :type filename: str
        :param master: If True, will add master crawler execution
            commands to the bottom of the file.
        :type master: bool
        :param depth: Specifies the depth of the master crawler
            definitions (if `master` is True). Defaults to 1 to
            reduce the crawling depth of the master crawler.
            A value of 0 means no limit.
        :type depth: int"""
        if crawlername is None:
            crawlername = str(self) + 'Crawler'
        if filename is None:
            filename = os.path.join(
                self.root_directory(),
                MasterCrawler.FN_ACCESS_MODULE)
        workspace = os.path.relpath(self.workspace(), self.root_directory())

        imports = set()
        if formats is None:
            definitions = ''
        else:
            dl = "{}.define('{}', {})"
            defs = list()
            for expr, fmt in formats.items():
                if is_string(fmt):
                    defs.append(dl.format(crawlername, expr, "'{}'".format(fmt)))
                else:
                    defs.append(dl.format(crawlername, expr, fmt.__name__))
                    imports.add(
                        'from {} import {}'.format(fmt.__module__, fmt.__name__))
            definitions = '\n'.join(defs)
        if master:
            imports.add('from signac.contrib import MasterCrawler')
        imports = '\n'.join(imports)

        module = ACCESS_MODULE_TEMPLATE.format(
            crawlername=crawlername,
            imports=imports,
            definitions=definitions,
            wd=workspace)
        if master:
            module += '\n\n' + ACCESS_MODULE_MC_TEMPLATE.format(
                depth=depth)

        with open(filename, 'wx' if six.PY2 else 'x') as file:
            file.write(module)
        logger.info("Created access module file '{}'.".format(filename))

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


def _move_job(src, dst):
    logger.debug("Attempting to move job {} to {}".format(src, dst))
    fn_src_manifest = os.path.join(src.workspace(), src.FN_MANIFEST)
    fn_src_manifest_backup = fn_src_manifest + '~'
    os.rename(fn_src_manifest, fn_src_manifest_backup)
    try:
        os.rename(src.workspace(), dst.workspace())
    except OSError:  # rollback
        os.rename(fn_src_manifest_backup, fn_src_manifest)
        raise RuntimeError(
            "Failed to move {} to {}, destination already exists.".format(
                src, dst))
    else:
        dst.init()
        logger.info("Moved job {} to {}.".format(src, dst))


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


def _make_paths(sp_index):
    tmp = collections.defaultdict(list)
    for key, jids in sp_index:
        for jid in jids:
            tmp[jid].append(key)
    for jid, sps in tmp.items():
        p = ('_'.join(str(x) for x in json.loads(sp)) for sp in sorted(sps))
        path = os.path.join(* list(p) + ['job'])
        yield path, jid


def _find_unique_keys(statepoints):
    key_set = _aggregate_statepoints(statepoints)
    if six.PY2:
        def flatten(l):
            for el in l:
                if isinstance(el, collections.Iterable) and not \
                        (isinstance(el, str) or isinstance(el, unicode)):  # noqa
                    for sub in flatten(el):
                        yield sub
                else:
                    yield el
    else:
        def flatten(l):
            for el in l:
                if isinstance(el, collections.Iterable) and \
                        not (isinstance(el, str)):
                    for sub in flatten(el):
                        yield sub
                else:
                    yield el
    key_set = (list(flatten(k)) for k in key_set)
    for key in sorted(key_set, key=len):
        yield key


def _aggregate_statepoints(statepoints, prefix=None):
    result = list()
    statepoint_set = collections.defaultdict(set)
    # Gather all keys.
    ignore = set()
    for statepoint in statepoints:
        for key, value in statepoint.items():
            if key in ignore:
                continue
            try:
                statepoint_set[key].add(value)
            except TypeError:
                if isinstance(value, Mapping):
                    result.extend(_aggregate_statepoints(
                        [sp[key] for sp in statepoints if key in sp],
                        prefix=(key) if prefix is None else (prefix, key)))
                    ignore.add(key)
                else:
                    statepoint_set[key].add(calc_id(value))
    # Heal heterogenous parameter space.
    for statepoint in statepoints:
        for key in statepoint_set.keys():
            if key not in statepoint:
                statepoint_set[key].add(None)
    unique_keys = list(k for k, v in sorted(
        statepoint_set.items(), key=lambda i: len(i[1])) if len(v) > 1)
    result.extend((k,) if prefix is None else (prefix, k) for k in unique_keys)
    return result


def _skip_errors(iterable, log=print):
    while True:
        try:
            yield next(iterable)
        except StopIteration:
            return
        except Exception as error:
            log(error)


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
