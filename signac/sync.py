# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
import shutil
import filecmp
import logging
from copy import deepcopy
from contextlib import contextmanager

from .errors import DestinationExistsError
from .errors import FileMergeConflict
from .errors import DocumentMergeConflict
from .errors import SchemaMergeConflict
from .contrib.utility import query_yes_no
from .common import six
from filecmp import dircmp
if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping


LEVEL_MORE = logging.INFO - 5

logger = logging.getLogger(__name__)
logging.addLevelName(LEVEL_MORE, 'MORE')
logging.MORE = LEVEL_MORE


def log_more(msg, *args, **kwargs):
    logger.log(LEVEL_MORE, msg, *args, **kwargs)


logger.more = log_more


__all__ = [
    'merge_jobs',
    'merge_projects',
    'FileMerge',
    'DocMerge',
]


class dircmp_deep(dircmp):

    def phase3(self):  # Find out differences between common files
        xx = filecmp.cmpfiles(self.left, self.right, self.common_files, shallow=False)
        self.same_files, self.diff_files, self.funny_files = xx

    methodmap = dict(dircmp.methodmap)
    methodmap['samefiles'] = methodmap['diff_files'] = phase3


class _DocProxy(object):
    """Proxy object for document (mapping) modifications.

    This proxy is used to keep track of changes and ensure that
    dry runs do not actually modify any data.

    :param dry_run:
        Do not actually perform any data modification operation, but
        still log the action.
    :type dry_run:
        bool
    """

    def __init__(self, doc, dry_run=False):
        self.doc = doc
        self.dry_run = dry_run

    def __str__(self):
        return "_DocProxy({})".format(str(self.doc))

    def __repr__(self):
        return "_DocProxy({})".format(repr(self.doc))

    def __getitem__(self, key):
        return self.doc[key]

    def __setitem__(self, key, value):
        logger.more("Set '{}'='{}'.".format(key, value))
        if not self.dry_run:
            self.doc[key] = value

    def keys(self):
        return self.doc.keys()

    def clear(self):
        self.doc.clear()

    def update(self, other):
        for key in other.keys():
            self[key] = other[key]

    def __iter__(self):
        return iter(self.doc)

    def __contains__(self, key):
        return key in self.doc

    def __eq__(self, other):
        return self.doc.__eq__(other)

    def __len__(self):
        return len(self.doc)


class _FileModifyProxy(object):
    """This proxy used for data modification.

    This proxy is used for all file data modification to keep
    track of changes and to ensure that dry runs do not actually
    modify any data.

    :param dry_run:
        Do not actually perform any data modification operation, but
        still log the action.
    :type dry_run:
        bool
    """

    def __init__(self, dry_run=False):
        self.dry_run = dry_run

    def copy(self, src, dst):
        logger.more("Copy file '{}' -> '{}'.".format(os.path.relpath(src), os.path.relpath(dst)))
        if not self.dry_run:
            shutil.copy(src, dst)

    def copytree(self, src, dst):
        logger.more("Copy tree '{}' -> '{}'.".format(os.path.relpath(src), os.path.relpath(dst)))
        if not self.dry_run:
            shutil.copytree(src, dst)

    def remove(self, path):
        logger.more("Remove path '{}'.".format(os.path.relpath(path)))
        if not self.dry_run:
            os.remove(path)

    @contextmanager
    def create_backup(self, path):
        logger.debug("Create backup of '{}'.".format(os.path.relpath(path)))
        path_backup = path + '~'
        if os.path.isfile(path_backup):
            raise RuntimeError(
                "Failed to create backup, file already exists: '{}'.".format(
                    os.path.relpath(path_backup)))
        try:
            self.copy(path, path_backup)
            yield path_backup
        except:
            logger.more("Error occured, restoring backup...")
            self.copy(path_backup, path)
            raise
        finally:
            logger.debug("Remove backup of '{}'.".format(os.path.relpath(path)))
            self.remove(path_backup)

    @contextmanager
    def create_doc_backup(self, doc):
        proxy = _DocProxy(doc, dry_run=self.dry_run)
        fn = getattr(doc, 'filename', getattr(doc, '_filename', None))
        if not len(proxy) or fn is None or not os.path.isfile(fn):
            backup = deepcopy(doc)  # use in-memory backup
            try:
                yield proxy
            except:     # roll-back
                proxy.clear()
                proxy.update(backup)
                raise
        else:
            with self.create_backup(fn):
                yield proxy


# Definition of default merge strategies

class FileMerge(object):
    "Collection of file merge strategies."

    @classmethod
    def keys(cls):
        return ('always', 'never', 'time', 'Ask')

    @staticmethod
    def always(src, dst, fn):
        "Always merge files on conflict."
        return True

    @staticmethod
    def never(src, dst, fn):
        "Never merge files on conflict."
        return False

    @staticmethod
    def time(src, dst, fn):
        "Merge a file based on its modification time stamp."
        return os.path.getmtime(src.fn(fn)) > os.path.getmtime(dst.fn(fn))

    class Ask(object):
        "Ask whether a file should be merged interactively."

        def __init__(self):
            self.yes = set()
            self.no = set()

        def __call__(self, src, dst, fn):
            if fn in self.yes:
                return True
            elif fn in self.no:
                return False
            else:
                overwrite = query_yes_no("Overwrite files named '{}'?".format(fn), 'no')
                if overwrite:
                    self.yes.add(fn)
                    return True
                else:
                    self.no.add(fn)
                    return False


class DocMerge(object):
    "Collection of document merge functions."

    NO_MERGE = False
    "Do not merge documents."

    COPY = 'copy'
    "Copy documents like all other files."

    @staticmethod
    def update(src, dst):
        "Perform simple update."
        for key in src.keys():
            dst[key] = src[key]

    class ByKey(object):
        "Merge documents key by key."

        def __init__(self, key_strategy=None):
            if isinstance(key_strategy, str):

                def regex_key_strategy(key):
                    return re.match(key_strategy, key)

                self.key_strategy = regex_key_strategy
            else:
                self.key_strategy = key_strategy
            self.skipped_keys = set()

        def __str__(self):
            return "{}({})".format(type(self).__name__, self.key_strategy)

        def __call__(self, src, dst, root=''):
            if src == dst:
                return
            for key, value in src.items():
                if key in dst:
                    if dst[key] == value:
                        continue
                    elif isinstance(value, Mapping):
                        self(src[key], dst[key], key + '.')
                        continue
                    elif self.key_strategy is None or not self.key_strategy(root + key):
                        self.skipped_keys.add(root + key)
                        continue
                dst[key] = value

            # Check for skipped keys and raise an exception in case that no strategy
            # was provided, otherwise just log them.
            if self.skipped_keys and not root:
                if self.key_strategy is None:
                    raise DocumentMergeConflict(self.skipped_keys)
                else:
                    logger.more("Skipped keys: {}".format(', '.join(self.skipped_keys)))


def _merge_job_workspaces(src, dst, strategy, exclude, proxy, subdir='', deep=False):
    "Merge two job workspaces file by file, following the provided strategy."
    if deep:
        diff = dircmp_deep(src.fn(subdir), dst.fn(subdir))
    else:
        diff = dircmp(src.fn(subdir), dst.fn(subdir))
    for fn in diff.left_only:
        if exclude and any([re.match(p, fn) for p in exclude]):
            logger.debug("File named '{}' is skipped (excluded).".format(fn))
            continue
        fn_src = os.path.join(src.workspace(), subdir, fn)
        fn_dst = os.path.join(dst.workspace(), subdir, fn)
        if os.path.isfile(fn_src):
            proxy.copy(fn_src, fn_dst)
        else:
            proxy.copytree(fn_src, fn_dst)
    for fn in diff.diff_files:
        if exclude and any([re.match(p, fn) for p in exclude]):
            logger.debug("File named '{}' is skipped (excluded).".format(fn))
            continue
        if strategy is None:
            raise FileMergeConflict(fn)
        else:
            fn_src = os.path.join(src.workspace(), subdir, fn)
            fn_dst = os.path.join(dst.workspace(), subdir, fn)
            if strategy(src, dst, os.path.join(subdir, fn)):
                proxy.copy(fn_src, fn_dst)
            else:
                logger.debug("Skip file '{}'.".format(fn))
    for _subdir in diff.subdirs:
        _merge_job_workspaces(
            src, dst, strategy, exclude, proxy, os.path.join(subdir, _subdir), deep=deep)


def merge_jobs(src, dst, strategy=None, exclude=None, doc_merge=None, dry_run=False, deep=False):
    """Merge the data of the src job into the dst job.

        By default, this method will merge all files and document data from the src job
        to the dst job until a merge conflict occurs. There are two different kinds of
        merge conflicts:

            1. The two jobs have files with the same, but different content.
            2. The two jobs have documents that share keys, but those keys are
               associated with different values.

        A file conflict can be resolved by providing a 'FileMerge' *strategy* or by
        *excluding* files from the merge. An unresolvable conflict is indicated with
        the raise of a :py:class:`~.errors.FileMergeConflict` exception.

        A document merge conflict can be resolved by providing a doc_merge function
        that takes the source and the destination document as first and second argument.

        :param src:
            The src job, data will be copied from this job's workspace.
        :type src:
            `~.Job`
        :param dst:
            The dst job, data will be merged with this job's data.
        :type dst:
            `~.Job`
        :param strategy:
            A merge strategy for file conflicts. If no strategy is provided, a
            MergeConflict exception will be raised upon conflict.
        :param exclude:
            An filename exclude pattern. All files matching this pattern will be
            excluded from merging.
        :type exclude:
            str
        :param doc_merge:
            A merge strategy for document keys. If this argument is None, by default
            no keys will be merged upon conflict.
        :param dry_run:
            If True, do not actually perform any merge actions.
    """
    # check src and dst compatiblity
    assert src.FN_MANIFEST == dst.FN_MANIFEST
    assert src.FN_DOCUMENT == dst.FN_DOCUMENT

    # Nothing to be done if the src is not initialized.
    if src not in src._project:
        return

    # The doc_merge functions defaults to a safe "by_key" strategy.
    if doc_merge is None:
        doc_merge = DocMerge.ByKey()

    # the exclude argument must be a list
    if exclude is None:
        exclude = []
    elif not isinstance(exclude, list):
        exclude = [exclude]
    exclude.append(src.FN_MANIFEST)
    if doc_merge != DocMerge.COPY:
        exclude.append(src.FN_DOCUMENT)

    if type(dry_run) == _FileModifyProxy:
        proxy = dry_run
    else:
        proxy = _FileModifyProxy(dry_run=bool(dry_run))
    if proxy.dry_run:
        logger.debug("Merging job '{}' (dry run)...".format(src))
    else:
        logger.debug("Merging job '{}'...".format(src))

    if os.path.isdir(src.workspace()):
        dst.init()
        _merge_job_workspaces(src, dst, strategy, exclude, proxy, deep=deep)

    if not (doc_merge is DocMerge.NO_MERGE or doc_merge == DocMerge.COPY):
        with proxy.create_doc_backup(dst.document) as dst_proxy:
            doc_merge(src.document, dst_proxy)


def merge_projects(source, destination, strategy=None, exclude=None, doc_merge=None,
                   selection=None, check_schema=True, dry_run=False):
    """Merge the source project into the destination project.

    Try to clone all jobs from the source to the destination.
    If the destination job already exist, try to merge the job using the
    optionally specified strategy.
    """
    if source == destination:
        raise ValueError("Source and destination can't be the same!")

    # Setup data modification proxy
    proxy = _FileModifyProxy(dry_run=dry_run)

    # Perform a schema check in an attempt to avoid bad merge operations.
    if check_schema:
        schema_src = source.detect_schema()
        schema_dst = destination.detect_schema()
        if schema_dst and schema_src and schema_src != schema_dst:
            if schema_src.difference(schema_dst) or schema_dst.difference(schema_src):
                raise SchemaMergeConflict(schema_src, schema_dst)

    if doc_merge is None:
        doc_merge = DocMerge.ByKey()

    if selection is not None:  # The selection argument may be a jobs or job ids sequence.
        selection = {str(j) for j in selection}

    # Provide some information about this merge process.
    if selection:
        logger.info("Merging selection ({}) of project '{}' into '{}'.".format(
            len(selection), source, destination))
    else:
        logger.info("Merging project '{}' into '{}'.".format(source, destination))
    logger.more("'{}' -> '{}'".format(source.root_directory(), destination.root_directory()))
    if dry_run:
        logger.info("Performing dry run!")
    if exclude is not None:
        logger.more("File name exclude pattern: '{}'".format(exclude))
    logger.more("Merge strategy: '{}'".format(strategy))
    logger.more("Doc merge strategy: '{}'".format(doc_merge))

    # Merge the Project document.
    if not (doc_merge is DocMerge.NO_MERGE or doc_merge == DocMerge.COPY):
        with proxy.create_doc_backup(destination.document) as dst_proxy:
            doc_merge(source.document, dst_proxy)

    # Merge jobs from source to destination.
    num_cloned, num_merged = 0, 0
    for src_job in source:
        if selection is not None and src_job.get_id() not in selection:
            logger.more("{} not in selection.".format(src_job))
            continue
        try:
            destination.clone(src_job)
            num_cloned += 1
            logger.more("Cloned job '{}'.".format(src_job))
        except DestinationExistsError as e:
            dst_job = destination.open_job(id=src_job.get_id())
            merge_jobs(src_job, dst_job, strategy, exclude, doc_merge, proxy)
            num_merged += 1
            logger.more("Merged job '{}'.".format(src_job))
    logger.info("Cloned {} and merged {} job(s).".format(num_cloned, num_merged))
