# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
import shutil
import filecmp
import logging
from collections.abc import Mapping
from contextlib import contextmanager

from .errors import DestinationExistsError
from .errors import MergeConflict
from .errors import MergeSchemaConflict
from .contrib.utility import query_yes_no


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


def _merge_dirs(src, dst, exclude, strategy, proxy):
    "Merge two directories file by file, following the provided strategy."
    diff = filecmp.dircmp(src, dst)
    for fn in diff.left_only:
        if exclude and any([re.match(p, fn) for p in exclude]):
            logger.debug("File '{}' is skipped (excluded).".format(fn))
            continue
        fn_src = os.path.join(src, fn)
        fn_dst = os.path.join(dst, fn)
        if os.path.isfile(fn_src):
            proxy.copy(fn_src, fn_dst)
        else:
            proxy.copytree(os.path.join(src, fn), os.path.join(dst, fn))
    for fn in diff.diff_files:
        if exclude and any([re.match(p, fn) for p in exclude]):
            logger.debug("File '{}' is skipped (excluded).".format(fn))
            continue
        if strategy is None:
            raise MergeConflict(fn)
        else:
            fn_src = os.path.join(src, fn)
            fn_dst = os.path.join(dst, fn)
            if strategy(fn_src, fn_dst):
                proxy.copy(fn_src, fn_dst)
            else:
                logger.debug("Skip file '{}'.".format(fn))
    for subdir in diff.subdirs:
        _merge_dirs(os.path.join(src, subdir), os.path.join(dst, subdir), exclude, strategy, proxy)


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
        return self.docs.keys()

    def __iter__(self):
        return iter(self.doc)

    def __contains__(self, key):
        return key in self.doc


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

    def set_value(self, doc, key, value):
        logger.more("Set '{}'='{}'.".format(key, value))
        if not self.dry_run:
            doc[key] = value

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
            if not self.dry_run:
                shutil.copy(path, path_backup)
            yield path_backup
        finally:
            logger.debug("Remove backup of '{}'.".format(os.path.relpath(path)))
            if not self.dry_run:
                try:
                    os.remove(path_backup)
                except IOError as error:
                    logger.error(error)

    @contextmanager
    def create_doc_backup(self, doc):
        fn = getattr(doc, 'filename', getattr(doc, '_filename', None))
        if fn is None or not os.path.isfile(fn):
            yield _DocProxy(doc, dry_run=self.dry_run)
        else:
            with self.create_backup(fn) as fn_backup:
                try:
                    yield _DocProxy(doc, dry_run=self.dry_run)
                except:
                    logger.warning("Error during doc merge, restoring backup...")
                    self.copy(fn_backup, doc._filename)
                    raise


# Definition of default merge strategies

class FileMerge(object):
    "Collection of file merge strategies."

    @classmethod
    def keys(cls):
        return ('theirs', 'ours', 'ask', 'by_timestamp')

    def theirs(fn_src, fn_dst):
        "Always merge files on conflict."
        return True

    def ours(fn_src, fn_dst):
        "Never merge files on conflict."
        return False

    def ask(fn_src, fn_dst):
        "Ask whether a file should be merged interactively."
        return query_yes_no(
            "Overwrite file '{}' with '{}'?".format(fn_src, fn_dst),
            'no')

    def by_timestamp(fn_src, fn_dst):
        "Merge a file based on its modification time stamp."
        return os.path.getmtime(fn_src) > os.path.getmtime(fn_dst)


class DocMerge(object):
    "Collection of document merge functions."

    NO_MERGE = False
    "Do not merge documents."

    COPY = 'copy'
    "Copy documents like all other files."

    def update(src, dst):
        "Perform simple update."
        for key in src.keys():
            dst[key] = src[key]

    class ByKey(object):
        "Merge documents key by key."

        def __init__(self, key_strategy=None):
            if isinstance(key_strategy, str):
                self.key_strategy = lambda k: re.match(key_strategy, k)
            else:
                self.key_strategy = key_strategy
            self.skipped_keys = set()

        def __str__(self):
            return "{}({})".format(type(self).__name__, self.key_strategy)

        def __call__(self, src, dst):
            if src == dst:
                return
            for key, value in src.items():
                if key in dst:
                    if dst[key] == value:
                        continue
                    elif self.key_strategy is None or not self.key_strategy(key):
                        self.skipped_keys.add(key)
                        continue
                    elif isinstance(value, Mapping):
                        try:
                            self(src[key], dst[key])
                            continue
                        except KeyError:
                            pass
                dst[key] = value


def merge_jobs(src, dst, exclude=None, strategy=None, doc_merge=None, dry_run=False):
    "Merge two jobs."

    # check src and dst compatiblity
    assert type(src) == type(dst)
    assert src.get_id() == dst.get_id()
    assert src.FN_MANIFEST == dst.FN_MANIFEST
    assert src.FN_DOCUMENT == dst.FN_DOCUMENT

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

    _merge_dirs(src.workspace(), dst.workspace(), exclude, strategy, proxy)

    if not (doc_merge is DocMerge.NO_MERGE or doc_merge == DocMerge.COPY):
        with proxy.create_doc_backup(dst.document) as dst_proxy:
            doc_merge(src.document, dst_proxy)


def merge_projects(source, destination, exclude=None, strategy=None, doc_merge=None,
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
        if schema_dst and schema_src != schema_dst:
            if schema_src.difference(schema_dst) or schema_dst.difference(schema_src):
                raise MergeSchemaConflict(schema_src, schema_dst)

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
            merge_jobs(src_job, dst_job, exclude, strategy, doc_merge, proxy)
            num_merged += 1
            logger.more("Merged job '{}'.".format(src_job))
    logger.info("Cloned {} and merged {} job(s).".format(num_cloned, num_merged))

    # Provide some information about skipped document keys.
    skipped_keys = getattr(doc_merge, 'skipped_keys', None)
    if skipped_keys:
        logger.info("Skipped {} document key(s).".format(len(skipped_keys)))
        logger.more("Skipped key(s): {}".format(', '.join(skipped_keys)))
    return skipped_keys
