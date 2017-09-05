# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import shutil
import filecmp
import logging
from collections.abc import Mapping
from collections import OrderedDict

from .errors import DestinationExistsError
from .errors import MergeConflict
from .utility import query_yes_no


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
    'MERGE_STRATEGIES',
    'ask',
    'ours',
    'theirs',
    'last_modified',
    ]


# Definition of default merge strategies
def theirs(fn_src, fn_dst):
    "Merge strategy: Always merge files on conflict."
    return True


def ours(fn_src, fn_dst):
    "Merge strategy: Never merge files on conflict."
    return False


def ask(fn_src, fn_dst):
    "Merge strategy: Ask whether a file should be merged interactively."
    return query_yes_no(
        "Overwrite file '{}' with '{}'?".format(fn_src, fn_dst),
        'no')


def last_modified(fn_src, fn_dst):
    "Merge strategy: Merge a file based on its modification time stamp."
    return os.path.getmtime(fn_src) > os.path.getmtime(fn_dst)


MERGE_STRATEGIES = OrderedDict([
    ('ask', ask),
    ('ours', ours),
    ('theirs', theirs),
    ('last_modified', last_modified),
])
"A ordered dictionary of default merge strategies."


# Merge algorithms

def _merge_dicts(src, dst, strategy):
    if src == dst:
        return set()
    skipped = set()
    for key, value in src.items():
        if key in dst:
            if dst[key] == value:
                continue
            elif strategy is None or not strategy(key):
                skipped.add(key)
                continue
            elif isinstance(value, Mapping):
                try:
                    child = dst[key]
                    skipped.update(_merge_dicts(src[key], child, strategy))
                    assert src[key] == child
                    continue
                except KeyError:
                    pass

        logger.more("Merge key '{}'.".format(key))
        dst[key] = value
    return skipped


def _merge_json_dicts(src, dst, strategy):
    if dst._filename is None or not os.path.isfile(dst._filename):
        return _merge_dicts(src, dst, strategy)
    else:
        try:
            # Create backup copy
            shutil.copy(dst._filename, dst._filename + '~')
            return _merge_dicts(src, dst, strategy)
        except Exception:
            # Try to restore backup
            logger.warning("Error during json dict merge, restoring backup...")
            shutil.copy(dst._filename + '~', dst._filename)
            raise
        finally:
            os.remove(dst._filename + '~')


def _merge_dirs(src, dst, strategy, exclude):
    "Merge two directories."
    diff = filecmp.dircmp(src, dst)
    for fn in diff.left_only:
        if fn in exclude:
            continue
        fn_src = os.path.join(src, fn)
        fn_dst = os.path.join(dst, fn)
        if os.path.isfile(fn_src):
            logger.more("Copy file '{}'.".format(fn))
            shutil.copy(fn_src, fn_dst)
        else:
            logger.more("Copy tree '{}'.".format(fn))
            shutil.copytree(os.path.join(src, fn), os.path.join(dst, fn))
    for fn in diff.diff_files:
        if fn in exclude:
            continue
        if strategy is None:
            raise MergeConflict(fn)
        else:
            fn_src = os.path.join(src, fn)
            fn_dst = os.path.join(dst, fn)
            if strategy(fn_src, fn_dst):
                logger.more("Copy file '{}'.".format(fn))
                shutil.copy(fn_src, fn_dst)
            else:
                logger.more("Skip file '{}'.".format(fn))
    for subdir in diff.subdirs:
        _merge_dirs(os.path.join(src, subdir), os.path.join(dst, subdir), strategy, exclude)


def merge_jobs(src_job, dst_job, strategy=None, doc_strategy=None, exclude=None):
    "Merge two jobs."
    if exclude is None:
        exclude = []
    logger.info("Merging job '{}'...".format(src_job))
    assert type(src_job) == type(dst_job)
    assert src_job.get_id() == dst_job.get_id()
    assert src_job.FN_MANIFEST == dst_job.FN_MANIFEST
    assert src_job.FN_DOCUMENT == dst_job.FN_DOCUMENT
    exclude.append(src_job.FN_MANIFEST)
    exclude.append(src_job.FN_DOCUMENT)
    _merge_dirs(src_job.workspace(), dst_job.workspace(), strategy, exclude=exclude)
    return _merge_json_dicts(src_job.doc, dst_job.doc, doc_strategy)


def merge_projects(source, destination, strategy=None, doc_strategy=None, subset=None):
    """Merge the source project into the destination project.

    Try to clone all jobs from the source to the destination.
    If the destination job already exist, try to merge the job using the
    optionally specified strategy.
    """
    if subset is not None:  # The subset argument may be a jobs or job ids sequence.
        subset = {str(j) for j in subset}
    if source == destination:
        raise ValueError("Source and destination can't be the same!")
    logger.info("Merging project '{}' into '{}'.".format(source, destination))
    logger.more("'{}' -> '{}'".format(source.root_directory(), destination.root_directory()))
    logger.more("Merge strategy: '{}'".format(strategy))
    if subset:
        logger.more("Merging over subset (size={}).".format(len(subset)))
    skipped = set()
    for src_job in source:
        if subset is not None and src_job.get_id() not in subset:
            logger.more("{} not in selected subset.".format(src_job))
            continue
        try:
            destination.clone(src_job)
        except DestinationExistsError as e:
            dst_job = destination.open_job(id=src_job.get_id())
            skipped.update(merge_jobs(src_job, dst_job, strategy, doc_strategy))
    skipped.update(_merge_json_dicts(source.document, destination.document, doc_strategy))
    if skipped:
        logger.info("Skipped {} document keys.".format(len(skipped)))
        logger.more("Skipped keys: {}".format(', '.join(skipped)))
    return skipped
