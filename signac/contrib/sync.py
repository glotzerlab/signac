# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import sys
import shutil
import filecmp
import logging
from collections.abc import Mapping
from collections import OrderedDict

from .errors import DestinationExistsError
from .errors import NoMergeStrategyError
from .utility import query_yes_no

logger = logging.getLogger(__name__)


# Strategies

def by_timestamp(fn_src, fn_dst):
    return os.path.getmtime(fn_src) > os.path.getmtime(fn_dst)


def ours(fn_src, fn_dst):
    return False


def theirs(fn_src, fn_dst):
    return True


def ask(fn_src, fn_dst):
    return query_yes_no(
        "Overwrite file '{}' with '{}'?".format(fn_src, fn_dst),
        'no')


MERGE_STRATEGIES = OrderedDict([
    ('ask', ask),
    ('ours', ours),
    ('theirs', theirs),
    ('by_timestamp', by_timestamp),
])


# Merge implementation

def _merge_dicts(src, dst, strategy, log):
    if src == dst:
        return set()
    skipped = set()
    for key, value in src.items():
        if key in dst:
            if dst[key] == value:
                continue
            elif strategy is None or not strategy(key):
                log("Skipping key '{}'!".format(key))
                skipped.add(key)
                continue
            elif isinstance(value, Mapping):
                try:
                    child = dst[key]
                    skipped.update(_merge_dicts(src[key], child, strategy, log))
                    assert src[key] == child
                    continue
                except KeyError:
                    pass

        log("Merge key '{}'.".format(key))
        dst[key] = value
    return skipped


def _merge_json_dicts(src, dst, strategy, log):
    if dst._filename is None or not os.path.isfile(dst._filename):
        return _merge_dicts(src, dst, strategy, log)
    else:
        try:
            # Create backup copy
            shutil.copy(dst._filename, dst._filename + '~')
            return _merge_dicts(src, dst, strategy, log)
        except Exception:
            # Try to restore backup
            log("Error during json dict merge, restoring backup...")
            shutil.copy(dst._filename + '~', dst._filename)
            raise
        finally:
            os.remove(dst._filename + '~')


def _merge_dirs(src, dst, strategy, exclude, log):
    "Merge two directories."
    diff = filecmp.dircmp(src, dst)
    for fn in diff.left_only:
        if fn in exclude:
            continue
        fn_src = os.path.join(src, fn)
        fn_dst = os.path.join(dst, fn)
        if os.path.isfile(fn_src):
            log("Copy file '{}'.".format(fn))
            shutil.copy(fn_src, fn_dst)
        else:
            log("Copy tree '{}'.".format(fn))
            shutil.copytree(os.path.join(src, fn), os.path.join(dst, fn))
    for fn in diff.diff_files:
        if fn in exclude:
            continue
        if strategy is None:
            raise NoMergeStrategyError(fn)
        else:
            fn_src = os.path.join(src, fn)
            fn_dst = os.path.join(dst, fn)
            if strategy(fn_src, fn_dst):
                log("Copy file '{}'.".format(fn))
                shutil.copy(fn_src, fn_dst)
            else:
                log("Skip file '{}'.".format(fn))
    for subdir in diff.subdirs:
        _merge_dirs(os.path.join(src, subdir), os.path.join(dst, subdir), strategy, exclude, log)


def merge_jobs(src_job, dst_job, strategy=None, doc_strategy=None, exclude=None, log=None):
    "Merge two jobs."
    if exclude is None:
        exclude = []
    if log is None:
        log = sys.stdout.write
    assert type(src_job) == type(dst_job)
    assert src_job.get_id() == dst_job.get_id()
    assert src_job.FN_MANIFEST == dst_job.FN_MANIFEST
    assert src_job.FN_DOCUMENT == dst_job.FN_DOCUMENT
    exclude.append(src_job.FN_MANIFEST)
    exclude.append(src_job.FN_DOCUMENT)
    _merge_dirs(src_job.workspace(), dst_job.workspace(), strategy, exclude=exclude, log=log)
    return _merge_json_dicts(src_job.doc, dst_job.doc, doc_strategy, log)


def merge(source, destination, strategy=None, doc_strategy=None, log=None):
    """Merge the source project into the destination project.

    Try to clone all jobs from the source to the destination.
    If the destination job already exist, try to merge the job using the
    optionally specified strategy.
    """
    if log is None:
        log = logger.info
    if source == destination:
        raise ValueError("Source and destination can't be the same!")
    skipped = set()
    for src_job in source:
        try:
            destination.clone(src_job)
        except DestinationExistsError as e:
            dst_job = destination.open_job(id=src_job.get_id())
            skipped.update(merge_jobs(src_job, dst_job, strategy, doc_strategy, log=log))
    skipped.update(_merge_json_dicts(source.document, destination.document, doc_strategy, log))
    return skipped
