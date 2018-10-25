# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import errno
import logging
from itertools import chain

from ..common import six

logger = logging.getLogger(__name__)


def create_linked_view(project, prefix=None, job_ids=None, index=None, path=None):
    """Create or update a persistent linked view of the selected data space."""
    from .import_export import _make_path_function
    from .import_export import _check_directory_structure_validity
    if prefix is None:
        prefix = 'view'

    if index is None:
        if job_ids is None:
            index = [{'_id': job._id, 'statepoint': job.sp()} for job in project]
            jobs = list(project)
        else:
            index = [{'_id': job_id, 'statepoint': project.open_job(id=job_id).sp()}
                     for job_id in job_ids]
            jobs = list(project.open_job(id=job_id) for job_id in job_ids)
    elif job_ids is not None:
        if not isinstance(job_ids, set):
            job_ids = set(job_ids)
        index = [doc for doc in index if doc['_id'] in job_ids]
        jobs = list(project.open_job(id=job_id) for job_id in job_ids)
        if not job_ids.issubset({doc['_id'] for doc in index}):
            raise ValueError("Insufficient index for selected data space.")

    path_function = _make_path_function(jobs, path)

    links = dict()
    for job in jobs:
        paths = os.path.join(path_function(job), 'job')
        links[paths] = job.workspace()
    if not links:   # data space contains less than two elements
        for job in project.find_jobs():
            links['./job'] = job.workspace()
        assert len(links) < 2
    _check_directory_structure_validity(links.keys())

    _update_view(prefix, links)
    return links


def _update_view(prefix, links, leaf='job'):
    "Update an existing linked view hierarchy in prefix."
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


def _analyze_view(prefix, links, leaf='job'):
    "Analyze an existing view to prepare for update."
    logger.info("Analyzing view prefix '{}'...".format(prefix))
    existing_paths = {os.path.join(p, leaf) for p in _find_all_links(prefix, leaf)}
    existing_tree = _build_tree(existing_paths)
    for path in links:
        _color_path(existing_tree, path.split(os.sep))
    obsolete = []
    dead_branches = _find_dead_branches(existing_tree)
    for branch in reversed(sorted(dead_branches, key=len)):
        if branch:
            obsolete.append(os.path.join(* (n.name for n in branch)))
    if '.' in obsolete:
        obsolete.remove('.')
    keep_or_update = existing_paths.intersection(links.keys())
    new = set(links.keys()).difference(keep_or_update)
    to_update = [p for p in keep_or_update if
                 os.path.realpath(os.path.join(prefix, p)) != links[p]]
    return obsolete, to_update, new


def _make_link(src, dst):
    "Create a symbolic link and all directories leading to it."
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


def _find_all_links(root, leaf='job'):
    "Find all symbolic links under root."
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
    "Generic graph-node class."

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
    "Build a graph structure for paths."
    root = _Node()
    for path in paths:
        node = root
        for p in path.split(os.sep):
            node = node.get_child(p)
    return root


def _get_branches(root, branch=None):
    "Get all branches from the root node."
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
    "Color the path from root by setting value to True."
    root.value = True
    for name in path:
        root = root.get_child(name)
        root.value = True


def _find_dead_branches(root, branch=None):
    "Find all branches considered dead (not-colored)."
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
