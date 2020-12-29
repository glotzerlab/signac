# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Linked view classes."""

import errno
import logging
import os
import sys
from itertools import chain

from .utility import _mkdir_p

logger = logging.getLogger(__name__)


def create_linked_view(project, prefix=None, job_ids=None, index=None, path=None):
    """Create or update a persistent linked view of the selected data space.

    Parameters
    ----------
    project : signac.Project
        Project handle.
    prefix : str
        The path where the linked view will be created or updated (Default value = None).
    job_ids : iterable
        If None (the default), create the view for the complete data space,
        otherwise only for this iterable of job ids.
    index :
        A document index (Default value = None).
    path :
        The path (function) used to structure the linked data space (Default value = None).

    Returns
    -------
    dict
        A dictionary that maps the source directory paths to the linked
        directory paths.

    Raises
    ------
    OSError
        Linked views cannot be created on Windows because
        symbolic links are not supported by the platform.
    ValueError
        When the selected data space is provided with an insufficient index.
    RuntimeError
        When state points contain one of ``[os.sep, " ", "*"]``.

    """
    from .import_export import _check_directory_structure_validity, _make_path_function

    # Windows does not support the creation of symbolic links.
    if sys.platform == "win32":
        raise OSError(
            "signac cannot create linked views on Windows, because "
            "symbolic links are not supported by the platform."
        )

    if prefix is None:
        prefix = "view"

    if index is None:
        if job_ids is None:
            index = [{"_id": job.id, "statepoint": job.statepoint()} for job in project]
            jobs = list(project)
        else:
            index = [
                {"_id": job_id, "statepoint": project.open_job(id=job_id).statepoint()}
                for job_id in job_ids
            ]
            jobs = list(project.open_job(id=job_id) for job_id in job_ids)
    elif job_ids is not None:
        if not isinstance(job_ids, set):
            job_ids = set(job_ids)
        index = [doc for doc in index if doc["_id"] in job_ids]
        jobs = list(project.open_job(id=job_id) for job_id in job_ids)
        if not job_ids.issubset({doc["_id"] for doc in index}):
            raise ValueError("Insufficient index for selected data space.")

    key_list = [k for job in jobs for k in job.statepoint().keys()]
    value_list = [v for job in jobs for v in job.statepoint().values()]
    item_list = key_list + value_list
    bad_chars = [os.sep, " ", "*"]
    bad_items = [
        item
        for item in item_list
        for char in bad_chars
        if isinstance(item, str) and char in item
    ]

    if any(bad_items):
        err_msg = " ".join(
            [
                f"In order to use view, statepoints should not contain {bad_chars}:",
                *bad_items,
            ]
        )
        raise RuntimeError(err_msg)

    path_function = _make_path_function(jobs, path)

    links = {}
    for job in jobs:
        paths = os.path.join(path_function(job), "job")
        links[paths] = job.workspace()
    if not links:  # data space contains less than two elements
        for job in project.find_jobs():
            links["./job"] = job.workspace()
        assert len(links) < 2
    _check_directory_structure_validity(links.keys())

    _update_view(prefix, links)
    return links


def _update_view(prefix, links, leaf="job"):
    """Update an existing linked view hierarchy in place.

    Parameters
    ----------
    prefix : str
        The path where the linked view will be created or updated (Default value = None).
    links : dict
        Linked view .
    leaf : str
        The name of the leaf directories in the view
        directory tree (Default value = 'job').

    """
    obsolete, to_update, new = _analyze_view(prefix, links)
    num_ops = len(obsolete) + 2 * len(to_update) + len(new)
    if num_ops:
        logger.info(f"Generating current view in '{prefix}' ({num_ops} operations)...")
    else:
        logger.info(f"View in '{prefix}' is up to date.")
        return
    logger.debug("Removing {} obsolete links.".format(len(obsolete)))
    for path in obsolete:
        p = os.path.join(prefix, path)
        try:
            os.unlink(p)
        except OSError:
            os.rmdir(p)
    logger.debug(
        "Creating {} new and updating {} existing links.".format(
            len(new), len(to_update)
        )
    )
    for path in to_update:
        os.unlink(os.path.join(prefix, path))
    for path in chain(new, to_update):
        dst = os.path.join(prefix, path)
        src = os.path.relpath(links[path], os.path.split(dst)[0])
        _make_link(src, dst)


def _analyze_view(prefix, links, leaf="job"):
    """Analyze an existing view to prepare for update.

    Parameters
    ----------
    prefix : str
        The path where the linked view will be created or updated (Default value = None).
    links : dict
        Linked view.
    leaf : str
        The name of the leaf directories in the view
        directory tree (Default value = 'job').

    Returns
    -------
    tuple
        tuple that contains: (list of outdated links, list of links to update, set of new links).

    """
    logger.info(f"Analyzing view prefix '{prefix}'...")
    existing_paths = {os.path.join(p, leaf) for p in _find_all_links(prefix, leaf)}
    existing_tree = _build_tree(existing_paths)
    for path in links:
        _color_path(existing_tree, path.split(os.sep))
    obsolete = []
    dead_branches = _find_dead_branches(existing_tree)
    for branch in reversed(sorted(dead_branches, key=len)):
        if branch:
            obsolete.append(os.path.join(*(n.name for n in branch)))
    if "." in obsolete:
        obsolete.remove(".")
    keep_or_update = existing_paths.intersection(links.keys())
    new = set(links.keys()).difference(keep_or_update)
    to_update = [
        p
        for p in keep_or_update
        if os.path.realpath(os.path.join(prefix, p)) != links[p]
    ]
    return obsolete, to_update, new


def _make_link(src, dst):
    """Create a symbolic link and all directories leading to it.

    Parameters
    ----------
    src : str
        Name of directory/file to create a symbolic link.
    dst : str
        Destination symbolic link directory/file name.

    """
    _mkdir_p(os.path.dirname(dst))
    try:
        os.symlink(src, dst, target_is_directory=True)
    except OSError as error:
        if error.errno == errno.EEXIST:
            if os.path.realpath(src) == os.path.realpath(dst):
                return
        raise


def _find_all_links(root, leaf="job"):
    """Find all symbolic links under root.

    Parameters
    ----------
    root : str
        Project root directory.
    leaf : str
        The name of the leaf directories in the view
        directory tree (Default value = 'job').

    Yields
    ------
    str
        Relative path to root from ``leaf``.

    """
    for dirpath, dirnames, filenames in os.walk(root):
        for dirname in dirnames:
            if dirname == leaf:
                yield os.path.relpath(dirpath, root)
                break
        for filename in filenames:
            if filename == leaf:
                yield os.path.relpath(dirpath, root)
                break


class _Node:
    """Generic graph-node class."""

    def __init__(self, name=None, value=None):
        self.name = name
        self.value = value
        self.children = {}

    def get_child(self, name):
        """Get child node corresponding to the name passed.

        Parameters
        ----------
        name : str
            Name of child node to get.

        Returns
        -------
        :class:`~signac.contrib.linked_view._Node`
            The requested child node.

        """
        return self.children.setdefault(name, type(self)(name))

    def __str__(self):
        return f"_Node({self.name}, {self.value})"

    __repr__ = __str__


def _build_tree(paths):
    """Build a graph structure for paths.

    Parameters
    ----------
    paths : list
        A list of paths to views that already exist.

    Returns
    -------
    :class:`~signac.contrib.linked_view._Node`
        Graph structure for path.

    """
    root = _Node()
    for path in paths:
        node = root
        for p in path.split(os.sep):
            node = node.get_child(p)
    return root


def _get_branches(root, branch=None):
    """Get all branches from the root node.

    Parameters
    ----------
    root : :class:`~signac.contrib.linked_view._Node`
        Root node.
    branch : list
        The current list of branches that has been collected,
        used in recursive calls to build up the branches starting
        at the root (Default value = None).

    Yields
    ------
    list
        Branches for the root node.

    """
    if branch is None:
        branch = []
    else:
        branch = list(branch) + [root]
    if root.children:
        for child in root.children.values():
            yield from _get_branches(child, branch)
    else:
        yield branch


def _color_path(root, path):
    """Color the path from root by setting value to True.

    Parameters
    ----------
    root : :class:`~signac.contrib.linked_view._Node`
        Root node.
    path : list
        The name of the directory/file to color (set value to True).

    """
    root.value = True
    for name in path:
        root = root.get_child(name)
        root.value = True


def _find_dead_branches(root, branch=None):
    """Find all branches considered dead (not-colored).

    Parameters
    ----------
    root : :class:`~signac.contrib.linked_view._Node`
        Root node.
    branch : list
        The current list of branches that has been collected,
        used in recursive calls to build up the branches starting
        at the root (Default value = None).

    Yields
    ------
    list
        Branches that are considered as dead (not-colored).

    """
    if branch is None:
        branch = []
    else:
        branch = list(branch) + [root]
    if root.children:
        for child in root.children.values():
            yield from _find_dead_branches(child, branch)
    if not root.value:
        yield branch
