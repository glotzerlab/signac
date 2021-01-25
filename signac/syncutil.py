# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Utilities for synchronization."""

import filecmp
import logging
import os
import shutil
from contextlib import contextmanager
from copy import deepcopy
from filecmp import dircmp

from deprecation import deprecated

from .version import __version__

LEVEL_MORE = logging.INFO - 5

logger = logging.getLogger("sync")
logging.addLevelName(LEVEL_MORE, "MORE")
logging.MORE = LEVEL_MORE  # type: ignore


def log_more(msg, *args, **kwargs):
    """Log using LEVEL_MORE."""
    logger.log(LEVEL_MORE, msg, *args, **kwargs)


logger.more = log_more  # type: ignore


@deprecated(
    deprecated_in="1.6.0",
    removed_in="2.0.0",
    current_version=__version__,
    details="Use shutil.copytree instead.",
)
def copytree(src, dst, copy_function=shutil.copy2, symlinks=False):
    """Recursively copy a directory tree from src to dst, using a custom copy function.

    Implementation adapted from https://docs.python.org/3/library/shutil.html#copytree-example.

    Parameters
    ----------
    src : str
        Source directory tree.
    dst : str
        Destination directory tree.
    copy_function :
        Function used to copy (Default value = ``shutil.copy2``).
    symlinks : bool
        Whether to copy symlinks (Default value = False).

    """
    os.makedirs(dst)
    names = os.listdir(src)
    errors = []
    for name in names:
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        try:
            if symlinks and os.path.islink(srcname):
                linkto = os.readlink(srcname)
                os.symlink(linkto, dstname)
            elif os.path.isdir(srcname):
                copytree(srcname, dstname, copy_function, symlinks)
            else:
                copy_function(srcname, dstname)
        except OSError as why:
            errors.append((srcname, dstname, str(why)))
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except shutil.Error as err:
            errors.extend(err.args[0])
    if errors:
        raise shutil.Error(errors)


class dircmp_deep(dircmp):
    """Deep directory comparator."""

    def phase3(self):
        """Find out differences between common files."""
        xx = filecmp.cmpfiles(self.left, self.right, self.common_files, shallow=False)
        self.same_files, self.diff_files, self.funny_files = xx

    methodmap = dict(dircmp.methodmap)
    # The type check for the following line must be ignored.
    # See: https://github.com/python/mypy/issues/708
    methodmap["same_files"] = methodmap["diff_files"] = phase3  # type: ignore


class _DocProxy:
    """Proxy object for document (mapping) modifications.

    This proxy is used to keep track of changes and ensure that
    dry runs do not actually modify any data.

    Parameters
    ----------
    doc : dict
        Document data.
    dry_run : bool
        Do not actually perform any data modification operation, but still log
        the action (Default value = False).

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
        logger.more(f"Set '{key}'='{value}'.")
        if not self.dry_run:
            self.doc[key] = value

    def keys(self):
        """Return keys of proxy data."""
        return self.doc.keys()

    def clear(self):
        """Clear proxy data."""
        self.doc.clear()

    def update(self, other):
        """Update proxy data with other."""
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


class _FileModifyProxy:
    """Proxy used for data modification.

    This proxy is used for all file data modification to keep
    track of changes and to ensure that dry runs do not actually
    modify any data.

    Parameters
    ----------
    root :
        Root path.
    follow_symlinks : bool
        Whether to follow symlinks (Default value = True).
    permissions : bool
        Whether to preserve permissions (Default value = False).
    times : bool
        Whether to preserve timestamps (Default value = False).
    owner : bool
        Whether to preserve owner (Default value = False).
    group : bool
        Whether to preserve group (Default value = False).
    dry_run : bool
        If True, do not actually perform any data modification operation, but still log
        the action (Default value = False).
    collect_stats : bool
        Whether to collect stats (Default value = False).

    """

    def __init__(
        self,
        root=None,
        follow_symlinks=True,
        permissions=False,
        times=False,
        owner=False,
        group=False,
        dry_run=False,
        collect_stats=False,
    ):
        self.root = root
        self.follow_symlinks = follow_symlinks
        self.permissions = permissions
        self.times = times
        self.owner = owner
        self.group = group
        self.dry_run = dry_run
        self.stats = dict(num_files=0, volume=0) if collect_stats else None

    # Internal proxy functions

    def _copy(self, src, dst):
        """Copy src to dst."""
        if not self.dry_run:
            shutil.copy(src, dst)

    def _copy_p(self, src, dst):
        """Copy src to dst with permissions."""
        if not self.dry_run:
            shutil.copy(src, dst)
            shutil.copymode(src, dst)

    def _copy2(self, src, dst):
        """Copy src to dst with preserved metadata."""
        if not self.dry_run:
            shutil.copy2(src, dst)

    def _remove(self, path):
        """Remove path."""
        if not self.dry_run:
            os.remove(path)

    # Public functions

    def remove(self, path):
        """Remove path."""
        logger.more("Remove path '{}'.".format(os.path.relpath(path)))
        self._remove(path)

    def copy(self, src, dst):
        """Copy src to dst."""
        if self.dry_run and self.root is not None:
            print(os.path.relpath(src, self.root))
        if os.path.islink(src) and not self.follow_symlinks:
            link_target = os.readlink(src)
            logger.more(
                "Creating link '{}' -> '{}'.".format(
                    os.path.relpath(dst), os.path.relpath(link_target)
                )
            )
            if os.path.isfile(dst):
                self.remove(dst)
            if not self.dry_run:
                os.symlink(link_target, dst)
        else:
            msg = "Copy file{{}} '{}' -> '{}'.".format(
                os.path.relpath(src), os.path.relpath(dst)
            )
            if self.permissions and self.times:
                logger.more(msg.format(" (preserving: permissions, times)"))
                self._copy2(src, dst)
            elif self.permissions:
                logger.more(msg.format(" (preserving: permissions)"))
                self._copy_p(src, dst)
            elif self.times:
                raise ValueError("Cannot copy timestamps without permissions.")
            else:
                logger.more(msg.format(""))
                self._copy(src, dst)
            if self.owner or self.group or self.stats is not None:
                stat = os.stat(src)
                if self.stats is not None:
                    self.stats["num_files"] += 1
                    self.stats["volume"] += stat.st_size
                if self.owner or self.group:
                    logger.more(
                        "Copy owner/group '{}' -> '{}'".format(
                            os.path.relpath(src), os.path.relpath(dst)
                        )
                    )
                    if not self.dry_run:
                        os.chown(
                            dst,
                            uid=stat.st_uid if self.owner else -1,
                            gid=stat.st_gid if self.group else -1,
                        )

    def copytree(self, src, dst, **kwargs):
        """Copy tree src to dst."""
        logger.more(
            "Copy tree '{}' -> '{}'.".format(os.path.relpath(src), os.path.relpath(dst))
        )
        copytree(src, dst, copy_function=self.copy, **kwargs)

    @contextmanager
    def create_backup(self, path):
        """Create a backup of path."""
        logger.debug("Create backup of '{}'.".format(os.path.relpath(path)))
        path_backup = path + "~"
        if os.path.isfile(path_backup):
            raise RuntimeError(
                "Failed to create backup, file already exists: '{}'.".format(
                    os.path.relpath(path_backup)
                )
            )
        try:
            self._copy2(path, path_backup)
            yield path_backup
        except:  # noqa roll-back
            logger.more("Error occurred, restoring backup...")
            self._copy2(path_backup, path)
            raise
        finally:
            logger.debug("Remove backup of '{}'.".format(os.path.relpath(path)))
            self._remove(path_backup)

    @contextmanager
    def create_doc_backup(self, doc):
        """Create a backup of doc."""
        proxy = _DocProxy(doc, dry_run=self.dry_run)
        fn = getattr(doc, "filename", getattr(doc, "_filename", None))
        if not len(proxy) or fn is None or not os.path.isfile(fn):
            backup = deepcopy(doc)  # use in-memory backup
            try:
                yield proxy
            except:  # noqa roll-back
                proxy.clear()
                proxy.update(backup)
                raise
        else:
            with self.create_backup(fn):
                yield proxy
