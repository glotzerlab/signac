# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Synchronization of jobs and projects.

Jobs may be synchronized by copying all data from the source job to the
destination job. This means all files are copied and the documents
are synchronized. Conflicts, that means both jobs contain conflicting
data, may be resolved with a user defined strategy.

The synchronization of projects is in essence the synchronization of all jobs
which are in the destination project with the ones in the source project and
the sync synchronization of the project document. If a specific job does not
exist yet at the destination it is simply cloned, otherwise it is synchronized.

A sync strategy is a function (or functor) that takes the source job,
the destination job, and the name of the file generating the conflict
as arguments and returns the decision whether to overwrite the file as
Boolean. There are some default strategies defined within this module as
part of the :class:`~.FileSync` class. These are the default strategies:

    1. always -- Always overwrite on conflict.
    2. never -- Never overwrite on conflict.
    3. update -- Overwrite when the modification time of the source file is newer.
    4. Ask -- Ask the user interactively about each conflicting filename.

For example, to synchronize two projects resolving conflicts by modification time, use:

.. code-block:: python

    dest_project.sync(source_project, strategy=sync.FileSync.update)

Unlike files, which are always either overwritten as a whole or not, documents
can be synchronized more fine-grained with a *sync function*. Such a function (or
functor) takes the source and the destination document as arguments and performs
the synchronization. The user is encouraged to implement their own sync functions,
but there are a few default functions implemented as part of the :class:`~.DocSync` class:

    1. NO_SYNC -- Do not perform any synchronization.
    2. COPY -- Apply the same strategy used to resolve file conflicts.
    3. update -- Equivalent to dst.update(src).
    4. ByKey -- Synchronize the source document key by key, more information below.

This is how we could synchronize two jobs, where the documents are synchronized
with a simple update function:

.. code-block:: python

    dst_job.sync(src_job, doc_sync=sync.DocSync.update)

The :class:`.DocSync.ByKey` functor attempts to synchronize the destination document
with the source document without overwriting any data. That means this function
behaves similar to :func:`~.DocSync.update` for a non-intersecting set of keys,
but in addition will preserve nested mappings without overwriting values. In addition,
any key conflict, that means keys that are present in both documents, but have
differing data, will lead to the raise of a :class:`.DocumentSyncConflict` exception.
The user may expclitly decide to overwrite certain keys by providing a "key-strategy",
which is a function that takes the conflicting key as argument, and returns the
decision whether to overwrite that specific key as Boolean. For example, to sync
two jobs, where conflicting keys should only be overwritten if they contain the
term 'foo', we could execute:

.. code-block:: python

    dst_job.sync(src_job, doc_sync=sync.DocSync.ByKey(lambda key: 'foo' in key))

This means that all documents are synchronized 'key-by-key' and only conflicting keys that
contain the word "foo" will be overwritten, any other conflicts would lead to the
raise of a :class:`~.DocumentSyncConflict` exception. A key-strategy may also be
a regular expression, so the synchronization above could also be achieved with:

.. code-block:: python

    dst_job.sync(src_job, doc_sync=sync.DocSync.ByKey('foo'))
"""
import logging
import os
import re
import shutil
from collections import defaultdict as ddict
from collections import namedtuple
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from filecmp import cmpfiles, dircmp
from multiprocessing.pool import ThreadPool

from ._utility import _query_yes_no, _safe_relpath
from .errors import (
    DestinationExistsError,
    DocumentSyncConflict,
    FileSyncConflict,
    SchemaSyncConflict,
)

__all__ = [
    "FileSync",
    "DocSync",
    "sync_jobs",
    "sync_projects",
]

# Definition of helpers for syncing


_LEVEL_MORE = logging.INFO - 5

logger = logging.getLogger("sync")
logging.addLevelName(_LEVEL_MORE, "MORE")
logging.MORE = _LEVEL_MORE  # type: ignore


def _log_more(msg, *args, **kwargs):
    """Log using _LEVEL_MORE."""
    logger.log(_LEVEL_MORE, msg, *args, **kwargs)


logger.more = _log_more  # type: ignore


class _dircmp_deep(dircmp):
    """Deep directory comparator."""

    def phase3(self):
        """Find out differences between common files."""
        xx = cmpfiles(self.left, self.right, self.common_files, shallow=False)
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
    dry_run : bool, optional
        Do not actually perform any data modification operation, but still log
        the action (Default value = False).

    """

    def __init__(self, doc, dry_run=False):
        self.doc = doc
        self.dry_run = dry_run

    def __str__(self):
        return f"_DocProxy({str(self.doc)})"

    def __repr__(self):
        return f"_DocProxy({repr(self.doc)})"

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
    root : str, optional
        Root path.
    follow_symlinks : bool, optional
        Whether to follow symlinks (Default value = True).
    permissions : bool, optional
        Whether to preserve permissions (Default value = False).
    times : bool, optional
        Whether to preserve timestamps (Default value = False).
    owner : bool, optional
        Whether to preserve owner (Default value = False).
    group : bool, optional
        Whether to preserve group (Default value = False).
    dry_run : bool, optional
        If True, do not actually perform any data modification operation, but still log
        the action (Default value = False).
    collect_stats : bool, optional
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
        logger.more(f"Remove path '{_safe_relpath(path)}'.")
        self._remove(path)

    def copy(self, src, dst):
        """Copy src to dst."""
        if self.dry_run and self.root is not None:
            print(_safe_relpath(src, self.root))
        if os.path.islink(src) and not self.follow_symlinks:
            link_target = os.readlink(src)
            logger.more(
                "Creating link '{}' -> '{}'.".format(
                    _safe_relpath(dst), _safe_relpath(link_target)
                )
            )
            if os.path.isfile(dst):
                self.remove(dst)
            if not self.dry_run:
                os.symlink(link_target, dst)
        else:
            msg = "Copy file '{}' -> '{}'.".format(
                _safe_relpath(src), _safe_relpath(dst)
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
                            _safe_relpath(src), _safe_relpath(dst)
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
        logger.more(f"Copy tree '{_safe_relpath(src)}' -> '{_safe_relpath(dst)}'.")
        shutil.copytree(src, dst, copy_function=self.copy, **kwargs)

    @contextmanager
    def create_backup(self, path):
        """Create a backup of path."""
        logger.debug(f"Create backup of '{_safe_relpath(path)}'.")
        path_backup = path + "~"
        if os.path.isfile(path_backup):
            raise RuntimeError(
                "Failed to create backup, file already exists: '{}'.".format(
                    _safe_relpath(path_backup)
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
            logger.debug(f"Remove backup of '{_safe_relpath(path)}'.")
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


# Definition of default sync strategies


class FileSync:
    """Collection of file synchronization strategies."""

    @classmethod
    def keys(cls):
        """Return keys."""
        return ("always", "never", "update", "Ask")

    @staticmethod
    def always(src, dst, fn):
        """Resolve sync conflicts by always overwriting."""
        return True

    @staticmethod
    def never(src, dst, fn):
        """Resolve sync conflicts by never overwriting."""
        return False

    @staticmethod
    def update(src, dst, fn):
        """Resolve sync conflicts based on newest modified timestamp."""
        return os.path.getmtime(src.fn(fn)) > os.path.getmtime(dst.fn(fn))

    class Ask:
        """Resolve sync conflicts by asking whether a file should be overwritten interactively."""

        def __init__(self):
            self.yes = set()
            self.no = set()

        def __call__(self, src, dst, fn):
            """Ask user if a file should be overwritten."""
            if fn in self.yes:
                return True
            elif fn in self.no:
                return False
            else:
                overwrite = _query_yes_no(f"Overwrite files named '{fn}'?", "no")
                if overwrite:
                    self.yes.add(fn)
                    return True
                else:
                    self.no.add(fn)
                    return False


class DocSync:
    """Collection of document synchronization functions."""

    NO_SYNC = False
    "Do not synchronize documents."

    COPY = "copy"
    "Copy (and potentially overwrite) documents like any other file."

    @staticmethod
    def update(src, dst):
        """Perform a simple update."""
        for key in src.keys():
            dst[key] = src[key]

    class ByKey:
        """Synchronize documents key by key."""

        def __init__(self, key_strategy=None):
            if isinstance(key_strategy, str):

                def regex_key_strategy(key):
                    """Match keys according to key_strategy."""
                    return re.match(key_strategy, key)

                self.key_strategy = regex_key_strategy
            else:
                self.key_strategy = key_strategy
            self.skipped_keys = set()

        def __str__(self):
            return f"{type(self).__name__}({self.key_strategy})"

        def __call__(self, src, dst, root=""):
            """Synchronize src and dst."""
            if src == dst:
                return
            for key, value in src.items():
                if key in dst:
                    if dst[key] == value:
                        continue
                    elif isinstance(value, Mapping):
                        self(src[key], dst[key], key + ".")
                        continue
                    elif self.key_strategy is None or not self.key_strategy(root + key):
                        self.skipped_keys.add(root + key)
                        continue
                dst[key] = value

            # Check for skipped keys and raise an exception in case that no strategy
            # was provided, otherwise just log them.
            if self.skipped_keys and not root:
                if self.key_strategy is None:
                    raise DocumentSyncConflict(self.skipped_keys)
                else:
                    logger.more("Skipped keys: {}".format(", ".join(self.skipped_keys)))


def _sync_job_workspaces(
    src, dst, strategy, exclude, copy, copytree, recursive=True, deep=False, subdir=""
):
    """Synchronize two job workspaces file by file, following the provided strategy."""
    if deep:
        diff = _dircmp_deep(src.fn(subdir), dst.fn(subdir))
    else:
        diff = dircmp(src.fn(subdir), dst.fn(subdir))

    for fn in diff.left_only:
        if exclude and any([re.match(p, fn) for p in exclude]):
            logger.debug(f"File named '{fn}' is skipped (excluded).")
            continue
        fn_src = os.path.join(src.path, subdir, fn)
        fn_dst = os.path.join(dst.path, subdir, fn)
        if os.path.isfile(fn_src):
            copy(fn_src, fn_dst)
        elif recursive:
            copytree(fn_src, fn_dst)
        else:
            logger.warning(f"Skip directory '{fn_src}'.")
    for fn in diff.diff_files:
        if exclude and any([re.match(p, fn) for p in exclude]):
            logger.debug(f"File named '{fn}' is skipped (excluded).")
            continue
        if strategy is None:
            raise FileSyncConflict(fn)
        else:
            fn_src = os.path.join(src.path, subdir, fn)
            fn_dst = os.path.join(dst.path, subdir, fn)
            if strategy(src, dst, os.path.join(subdir, fn)):
                copy(fn_src, fn_dst)
            else:
                logger.debug(f"Skip file '{fn}'.")
    for _subdir in diff.subdirs:
        if recursive:
            _sync_job_workspaces(
                src=src,
                dst=dst,
                strategy=strategy,
                exclude=exclude,
                copy=copy,
                copytree=copytree,
                recursive=recursive,
                deep=deep,
                subdir=os.path.join(subdir, _subdir),
            )
        else:
            logger.warning(f"Skip directory '{os.path.join(subdir, _subdir)}'.")


def _identical_path(a, b):
    """Verify if two absolute real paths match."""
    return os.path.abspath(os.path.realpath(a)) == os.path.abspath(os.path.realpath(b))


def sync_jobs(
    src,
    dst,
    strategy=None,
    exclude=None,
    doc_sync=None,
    recursive=False,
    follow_symlinks=True,
    preserve_permissions=False,
    preserve_times=False,
    preserve_owner=False,
    preserve_group=False,
    deep=False,
    dry_run=False,
):
    """Synchronize the dst job with the src job.

    By default, this method will synchronize all files and document data
    of dst job with the src job until a synchronization conflict occurs.
    There are two different kinds of synchronization conflicts:

        1. The two jobs have files with the same name, but different content.
        2. The two jobs have documents that share keys, but those keys are
           mapped to different values.

    A file conflict can be resolved by providing a 'FileSync' *strategy* or by
    *excluding* files from the synchronization. An unresolvable conflict is indicated
    with the raise of a :class:`~.errors.FileSyncConflict` exception.

    A document synchronization conflict can be resolved by providing a doc_sync function
    that takes the source and the destination document as first and second argument.

    Parameters
    ----------
    src : :class:`~signac.job.Job`
        The src job, data will be copied from this job's workspace.
    dst : :class:`~signac.job.Job`
        The dst job, data will be copied to this job's workspace.
    strategy : callable, optional
        A synchronization strategy for file conflicts. The strategy should be a
        callable with signature ``strategy(src, dst, filepath)`` where ``src``
        and ``dst`` are the source and destination instances of
        :py:class:`~signac.Project` and ``filepath`` is the filepath relative
        to the project path. If no strategy is provided, a
        :class:`.errors.SyncConflict` exception will be raised upon conflict.
        (Default value = None)
    exclude : str, optional
        A filename exclusion pattern. All files matching this pattern will be
        excluded from the synchronization process. (Default value = None)
    doc_sync : attribute or callable from :py:class:`~signac.sync.DocSync`, optional
        A synchronization strategy for document keys. The default is to use a
        safe key-by-key strategy that will not overwrite any values on
        conflict, but instead raises a :class:`~.errors.DocumentSyncConflict`
        exception.
    recursive : bool, optional
        Recursively synchronize sub-directories encountered within the job
        workspace directories. (Default value = False)
    follow_symlinks : bool, optional
        Follow and copy the target of symbolic links. (Default value = True)
    preserve_permissions : bool, optional
        Preserve file permissions (Default value = False)
    preserve_times : bool, optional
        Preserve file modification times (Default value = False)
    preserve_owner : bool, optional
        Preserve file owner (Default value = False)
    preserve_group : bool, optional
        Preserve file group ownership (Default value = False)
    dry_run : bool, optional
        If True, do not actually perform any synchronization operations.
        (Default value = False)
    deep : bool, optional
        (Default value = False)

    """
    # Check identity
    if _identical_path(src.path, dst.path):
        raise ValueError("Source and destination can't be the same!")

    # check src and dst compatiblity
    assert src.FN_STATE_POINT == dst.FN_STATE_POINT
    assert src.FN_DOCUMENT == dst.FN_DOCUMENT

    # Nothing to be done if the src is not initialized.
    if src not in src._project:
        return

    # The doc_sync functions defaults to a safe "by_key" strategy.
    if doc_sync is None:
        doc_sync = DocSync.ByKey()

    # the exclude argument must be a list
    if exclude is None:
        exclude = []
    elif not isinstance(exclude, list):
        exclude = [exclude]
    exclude.append(src.FN_STATE_POINT)
    if doc_sync != DocSync.COPY:
        exclude.append(src.FN_DOCUMENT)

    if type(dry_run) is _FileModifyProxy:
        proxy = dry_run
    else:
        proxy = _FileModifyProxy(
            root=src.path,
            follow_symlinks=follow_symlinks,
            permissions=preserve_permissions,
            times=preserve_times,
            owner=preserve_owner,
            group=preserve_group,
            dry_run=bool(dry_run),
        )
    if proxy.dry_run:
        logger.debug(f"Synchronizing job '{src}' (dry run)...")
    else:
        logger.debug(f"Synchronizing job '{src}'...")

    if os.path.isdir(src.path):
        if not dry_run:
            dst.init()
        _sync_job_workspaces(
            src=src,
            dst=dst,
            strategy=strategy,
            exclude=exclude,
            copy=proxy.copy,
            copytree=proxy.copytree,
            recursive=recursive,
            deep=deep,
        )

    if doc_sync not in (DocSync.NO_SYNC, DocSync.COPY):
        if src.document != dst.document:
            with proxy.create_doc_backup(dst.document) as dst_proxy:
                doc_sync(src.document, dst_proxy)


FileTransferStats = namedtuple("FileTransferStats", ["num_files", "volume"])


def sync_projects(
    source,
    destination,
    strategy=None,
    exclude=None,
    doc_sync=None,
    selection=None,
    check_schema=True,
    recursive=False,
    follow_symlinks=True,
    preserve_permissions=False,
    preserve_times=False,
    preserve_owner=False,
    preserve_group=False,
    deep=False,
    dry_run=False,
    parallel=False,
    collect_stats=False,
):
    """Synchronize the destination project with the source project.

    Try to clone all jobs from the source to the destination.
    If the destination job already exist, try to synchronize the job using the
    optionally specified strategy.

    Parameters
    ----------
    source : class:`~.Project`
        The project presenting the source for synchronization.
    destination : class:`~.Project`
        The project that is modified for synchronization.
    strategy : callable, optional
        A synchronization strategy for file conflicts. The strategy should be a
        callable with signature ``strategy(src, dst, filepath)`` where ``src``
        and ``dst`` are the source and destination instances of
        :py:class:`~signac.Project` and ``filepath`` is the filepath relative
        to the project path. If no strategy is provided, a
        :class:`.errors.SyncConflict` exception will be raised upon conflict.
        (Default value = None)
    exclude : str, optional
        A filename exclusion pattern. All files matching this pattern will be
        excluded from the synchronization process. (Default value = None)
    doc_sync : attribute or callable from :py:class:`~signac.sync.DocSync`
        A synchronization strategy for document keys. The default is to use a
        safe key-by-key strategy that will not overwrite any values on
        conflict, but instead raises a :class:`~.errors.DocumentSyncConflict`
        exception.
    selection : sequence of :class:`~signac.job.Job` or job ids (str), optional
        Only synchronize the given selection of jobs. (Default value = None)
    check_schema : bool, optional
        If True, only synchronize if this and the other project have a matching
        state point schema. See also: :meth:`~.detect_schema`. (Default value =
        True)
    recursive : bool, optional
        Recursively synchronize sub-directories encountered within the job
        workspace directories. (Default value = False)
    follow_symlinks : bool, optional
        Follow and copy the target of symbolic links. (Default value = True)
    preserve_permissions : bool, optional
        Preserve file permissions (Default value = False)
    preserve_times : bool, optional
        Preserve file modification times (Default value = False)
    preserve_owner : bool, optional
        Preserve file owner (Default value = False)
    preserve_group : bool, optional
        Preserve file group ownership (Default value = False)
    dry_run : bool, optional
        If True, do not actually perform the synchronization operation, just
        log what would happen theoretically. Useful to test synchronization
        strategies without the risk of data loss. (Default value = False)
    deep : bool, optional
        (Default value = False)
    parallel : bool, optional
        (Default value = False)
    collect_stats : bool, optional
        (Default value = False)

    Returns
    -------
    NoneType or :class:`~signac.sync.FileTransferStats`
        Returns stats if ``collect_stats`` is ``True``, else ``None``.

    Raises
    ------
    :class:`~signac.errors.DocumentSyncConflict`
        If there are conflicting keys within the project or job documents that
        cannot be resolved with the given strategy or if there is no strategy
        provided.
    :class:`~signac.errors.FileSyncConflict`
        If there are differing files that cannot be resolved with the given
        strategy or if no strategy is provided.
    :class:`~signac.errors.SchemaSyncConflict`
        In case that the check_schema argument is True and the detected state
        point schema of this and the other project differ.

    """
    if source == destination:
        raise ValueError("Source and destination project cannot be identical!")

    # Setup data modification proxy
    proxy = _FileModifyProxy(
        root=source.workspace,
        follow_symlinks=follow_symlinks,
        permissions=preserve_permissions,
        times=preserve_times,
        owner=preserve_owner,
        group=preserve_group,
        dry_run=dry_run,
        collect_stats=collect_stats,
    )

    # Perform a schema check in an attempt to avoid bad sync operations.
    if check_schema:
        schema_src = source.detect_schema()
        schema_dst = destination.detect_schema()
        if schema_dst and schema_src and schema_src != schema_dst:
            only_in_src = schema_src.difference(schema_dst)
            only_in_dst = schema_dst.difference(schema_src)
            if only_in_src or only_in_dst:
                raise SchemaSyncConflict(schema_src, schema_dst)

    if doc_sync is None:
        doc_sync = DocSync.ByKey()

    if (
        selection is not None
    ):  # The selection argument may be a jobs or job ids sequence.
        selection = {str(j) for j in selection}

    # Provide some information about this sync process.
    if selection:
        logger.info(
            "Synchronizing selection ({}) of project '{}' to '{}'.".format(
                len(selection), source, destination
            )
        )
    else:
        logger.info(f"Synchronizing project '{source}' to '{destination}'.")
    logger.more(f"'{source.path}' -> '{destination.path}'")
    if dry_run:
        logger.info("Performing dry run!")
    if exclude is not None:
        logger.more(f"File name exclude pattern: '{exclude}'")
    logger.more(f"Sync strategy: '{strategy}'")
    logger.more(f"Doc sync strategy: '{doc_sync}'")

    # Sync the Project document.
    if doc_sync not in (DocSync.NO_SYNC, DocSync.COPY):
        if source.document != destination.document:
            with proxy.create_doc_backup(destination.document) as dst_proxy:
                doc_sync(source.document, dst_proxy)

    # Sync jobs from source to destination.
    logger.more("Collect all jobs to synchronize...")
    if selection is None:
        jobs_to_sync = list(source)
    else:
        jobs_to_sync = [job for job in source if job.id in selection]

    N = len(jobs_to_sync)
    logger.more(f"Synchronizing {N} jobs.")
    count = ddict(int)

    def _clone_or_sync(src_job):
        """Clone a job if it does not exist, or sync if it exists."""
        try:
            destination.clone(src_job, copytree=proxy.copytree)
            logger.more(f"Cloned job '{src_job}'.")
            return 1
        except DestinationExistsError:
            dst_job = destination.open_job(id=src_job.id)
            sync_jobs(
                src=src_job,
                dst=dst_job,
                strategy=strategy,
                exclude=exclude,
                doc_sync=doc_sync,
                recursive=recursive,
                dry_run=proxy,  # used as internal argument to forward the proxy
            )
            logger.more(f"Synchronized job '{src_job}'.")
            return 2

    if parallel:
        num_processes = None if parallel is True else parallel
        logger.more(
            "Parallelizing over {} threads for synchronization.".format(
                "multiple" if num_processes is None else num_processes
            )
        )
        with ThreadPool(None if parallel is True else parallel) as pool:
            for i, ret in enumerate(pool.imap(_clone_or_sync, jobs_to_sync)):
                count[ret] += 1
                logger.info(f"Project sync progress: {i + 1}/{N}")
    else:
        for i, src_job in enumerate(jobs_to_sync):
            count[_clone_or_sync(src_job)] += 1
            logger.info(f"Project sync progress: {i + 1}/{N}")

    num_cloned, num_synchronized = count[1], count[2]
    logger.info(f"Cloned {num_cloned} and synchronized {num_synchronized} job(s).")
    if collect_stats:
        return FileTransferStats(**proxy.stats)
