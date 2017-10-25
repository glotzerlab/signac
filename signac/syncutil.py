import os
import shutil
import filecmp
import logging
from copy import deepcopy
from contextlib import contextmanager
from filecmp import dircmp


LEVEL_MORE = logging.INFO - 5

logger = logging.getLogger('sync')
logging.addLevelName(LEVEL_MORE, 'MORE')
logging.MORE = LEVEL_MORE


def log_more(msg, *args, **kwargs):
    logger.log(LEVEL_MORE, msg, *args, **kwargs)


logger.more = log_more


def copytree(src, dst, copy_function=shutil.copy2, symlinks=False):
    "Implementation adapted from https://docs.python.org/3/library/shutil.html#copytree-example'."
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
            raise
            errors.append((srcname, dstname, str(why)))
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except shutil.Error as err:
            raise
            errors.extend(err.args[0])
    if errors:
        raise shutil.Error(errors)


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

    def __init__(self, root=None, follow_symlinks=True, permissions=False,
                 times=False, owner=False, group=False, dry_run=False,
                 collect_stats=False):
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
        if not self.dry_run:
            shutil.copy(src, dst)

    def _copy_p(self, src, dst):
        if not self.dry_run:
            shutil.copy(src, dst)
            shutil.copymode(src, dst)

    def _copy2(self, src, dst):
        if not self.dry_run:
            shutil.copy2(src, dst)

    def _remove(self, path):
        if not self.dry_run:
            os.remove(path)

    # Public functions

    def remove(self, path):
        logger.more("Remove path '{}'.".format(os.path.relpath(path)))
        self._remove(path)

    def copy(self, src, dst):
        if self.dry_run and self.root is not None:
            print(os.path.relpath(src, self.root))
        if os.path.islink(src) and not self.follow_symlinks:
            link_target = os.readlink(src)
            logger.more("Creating link '{}' -> '{}'.".format(
                os.path.relpath(dst), os.path.relpath(link_target)))
            if os.path.isfile(dst):
                self.remove(dst)
            if not self.dry_run:
                os.symlink(link_target, dst)
        else:
            msg = "Copy file{{}} '{}' -> '{}'.".format(os.path.relpath(src), os.path.relpath(dst))
            if self.permissions and self.times:
                logger.more(msg.format(' (preserving: permissions, times)'))
                self._copy2(src, dst)
            elif self.permissions:
                logger.more(msg.format(' (preserving: permissions)'))
                self._copy_p(src, dst)
            elif self.times:
                raise ValueError("Cannot copy timestamps without permissions.")
            else:
                logger.more(msg.format(''))
                self._copy(src, dst)
            if self.owner or self.group or self.stats is not None:
                stat = os.stat(src)
                if self.stats is not None:
                    self.stats['num_files'] += 1
                    self.stats['volume'] += stat.st_size
                if self.owner or self.group:
                    logger.more("Copy owner/group '{}' -> '{}'".format(
                        os.path.relpath(src), os.path.relpath(dst)))
                    if not self.dry_run:
                        os.chown(dst,
                                 uid=stat.st_uid if self.owner else -1,
                                 gid=stat.st_gid if self.group else -1)

    def copytree(self, src, dst, **kwargs):
        logger.more("Copy tree '{}' -> '{}'.".format(os.path.relpath(src), os.path.relpath(dst)))
        copytree(src, dst, copy_function=self.copy, **kwargs)

    @contextmanager
    def create_backup(self, path):
        logger.debug("Create backup of '{}'.".format(os.path.relpath(path)))
        path_backup = path + '~'
        if os.path.isfile(path_backup):
            raise RuntimeError(
                "Failed to create backup, file already exists: '{}'.".format(
                    os.path.relpath(path_backup)))
        try:
            self._copy2(path, path_backup)
            yield path_backup
        except:     # noqa roll-back
            logger.more("Error occured, restoring backup...")
            self._copy2(path_backup, path)
            raise
        finally:
            logger.debug("Remove backup of '{}'.".format(os.path.relpath(path)))
            self._remove(path_backup)

    @contextmanager
    def create_doc_backup(self, doc):
        proxy = _DocProxy(doc, dry_run=self.dry_run)
        fn = getattr(doc, 'filename', getattr(doc, '_filename', None))
        if not len(proxy) or fn is None or not os.path.isfile(fn):
            backup = deepcopy(doc)  # use in-memory backup
            try:
                yield proxy
            except:     # noqa roll-back
                proxy.clear()
                proxy.update(backup)
                raise
        else:
            with self.create_backup(fn):
                yield proxy
