import logging
import glob
import os
import itertools
import shutil
from shutil import copy2 as copy

logger = logging.getLogger(__name__)


class ReadOnlyStorage(object):

    def __init__(self, fs_path, wd_path, allow_move=False):
        self._wd_path = self._norm_path(wd_path)
        self._fs_path = self._norm_path(fs_path)
        self._allow_move = allow_move
        msg = "Opened storage at '{}' to '{}'."
        logger.debug(msg.format(self._wd_path, self._fs_path))

    def open_file(self, filename, * args, ** kwargs):
        return open(self._fn_fs(filename), 'rb', * args, ** kwargs)

    def _norm_path(self, path):
        p = os.path.abspath(path)
        if not os.path.isdir(p):
            raise NotADirectoryError(p)
        return p

    def _fn_fs(self, filename):
        return os.path.join(self._fs_path, filename)

    def _fn_wd(self, filename):
        return os.path.join(self._wd_path, filename)

    def list_files(self):
        return os.listdir(self._fs_path)

    def download_file(self, filename, overwrite=False):
        if not overwrite:
            if os.path.isfile(filename):
                msg = "File '{}' already exists. Use overwrite=True to ignore."
                raise FileExistsError(msg.format(filename))
        copy(self._fn_fs(filename), filename)


class Storage(ReadOnlyStorage):

    def open_file(self, filename, * args, ** kwargs):
        return open(self._fn_fs(filename), * args, ** kwargs)

    def remove_file(self, filename):
        os.remove(self._fn_fs(filename))

    def clear(self):
        self.remove()
        try:
            os.mkdir(self._fs_path)
        except FileExistsError:
            pass

    def remove(self):
        try:
            shutil.rmtree(self._fs_path)
        except FileNotFoundError:
            pass

    def _move_file(self, src, dst):
        msg = "Moving from '{}' to '{}'."
        logger.debug(msg.format(src, dst))
        if self._allow_move:
            shutil.move(src, dst)
        else:
            os.rename(src, dst)

    def store_file(self, filename):
        try:
            src = self._fn_wd(filename)
            dst = self._fn_fs(filename)
            self._move_file(src, dst)
        except FileNotFoundError as error:
            raise FileNotFoundError(filename) from error

    def restore_file(self, filename):
        try:
            src = self._fn_fs(filename)
            dst = self._fn_wd(filename)
            self._move_file(src, dst)
        except FileNotFoundError as error:
            raise FileNotFoundError(filename) from error

    def store_files(self, pathname='*', * args):
        for pattern in itertools.chain([pathname], args):
            p = os.path.join(self._wd_path, pattern)
            logger.debug("Pattern '{}'.".format(p))
            for fn in glob.glob(p):
                base = os.path.split(fn)[1]
                self.store_file(base)

    def restore_files(self, pathname='*', * args):
        for pattern in itertools.chain([pathname], args):
            p = os.path.join(self._fs_path, pattern)
            logger.debug("Pattern '{}'.".format(p))
            for fn in glob.glob(p):
                base = os.path.split(fn)[1]
                self.restore_file(base)
                # self.restore_file(fn)

    def fetch_file(self, storage, fn):
        copy(storage._fn_fs(fn), self._fn_fs(fn))
