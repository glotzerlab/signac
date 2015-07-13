import logging
logger = logging.getLogger(__name__)

class ReadOnlyStorage(object):
    
    def __init__(self, fs_path, wd_path, allow_move = False):
        self._wd_path = self._norm_path(wd_path)
        self._fs_path = self._norm_path(fs_path)
        self._allow_move = allow_move
        msg = "Opened storage at '{}' to '{}'."
        logger.debug(msg.format(self._wd_path, self._fs_path))

    def open_file(self, filename, * args, ** kwargs):
        return open(self._fn_fs(filename), 'rb', * args, ** kwargs)

    def _norm_path(self, path):
        import os
        p = os.path.abspath(path)
        if not os.path.isdir(p):
            raise NotADirectoryError(p)
        return p

    def _fn_fs(self, filename):
        from os.path import join
        return join(self._fs_path, filename)

    def _fn_wd(self, filename):
        from os.path import join
        return join(self._wd_path, filename)

    def list_files(self):   
        from os import listdir
        return listdir(self._fs_path)

    def download_file(self, filename, overwrite = False):
        from shutil import copy2 as copy
        if not overwrite:
            import os
            if os.path.isfile(filename):
                msg = "File '{}' already exists. Use overwrite=True to ignore."
                raise FileExistsError(msg.format(filename))
        copy(self._fn_fs(filename), filename)
    
class Storage(ReadOnlyStorage):

    def open_file(self, filename, * args, ** kwargs):
        return open(self._fn_fs(filename), * args, ** kwargs)

    def remove_file(self, filename):
        import os
        os.remove(self._fn_fs(filename))

    def clear(self):
        import os
        self.remove()
        try:
            os.mkdir(self._fs_path)
        except FileExistsError:
            pass

    def remove(self):
        from shutil import rmtree
        try:
            rmtree(self._fs_path)
        except FileNotFoundError as error:
            pass

    def _move_file(self, src, dst):
        from shutil import move
        from os import rename
        msg = "Moving from '{}' to '{}'."
        logger.debug(msg.format(src, dst))
        if self._allow_move:
            move(src, dst)
        else:
            rename(src, dst)

    def store_file(self, filename):
        from shutil import move
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

    def store_files(self, pathname = '*', * args):
        import glob
        from itertools import chain
        from os.path import join, split
        for pattern in chain([pathname], args):
            p = join(self._wd_path, pattern)
            logger.debug("Pattern '{}'.".format(p))
            for fn in glob.glob(p):
                base = split(fn)[1]
                self.store_file(base)

    def restore_files(self, pathname = '*', * args):
        import glob
        from itertools import chain
        from os.path import join, split
        for pattern in chain([pathname], args):
            p = join(self._fs_path, pattern)
            logger.debug("Pattern '{}'.".format(p))
            for fn in glob.glob(p):
                base = split(fn)[1]
                self.restore_file(base)
                #self.restore_file(fn)

    def fetch_file(self, storage, fn):
        from shutil import copy2 as copy
        copy(storage._fn_fs(fn), self._fn_fs(fn))
