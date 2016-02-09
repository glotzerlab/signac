"""General description of the filsystems module."""
import os
import errno
import io
import warnings

from ..common import six
from ..db import get_database
from .hashing import calc_id

try:
    import gridfs
except ImportError:
    GRIDFS = False
else:
    GRIDFS = True
if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping

GRIDFS_LARGE_FILE_WARNING_THRSHLD = 1e9  # 1GB


class LocalFS(object):
    """A file system handler for the local file system.

    This handler wraps all operations required to create
    new and get files on a local file system.
    This allows the storage and retrieval of files in a
    heterogeneous environment.
    """
    name = 'localfs'
    "General identifier for this file system handler."

    FileExistsError = IOError
    FileNotFoundError = IOError

    def __init__(self, root):
        self.root = root

    def config(self):
        return {'root': self.root}

    def __repr__(self):
        return '{}({})'.format(
            type(self),
            ', '.join('{}={}'.format(k, v) for k, v in self.config().items()))

    @classmethod
    def from_config(cls, config):
        return LocalFS(root=config['root'])

    def _fn(self, file_id, n=2, suffix='.dat'):
        fn = os.path.join(
            self.root,
            * [file_id[i:i + n] for i in range(0, len(file_id), n)]) + suffix
        return fn

    def new_file(self, **kwargs):
        assert 'x' in kwargs.get('mode', 'x')
        file_id = kwargs.get('_id', calc_id(kwargs))
        fn = self._fn(file_id)
        try:
            path = os.path.dirname(fn)
            os.makedirs(path)
        except OSError as error:
            if not (error.errno == errno.EEXIST and os.path.isdir(path)):
                raise
        return open(fn, mode='wxb' if six.PY2 else 'xb')

    def get(self, file_id, mode='r'):
        assert 'r' in mode
        return open(self._fn(file_id), mode=mode)


class GridFS(object):
    """A file system handler for the MongoDB GridFS file system."""
    name = 'gridfs'

    FileExistsError = gridfs.errors.FileExists
    FileNotFoundError = gridfs.errors.NoFile

    def __init__(self, db, collection='fs'):
        if isinstance(db, str):
            self.db = None
            self.db_name = db
        else:
            self.db = db
            self.db_name = db.name
        self.collection = collection
        self._gridfs = None

    def config(self):
        return {'db': self.db_name, 'collection': self.collection}

    def __repr__(self):
        return '{}({})'.format(
            type(self),
            ', '.join('{}={}'.format(k, v) for k, v in self.config().items()))

    @classmethod
    def from_config(cls, config):
        return GridFS(
            db=config['db'],
            collection=config.get('collection', 'fs'))

    @property
    def gridfs(self):
        if self._gridfs is None:
            if self.db is None:
                self.db = get_database(self.db_name)
            self._gridfs = gridfs.GridFS(self.db, collection=self.collection)
        return self._gridfs

    def new_file(self, **kwargs):
        return self.gridfs.new_file(** kwargs)

    def get(self, file_id, mode='r'):
        if mode == 'r':
            file = io.StringIO(self.gridfs.get(file_id).read().decode())
            if len(file.getvalue()) > GRIDFS_LARGE_FILE_WARNING_THRSHLD:
                warnings.warn(
                    "Open large GridFS files more efficiently in 'rb' mode.")
            return file
        elif mode == 'rb':
            return self.gridfs.get(file_id=file_id)
        else:
            raise ValueError(mode)


def filesystems_from_config(fs_config):
    for item in fs_config:
        if isinstance(item, Mapping):
            for key in item:
                if key == 'localfs':
                    if isinstance(item[key], Mapping):
                        yield LocalFS.from_config(item[key])
                    else:
                        yield LocalFS(item[key])
                elif key == 'gridfs':
                    if GRIDFS:
                        if isinstance(item[key], Mapping):
                            yield GridFS.from_config(item[key])
                        else:
                            yield GridFS(item[key])
                    else:
                        warnings.warn("gridfs not available!")
                else:
                    warnings.warn("Unknown filesystem type '{}'.".format(key))
        else:
            yield item
