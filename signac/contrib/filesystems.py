# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The file system handlers defined in this module
encapsulate the I/O operations required to store
and fetch data from different file systems."""
import os
import errno
import io
import warnings

from ..common import six
from ..db import get_database

try:
    import gridfs
except ImportError:
    GRIDFS = False
else:
    GRIDFS = True
if six.PY2:
    from collections import Mapping, Iterable
else:
    from collections.abc import Mapping, Iterable

GRIDFS_LARGE_FILE_WARNING_THRSHLD = int(1e9)  # 1GB
FILESYSTEM_REGISTRY = dict()


def _register_fs_class(fs):
    "Register a file system handler in the module's registry."
    FILESYSTEM_REGISTRY[fs.name] = fs


class LocalFS(object):
    """A file system handler for the local file system.

    This handler will store all files at the specified
    root path using a file id based naming scheme.

    :param root: The path to the root directory.
    :type root: str
    """
    name = 'localfs'
    "General identifier for this file system handler."

    FileExistsError = IOError
    "A file with the specified id already exists."
    FileNotFoundError = IOError
    "A file with the specified id is not found."

    def __init__(self, root):
        self.root = root

    def config(self):
        "Return the file system configuration for this handler."
        return {'root': self.root}

    def __repr__(self):
        return '{}({})'.format(
            type(self),
            ', '.join('{}={}'.format(k, v) for k, v in self.config().items()))

    def _fn(self, _id, n=2, suffix='.dat'):
        fn = os.path.join(
            self.root,
            * [_id[i:i + n] for i in range(0, len(_id), n)]) + suffix
        return fn

    def new_file(self, _id, mode=None):
        """Create a new file for _id.

        :param _id: The file identifier.
        :type _id: str
        :returns: A file-like object to write to."""
        if mode is None:
            mode = 'wxb' if six.PY2 else 'xb'
        if 'x' not in mode:
            raise ValueError(mode)
        fn = self._fn(_id)
        try:
            path = os.path.dirname(fn)
            os.makedirs(path)
        except OSError as error:
            if not (error.errno == errno.EEXIST and os.path.isdir(path)):
                raise
        return open(fn, mode=mode)

    def get(self, _id, mode='r'):
        """Open the file with the specified id.

        :param _id: The file identifier.
        :type _id: str
        :param mode: The file mode used for opening.
        :returns: A file-like object to read from."""

        if 'r' not in mode:
            raise ValueError(mode)
        return open(self._fn(_id), mode=mode)
_register_fs_class(LocalFS)

if GRIDFS:

    class GridFS(object):
        """A file system handler for the MongoDB `GridFS`_ file system.

        .. note::

            If the `database` argument is a :class:`str`, signac will
            attempt to connect to the database using the
            global configuration.

        .. _`GridFS`: http://api.mongodb.org/python/current/api/gridfs/

        :param db: The database used to store the grid.
        :type db: str or :class:`pymongo.database.Database`
        """
        name = 'gridfs'
        "General identifier for this file system handler."

        FileExistsError = gridfs.errors.FileExists
        "A file with the specified id already exists."
        FileNotFoundError = gridfs.errors.NoFile
        "A file with the specified id is not found."

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
            "Return the file system configuration for this handler."
            return {'db': self.db_name, 'collection': self.collection}

        def __repr__(self):
            return '{}({})'.format(
                type(self),
                ', '.join('{}={}'.format(k, v) for k, v in self.config().items()))

        @property
        def gridfs(self):
            "Instance of :class:`pymongo.gridfs.GridFS`."
            if self._gridfs is None:
                if self.db is None:
                    self.db = get_database(self.db_name)
                self._gridfs = gridfs.GridFS(
                    self.db, collection=self.collection)
            return self._gridfs

        def new_file(self, _id):
            """Create a new file for _id.

            :param _id: The file identifier.
            :type _id: str
            :returns: A file-like object to write to."""
            return self.gridfs.new_file(_id=_id)

        def get(self, _id, mode='r'):
            """Open the file with the specified id.

            .. warning::

                To avoid compatiblity issues, all files are
                opened in text-mode (`r`) by default, however
                for higher efficiency, files should generally
                be opened in binary mode (`rb`) whenever possible.

            :param _id: The file identifier.
            :type _id: str
            :param mode: The file mode used for opening.
            :returns: A file-like object to read from."""
            if mode == 'r':
                file = io.StringIO(self.gridfs.get(_id).read().decode())
                if len(file.getvalue()) > GRIDFS_LARGE_FILE_WARNING_THRSHLD:
                    warnings.warn(
                        "Open large GridFS files more efficiently in 'rb' mode.")
                return file
            elif mode == 'rb':
                return self.gridfs.get(_id=_id)
            else:
                raise ValueError(mode)
    _register_fs_class(GridFS)


def filesystems_from_config(fs_config):
    """Generate file system handlers from a configuration.

    This function yields file system handler objects from
    a file system configuration.
    A configuration is a mapping where the key identifies the
    type of file system, and the values represent the argument(s)
    to the constructor of the specified file system handler.
    Arguments can be provided as mappings, sequences or single values, e.g.:

    .. code::

        # The following two function calls are equivalent and both
        # generate two file system handler objects:
        filesystems_from_config({
            'localfs': '/path/to/storage',
            'gridfs': ('gridfsdb', 'fs'),
            })

        filesystems_from_config({
            'localfs': {'root': '/path/to/storage'},
            'gridfs': {'db': 'gridfsdb', 'collection': 'fs'}
            })

    See :class:`~.LocalFS` for an example of a file system class.

    :param fs_config: A file system configuration.
    :yields: file system handlers
    """
    for key, args in fs_config.items():
        fs_class = FILESYSTEM_REGISTRY[key]
        if six.PY2:
            is_str = isinstance(args, str) or isinstance(args, unicode)  # noqa
        else:
            is_str = isinstance(args, str)
        if isinstance(args, Mapping):
            yield fs_class(** args)
        elif isinstance(args, Iterable) and not is_str:
            yield fs_class(* args)
        else:
            yield fs_class(args)


def filesystems_from_configs(fs_configs):
    """Generate file system handlers.

    The ``fs_configs`` argument may be a sequence of file system
    handlers, file system configurations or a mix of both.

    See also: :func:`.filesystems_from_config`.

    :param fs_configs: A sequence of file system handlers or
        configurations.
    :yields: file system handlers
    """
    for item in fs_configs:
        if isinstance(item, Mapping):
            for fs in filesystems_from_config(item):
                yield fs
        else:
            yield item
