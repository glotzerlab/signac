# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Dict implementation with pymongo backend."""

from .attrdict import SyncedAttrDict
from collections.abc import Mapping
from copy import copy


class PyMongoDict(SyncedAttrDict):
    """A dict-like mapping interface to pymongo document.

    .. code-block:: python

        db = get_database('test', 'myhost')
        doc = PyMongoDict(db.my_collection, jobid)
        doc['foo'] = "bar"
        assert doc.foo == doc['foo'] == "bar"
        assert 'foo' in doc
        del doc['foo']

    This class allows access to values through key indexing or attributes
    named by keys, including nested keys:

    .. code-block:: python

        >>> doc['foo'] = dict(bar=True)
        >>> doc
        {'foo': {'bar': True}}
        >>> doc.foo.bar = False
        {'foo': {'bar': False}}

    :param collection:
        A handle to a :class:py:`pymongo.collection.Collection` object.
    :param jobid:
        A unique identifier for the pymongo document containing the job doc
    :param parent:
        A parent instance of PyMongoDic or None.
    """

    def __init__(self, collection=None, jobid=None, parent=None):
        if (collection is None or jobid is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of "
                "parent or collection/jobid must be None, but not both.")
        self._collection = collection
        self._jobid = jobid
        super(PyMongoDict, self).__init__(parent=parent)

    def reset(self, data):
        """Replace the document contents with data."""
        if isinstance(data, Mapping):
            with self._suspend_sync():
                backup = copy(self._data)
                try:
                    self._data = {
                        self._validate_key(k): self._dfs_convert(v)
                        for k, v in data.items()
                    }
                    self._save()
                except BaseException:  # rollback
                    self._data = backup
                    raise
        else:
            raise ValueError("The document must be a mapping.")

    def _load(self):
        assert self._collection is not None
        assert self._jobid is not None
        pymongo_doc = self._collection.find_one({'_id': self._jobid})
        if 'doc' in pymongo_doc:
            return pymongo_doc['doc']
        else:
            return dict()

    def _save(self):
        assert self._collection is not None
        assert self._jobid is not None

        data = self._data
        if data is None:
            data = self._as_dict()

        self._collection.update_one({'_id': self._jobid}, {'$set': {'doc': data}})
