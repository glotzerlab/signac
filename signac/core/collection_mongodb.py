# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements MongoDB-backend.

This implements the MongoDB-backend for SyncedCollection API by
implementing sync and load methods.
"""
from copy import deepcopy

from .synced_collection import SyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList


class MongoDBCollection(SyncedCollection):
    """Implement sync and load using a MongoDB backend."""

    backend = __name__  # type: ignore

    def __init__(self, collection=None, **kwargs):
        import bson  # for InvalidDocument

        self._collection = collection
        self._errors = bson.errors
        self._key = type(self).__name__ + '::name'
        super().__init__(**kwargs)

    def _load(self):
        """Load the data from a Mongo-database."""
        blob = self._collection.find_one({self._key: self._name})
        return blob['data'] if blob is not None else None

    def _sync(self):
        """Write the data from Mongo-database."""
        data = self.to_base()
        data_to_insert = {self._key: self._name, 'data': data}
        try:
            self._collection.replace_one({self._key: self._name}, data_to_insert, True)
        except self._errors.InvalidDocument as err:
            raise TypeError(str(err))

    def __deepcopy__(self, memo):
        return type(self)(collection=self._collection, name=self._name, data=self.to_base(),
                          parent=deepcopy(self._parent, memo))


class MongoDict(MongoDBCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent Mongo-database.

    The MongoDict inherits from :class:`~core.collection_api.MongoCollection`
    and :class:`~core.syncedattrdict.SyncedAttrDict`.

    .. code-block:: python

        doc = MongoDict('data')
        doc['foo'] = "bar"
        assert doc.foo == doc['foo'] == "bar"
        assert 'foo' in doc
        del doc['foo']

    .. code-block:: python

        >>> doc['foo'] = dict(bar=True)
        >>> doc
        {'foo': {'bar': True}}
        >>> doc.foo.bar = False
        {'foo': {'bar': False}}

    .. warning::

        While the MongoDict object behaves like a dictionary, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a MongoDict instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new MongoDict
        instance: `new_dict = MongoDict(old_dict.to_base())`.

    Parameters
    ----------
    name: str
        The name of the  collection (Default value = None).
    collection : object
        A pymongo.Collection instance
    data: mapping, optional
        The intial data pass to MongoDict. Defaults to `dict()`
    parent: object, optional
        A parent instance of MongoDict or None (Default value = None).
    """


class MongoList(MongoDBCollection, SyncedList):
    """A non-string sequence interface to a persistent Mongo file.

    The MongoDict inherits from :class:`~core.synced_collection.SyncedCollection`
    and :class:`~core.syncedlist.SyncedList`.

    .. code-block:: python

        synced_list = MongoList('data')
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    .. warning::

        While the MongoList object behaves like a list, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a MongoList instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new MongoList
        instance: `new_list = MongoList(old_list.to_base())`.

    Parameters
    ----------
    name: str
        The name of the  collection (Default value = None).
    collection : object
        A pymongo.Collection instance
    data: mapping, optional
        The intial data pass to MongoList. Defaults to `list()`
    parent: object, optional
        A parent instance of MongoList or None (Default value = None).
    """


SyncedCollection.register(MongoDict, MongoList)
