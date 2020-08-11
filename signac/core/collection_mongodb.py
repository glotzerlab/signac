# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements MongoDB-backend.

This implements the MongoDB-backend for SyncedCollection API by
implementing sync and load methods.
"""
from .synced_collection import SyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList


class MongoDBCollection(SyncedCollection):
    """Implement sync and load using a MongoDB backend."""

    backend = __name__  # type: ignore

    def __init__(self, collection, name=None, **kwargs):
        self._collection = collection
        self._name = name
        self._key = type(self).__name__ + '::name'
        super().__init__(**kwargs)
        if (name is None) == (self._parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or name must be None, but not both.")

    def _load(self):
        """Load the data from a Mongo-database."""
        blob = self._collection.find_one({self._key: self._name})
        return blob['data'] if blob is not None else None

    def _sync(self):
        """Write the data from Mongo-database."""
        data = self.to_base()
        data_to_insert = {self._key: self._name, 'data': data}
        self._collection.replace_one({self._key: self._name}, data_to_insert, True)


class MongoDict(MongoCollection, SyncedAttrDict):
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

    pass


class MongoList(MongoCollection, SyncedList):
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

    pass


SyncedCollection.register(MongoDict, MongoList)
