# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements MongoDB-backend.

This implements the MongoDB-backend for SyncedCollection API by
implementing sync and load methods.
"""
import bson
from copy import deepcopy

from .synced_collection import SyncedCollection
from .synced_attr_dict import SyncedAttrDict
from .synced_list import SyncedList


class MongoDBCollection(SyncedCollection):
    """Implement sync and load using a MongoDB backend."""

    _backend = __name__  # type: ignore

    # The key used to find a collection's document in the database.
    _key = 'MongoDBCollection::name'

    def __init__(self, collection=None, name=None, parent=None, **kwargs):

        self._collection = collection
        self._name = name
        super().__init__(parent=parent, **kwargs)

    def _load_from_resource(self):
        """Load the data from a MongoDB."""
        blob = self._collection.find_one({self._key: self._name})
        return blob['data'] if blob is not None else None

    def _save_to_resource(self):
        """Write the data from MongoDB."""
        data = self.to_base()
        data_to_insert = {self._key: self._name, 'data': data}
        try:
            self._collection.replace_one({self._key: self._name}, data_to_insert, True)
        except bson.errors.InvalidDocument as err:
            raise TypeError(str(err))

    def _pseudo_deepcopy(self):
        """Return a copy of instance.

        It is a psuedo implementation for `deepcopy` because
        `pymongo.Collection` does not support `deepcopy` method.
        """
        return type(self)(collection=self._collection, name=self._name, data=self.to_base(),
                          parent=deepcopy(self._parent))

    @property
    def collection(self):
        """`pymongo.collection.Collection`: The collection being synced to."""
        return self._collection

    @property
    def name(self):
        """str: The name associated with this collection."""
        return self._name


class MongoDBDict(MongoDBCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent Mongo-database.

    The MongoDBDict inherits from :class:`~core.synced_collection.MongoCollection`
    and :class:`~core.syncedattrdict.SyncedAttrDict`.

    .. code-block:: python

        doc = MongoDBDict('data')
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

        While the MongoDBDict object behaves like a dictionary, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a MongoDBDict instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new MongoDBDict
        instance: `new_dict = MongoDBDict(old_dict.to_base())`.

    Parameters
    ----------
    collection : object, optional
        A pymongo.Collection instance.
    data: mapping, optional
        The intial data pass to MongoDBDict. Defaults to `dict()`.
    name: str, optional
        The name of the  collection (Default value = None).
    parent: object, optional
        A parent instance of MongoDBDict (Default value = None).
    """
    def __init__(self, collection=None, name=None, data=None, parent=None, *args, **kwargs):
        self._validate_constructor_args({'collection': collection, 'name': name}, data, parent)
        super().__init__(collection=collection, name=name, data=data,
                         parent=parent, *args, **kwargs)


class MongoDBList(MongoDBCollection, SyncedList):
    """A non-string sequence interface to a persistent Mongo file.

    The MongoDBList inherits from :class:`~core.synced_collection.SyncedCollection`
    and :class:`~core.syncedlist.SyncedList`.

    .. code-block:: python

        synced_list = MongoDBList('data')
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    .. warning::

        While the MongoDBList object behaves like a list, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a MongoDBList instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new MongoDBList
        instance: `new_list = MongoDBList(old_list.to_base())`.

    Parameters
    ----------
    collection : object, optional
        A pymongo.Collection instance (Default value = None).
    data: non-str Sequence, optional
        The intial data pass to MongoDBList. Defaults to `list()`.
    name: str, optional
        The name of the  collection (Default value = None).
    parent: object, optional
        A parent instance of MongoDBList (Default value = None).
    """
    def __init__(self, collection=None, name=None, data=None, parent=None, *args, **kwargs):
        self._validate_constructor_args({'collection': collection, 'name': name}, data, parent)
        super().__init__(collection=collection, name=name, data=data,
                         parent=parent, *args, **kwargs)
