# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a MongoDB SyncedCollection backend."""
from .synced_attr_dict import SyncedAttrDict
from .synced_collection import SyncedCollection
from .synced_list import SyncedList

try:
    import bson

    MONGO = True
except ImportError:
    MONGO = False


class MongoDBCollection(SyncedCollection):
    """A :class:`SyncedCollection` that synchronizes with a MongoDB document.

    MongoDB stores documents in collections, where each document is a single
    record encoded in a JSON-like format (BSON). Each :class:`SyncedCollection` can be
    represented as a MongoDB document, so this backend stores the :class:`SyncedCollection`
    as a single document within the collection provided by the user. The document
    is identified by a unique key provided by the user.

    **Thread safety**

    The MongoDBCollection is not thread-safe.

    Parameters
    ----------
    collection : :py:class:`pymongo.collection.Collection`
        The MongoDB client in which to store data.
    uid : dict
        The unique key-value mapping added to the data and stored in the document
        so that it is uniquely identifiable in the MongoDB collection.

    Warnings
    --------
    The user is responsible for providing a unique id such that there are no
    possible collisions between different :class:`SyncedCollection` instances
    stored in the same MongoDB collection. Failure to do so may result data
    corruption if multiple documents are found to be apparently associated with
    a given :class:`SyncedCollection`.

    """

    _backend = __name__  # type: ignore

    def __init__(self, collection=None, uid=None, parent=None, **kwargs):
        if not MONGO:
            raise RuntimeError(
                "The PyMongo package must be installed to use the MongoDBCollection."
            )

        self._collection = collection
        self._uid = uid
        super().__init__(parent=parent, **kwargs)

    def _load_from_resource(self):
        """Load the data from a MongoDB document.

        Returns
        -------
        Collection or None
            An equivalent unsynced collection satisfying :meth:`is_base_type` that
            contains the data in the MongoDB database. Will return None if no data
            was found in the database.

        """
        blob = self._collection.find_one(self._uid)
        return blob["data"] if blob is not None else None

    def _save_to_resource(self):
        """Write the data to a MongoDB document."""
        data = self._to_base()
        data_to_insert = {**self._uid, "data": data}
        try:
            self._collection.replace_one(self._uid, data_to_insert, True)
        except bson.errors.InvalidDocument as err:
            raise TypeError(str(err))

    @property
    def collection(self):
        """pymongo.collection.Collection: Get the collection being synced to."""
        return self._collection

    @property
    def uid(self):  # noqa: D401
        """dict: Get the unique mapping used to identify this collection."""
        return self._uid

    def __deepcopy__(self, memo):
        # The underlying MongoDB collection cannot be deepcopied.
        raise TypeError("MongoDBCollection does not support deepcopying.")


class MongoDBDict(MongoDBCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent document in a MongoDB collection.

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

    Warnings
    --------

    While the MongoDBDict object behaves like a dictionary, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying database, copying a MongoDBDict instance may
    exhibit unexpected behavior. If a true copy is required, you should use the
    call operator to get a dictionary representation, and if necessary
    construct a new MongoDBDict instance:
    ``new_dict = MongoDBDict(old_dict())``.

    Parameters
    ----------
    collection : pymongo.collection.Collection, optional
        A :class:`pymongo.collection.Collection` instance (Default value = None).
    uid: dict, optional
        The unique key-value mapping identifying the collection (Default value = None).
    data: non-str :py:class:`collections.abc.Mapping`, optional
        The intial data pass to MongoDBDict. Defaults to `dict()`.
    parent: MongoDBCollection, optional
        A parent instance of MongoDBCollection (Default value = None).

    """

    def __init__(
        self, collection=None, uid=None, data=None, parent=None, *args, **kwargs
    ):
        super().__init__(
            collection=collection, uid=uid, data=data, parent=parent, *args, **kwargs
        )


class MongoDBList(MongoDBCollection, SyncedList):
    """A non-string sequence interface to a document in a MongoDB collection.

    .. code-block:: python

        synced_list = MongoDBList('data')
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    Parameters
    ----------
    collection : pymongo.collection.Collection, optional
        A :class:`pymongo.collection.Collection` instance (Default value = None).
    uid: dict, optional
        The unique key-value mapping identifying the collection (Default value = None).
    data: non-str :py:class:`collections.abc.Sequence`, optional
        The intial data pass to MongoDBList. Defaults to `list()`.
    parent: MongoDBCollection, optional
        A parent instance of MongoDBCollection (Default value = None).

    Warnings
    --------

    While the MongoDBList object behaves like a list, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying database, copying a MongoDBList instance may
    exhibit unexpected behavior. If a true copy is required, you should use the
    call operator to get a dictionary representation, and if necessary
    construct a new MongoDBList instance:
    ``new_list = MongoDBList(old_list())``.

    """

    def __init__(
        self, collection=None, uid=None, data=None, parent=None, *args, **kwargs
    ):
        super().__init__(
            collection=collection, uid=uid, data=data, parent=parent, *args, **kwargs
        )
