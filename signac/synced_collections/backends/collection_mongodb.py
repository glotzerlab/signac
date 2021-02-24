# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a MongoDB :class:`~.SyncedCollection` backend."""
from .. import SyncedCollection, SyncedDict, SyncedList
from ..validators import json_format_validator, require_string_key

try:
    import bson

    MONGO = True
except ImportError:
    MONGO = False


class MongoDBCollection(SyncedCollection):
    r"""A :class:`~.SyncedCollection` that synchronizes with a MongoDB document.

    In MongoDB, a database is composed of multiple MongoDB **collections**, which
    are analogous to tables in SQL databases but do not enforce a schema like
    in relational databases. In turn, collections are composed of **documents**,
    which are analogous to rows in a table but are much more flexible, storing
    any valid JSON object in a JSON-like encoded format known as BSON
    ("binary JSON").

    Each :class:`~.MongoDBCollection` can be represented as a MongoDB document,
    so this backend stores the :class:`~.MongoDBCollection` as a single
    document within the collection provided by the user. The document is
    identified by a unique key provided by the user.

    **Thread safety**

    The :class:`MongoDBCollection` is not thread-safe.

    Parameters
    ----------
    collection : :class:`pymongo.collection.Collection`
        The MongoDB client in which to store data.
    uid : dict
        The unique key-value mapping added to the data and stored in the document
        so that it is uniquely identifiable in the MongoDB collection. The key
        "data" is reserved and may not be part of this uid.
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    The user is responsible for providing a unique id such that there are no
    possible collisions between different :class:`~.MongoDBCollection` instances
    stored in the same MongoDB collection. Failure to do so may result in data
    corruption if multiple documents are found to be apparently associated with
    a given ``uid``.

    """

    _backend = __name__  # type: ignore

    # MongoDB uses BSON, which is not exactly JSON but is close enough that
    # JSON-validation is reasonably appropriate. we could generalize this to do
    # proper BSON validation if we find that the discrepancies (for instance, the
    # supported integer data types differ) are too severe.
    _validators = (json_format_validator,)

    def __init__(self, collection=None, uid=None, parent=None, *args, **kwargs):
        super().__init__(parent=parent, **kwargs)
        if not MONGO:
            raise RuntimeError(
                "The PyMongo package must be installed to use the MongoDBCollection."
            )

        self._collection = collection
        if uid is not None and "data" in uid:
            raise ValueError("The key 'data' may not be part of the uid.")
        self._uid = uid

    def _load_from_resource(self):
        """Load the data from a MongoDB document.

        Returns
        -------
        Collection or None
            An equivalent unsynced collection satisfying :meth:`~.is_base_type` that
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


class MongoDBDict(MongoDBCollection, SyncedDict):
    r"""A dict-like data structure that synchronizes with a document in a MongoDB collection.

    Examples
    --------
    >>> doc = MongoDBDict('data')
    >>> doc['foo'] = "bar"
    >>> assert doc['foo'] == "bar"
    >>> assert 'foo' in doc
    >>> del doc['foo']
    >>> doc['foo'] = dict(bar=True)
    >>> doc
    {'foo': {'bar': True}}

    Parameters
    ----------
    collection : pymongo.collection.Collection, optional
        A :class:`pymongo.collection.Collection` instance (Default value = None).
    uid : dict, optional
        The unique key-value mapping identifying the collection (Default value = None).
    data : non-str :class:`collections.abc.Mapping`, optional
        The initial data passed to :class:`MongoDBDict`. If ``None``, defaults to
        ``{}`` (Default value = None).
    parent : MongoDBCollection, optional
        A parent instance of :class:`MongoDBCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`MongoDBDict` object behaves like a :class:`dict`, there are
    important distinctions to remember. In particular, because operations are
    reflected as changes to an underlying database, copying a
    :class:`MongoDBDict` instance may exhibit unexpected behavior. If a true
    copy is required, you should use the call operator to get a dictionary
    representation, and if necessary construct a new :class:`MongoDBDict`
    instance.

    """

    _validators = (require_string_key,)

    def __init__(
        self, collection=None, uid=None, data=None, parent=None, *args, **kwargs
    ):
        super().__init__(
            collection=collection, uid=uid, data=data, parent=parent, *args, **kwargs
        )


class MongoDBList(MongoDBCollection, SyncedList):
    r"""A list-like data structure that synchronizes with a document in a MongoDB collection.

    Only non-string sequences are supported by this class.

    Examples
    --------
    >>> synced_list = MongoDBList('data')
    >>> synced_list.append("bar")
    >>> assert synced_list[0] == "bar"
    >>> assert len(synced_list) == 1
    >>> del synced_list[0]

    Parameters
    ----------
    collection : pymongo.collection.Collection, optional
        A :class:`pymongo.collection.Collection` instance (Default value = None).
    uid : dict, optional
        The unique key-value mapping identifying the collection (Default value = None).
    data : non-str :class:`collections.abc.Sequence`, optional
        The initial data passed to :class:`MongoDBList`. If ``None``, defaults to
        ``[]`` (Default value = None).
    parent : MongoDBCollection, optional
        A parent instance of :class:`MongoDBCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`MongoDBList` object behaves like a :class:`list`, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying database, copying a :class:`MongoDBList` instance may
    exhibit unexpected behavior. If a true copy is required, you should use the
    call operator to get a dictionary representation, and if necessary
    construct a new :class:`MongoDBList` instance.

    """

    def __init__(
        self, collection=None, uid=None, data=None, parent=None, *args, **kwargs
    ):
        super().__init__(
            collection=collection, uid=uid, data=data, parent=parent, *args, **kwargs
        )
