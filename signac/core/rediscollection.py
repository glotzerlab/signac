# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements Redis-backend.

This implements the Redis-backend for SyncedCollection API by
implementing sync and load methods.
"""
import json
import redis

from .synced_collection import SyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList


class RedisCollection(SyncedCollection):
    """Implement sync and load using a Redis backend."""

    backend = __name__  # type: ignore

    def __init__(self, name=None, client=None, redis_kwargs=None, **kwargs):
        if client is None:
            redis_kwargs = redis_kwargs if redis_kwargs is not None else {}
            self._client = redis.Redis(**redis_kwargs)
        else:
            self._client = client
        self._name = name
        super().__init__(**kwargs)
        if (name is None) == (self._parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or name must be None, but not both.")

    def _load(self):
        """Load the data from a Redis-database."""
        blob = self._client.get(self._name)
        return json.loads(blob) if blob is not None else blob

    def _sync(self):
        """Write the data from Redis-database."""
        data = self.to_base()
        # Serialize data:
        blob = json.dumps(data).encode()

        self._client.set(self._name, blob)


class RedisDict(RedisCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent Redis-database.

    The RedisDict inherits from :class:`~core.rediscollection.RedisCollection`
    and :class:`~core.syncedattrdict.SyncedAttrDict`.

    .. code-block:: python

        doc = RedisDict('data')
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

        While the RedisDict object behaves like a dictionary, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a RedisDict instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new RedisDict
        instance: `new_dict = RedisDict(old_dict.to_base())`.

    Parameters
    ----------
    name: str
        The name of the  collection (Default value = None).
    client:
        A redis client.
    redis_kwargs: dict
        kwargs arguments passed through to the `redis.Redis` function.
    data: mapping, optional
        The intial data pass to RedisDict. Defaults to `dict()`
    parent: object, optional
        A parent instance of RedisDict or None (Default value = None).


    """

    pass


class RedisList(RedisCollection, SyncedList):
    """A non-string sequence interface to a persistent Redis file.

    The RedisDict inherits from :class:`~core.collection_api.SyncedCollection`
    and :class:`~core.syncedlist.SyncedList`.

    .. code-block:: python

        synced_list = RedisList('data')
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    .. warning::

        While the RedisList object behaves like a list, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a RedisList instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new RedisList
        instance: `new_list = RedisList(old_list.to_base())`.

    Parameters
    ----------
    name: str
        The name of the  collection (Default value = None).
    client:
        A redis client.
    redis_kwargs: dict
        kwargs arguments passed through to the `redis.Redis` function.
    data: mapping, optional
        The intial data pass to RedisDict. Defaults to `list()`
    parent: object, optional
        A parent instance of RedisDict or None (Default value = None).

    """

    pass


SyncedCollection.register(RedisDict, RedisList)
