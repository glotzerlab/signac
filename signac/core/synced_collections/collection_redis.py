# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements Redis-backend.

This implements the Redis-backend for SyncedCollection API by
implementing sync and load methods.
"""
import json
from copy import deepcopy

from .synced_collection import SyncedCollection
from .synced_attr_dict import SyncedAttrDict
from .synced_list import SyncedList


class RedisCollection(SyncedCollection):
    """Implement sync and load using a Redis backend."""

    _backend = __name__  # type: ignore

    def __init__(self, client=None, key=None, parent=None, **kwargs):
        self._client = client
        self._key = key
        super().__init__(parent=parent, **kwargs)

    def _load_from_resource(self):
        """Load the data from a Redis-database."""
        blob = self._client.get(self._key)
        return None if blob is None else json.loads(blob)

    def _save_to_resource(self):
        """Write the data from Redis-database."""
        self._client.set(self._key, json.dumps(self._to_base()).encode())

    def _pseudo_deepcopy(self):
        """Return a copy of instance.

        It is a pseudo implementation for `deepcopy` because
        `redis.Redis` does not support `deepcopy` method.
        """
        if self._parent is not None:
            # TODO: Do we really want a deep copy of a nested collection to
            # deep copy the parent? Perhaps we should simply disallow this?
            return type(self)(client=None, key=None, data=self._to_base(),
                              parent=deepcopy(self._parent))
        else:
            return type(self)(client=self._client, key=self._key, data=None,
                              parent=None)

    @property
    def client(self):
        """`redis.Redis`: The Redis client used to store the data."""
        return self._client

    @property
    def key(self):
        """str: The key of this collection stored in Redis."""
        return self._key


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
        true copy is required, you should use the call operator to get a
        dictionary representation, and if necessary construct a new RedisDict
        instance: `new_dict = RedisDict(old_dict())`.

    Parameters
    ----------
    client: object, optional
        A redis client (Default value = None).
    data: mapping, optional
        The intial data pass to RedisDict. Defaults to `dict()`
    key: str, optional
        The key of the  collection (Default value = None).
    parent: object, optional
        A parent instance of RedisDict (Default value = None).
    """


class RedisList(RedisCollection, SyncedList):
    """A non-string sequence interface to a persistent Redis file.

    The RedisList inherits from :class:`~core.synced_collection.SyncedCollection`
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
        true copy is required, you should use the call operator to get a
        dictionary representation, and if necessary construct a new RedisList
        instance: `new_list = RedisList(old_list())`.

    Parameters
    ----------
    client: object, optional
        A redis client (Default value = None).
    data: non-str Sequence, optional
        The intial data pass to RedisList. Defaults to `list()`
    key: str, optional
        The key of the  collection (Default value = None).
    parent: object, optional
        A parent instance of RedisList (Default value = None).
    """
