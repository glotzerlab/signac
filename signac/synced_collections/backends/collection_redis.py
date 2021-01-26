# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a Redis SyncedCollection backend."""
import json

from .synced_attr_dict import SyncedAttrDict
from .synced_collection import SyncedCollection
from .synced_list import SyncedList


class RedisCollection(SyncedCollection):
    """A :class:`SyncedCollection` that synchronizes with a Redis database.

    This backend stores data in Redis by associating it with the provided key.

    **Thread safety**

    The RedisCollection is not thread-safe.

    Parameters
    ----------
    client : redis.Redis
        The Redis client used to persist data.
    key : str
        The key associated with this collection in the Redis database.

    """

    _backend = __name__  # type: ignore

    def __init__(self, client=None, key=None, **kwargs):
        self._client = client
        self._key = key
        super().__init__(**kwargs)

    def _load_from_resource(self):
        """Load the data from a Redis database.

        Returns
        -------
        Collection or None
            An equivalent unsynced collection satisfying :meth:`is_base_type` that
            contains the data in the Redis database. Will return None if no data
            was found in the Redis database.

        """
        blob = self._client.get(self._key)
        return None if blob is None else json.loads(blob)

    def _save_to_resource(self):
        """Write the data to a Redis database."""
        self._client.set(self._key, json.dumps(self._to_base()).encode())

    @property
    def client(self):
        """`redis.Redis`: The Redis client used to store the data."""
        return self._client

    @property
    def key(self):
        """str: The key associated with this collection stored in Redis."""
        return self._key

    def __deepcopy__(self, memo):
        # The underlying Redis client cannot be deepcopied.
        raise TypeError("RedisCollection does not support deepcopying.")


class RedisDict(RedisCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent Redis-database.

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

    Parameters
    ----------
    client: redis.Redis, optional
        A redis client (Default value = None).
    key: str, optional
        The key of the  collection (Default value = None).
    data: :py:class:`collections.abc.Mapping`, optional
        The intial data pass to RedisDict. Defaults to `dict()`
    parent: RedisCollection, optional
        A parent instance of RedisCollection (Default value = None).

    Warnings
    --------

    While the RedisDict object behaves like a dictionary, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying database, copying a RedisDict instance may
    exhibit unexpected behavior. If a true copy is required, you should use the
    call operator to get a dictionary representation, and if necessary
    construct a new RedisDict instance: ``new_dict = RedisDict(old_dict())``.

    """

    def __init__(self, client=None, key=None, data=None, parent=None, *args, **kwargs):
        super().__init__(
            client=client, key=key, data=data, parent=parent, *args, **kwargs
        )


class RedisList(RedisCollection, SyncedList):
    """A non-string sequence interface to a persistent Redis file.

    .. code-block:: python

        synced_list = RedisList('data')
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    .. warning::

        While the RedisList object behaves like a list, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying a RedisList
        instance may exhibit unexpected behavior. If a true copy is required,
        you should use the call operator to get a dictionary representation,
        and if necessary construct a new RedisList instance:
        ``new_list = RedisList(old_list())``.

    Parameters
    ----------
    client: redis.Redis, optional
        A Redis client (Default value = None).
    key: str, optional
        The key of the  collection (Default value = None).
    data: non-str :py:class:`collections.abc.Sequence`, optional
        The intial data pass to RedisList. Defaults to `list()`
    parent: RedisCollection, optional
        A parent instance of RedisCollection (Default value = None).

    """

    def __init__(self, client=None, key=None, data=None, parent=None, *args, **kwargs):
        super().__init__(
            client=client, key=key, data=data, parent=parent, *args, **kwargs
        )
