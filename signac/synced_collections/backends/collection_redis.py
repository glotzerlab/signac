# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a Redis :class:`~.SyncedCollection` backend."""
import json

from .. import SyncedCollection, SyncedDict, SyncedList
from ..utils import SyncedCollectionJSONEncoder
from ..validators import json_format_validator, require_string_key


class RedisCollection(SyncedCollection):
    r"""A :class:`~.SyncedCollection` that synchronizes with a Redis database.

    This backend stores data in Redis by associating it with the provided key.

    **Thread safety**

    The :class:`RedisCollection` is not thread-safe.

    Parameters
    ----------
    client : redis.Redis
        The Redis client used to persist data.
    key : str
        The key associated with this collection in the Redis database.
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    """

    _backend = __name__  # type: ignore

    # Redis collection relies on JSON-serialization for the data.
    _validators = (json_format_validator,)

    def __init__(self, client=None, key=None, *args, **kwargs):
        super().__init__(**kwargs)
        self._client = client
        self._key = key

    def _load_from_resource(self):
        """Load the data from a Redis database.

        Returns
        -------
        Collection or None
            An equivalent unsynced collection satisfying :meth:`~.is_base_type` that
            contains the data in the Redis database. Will return None if no data
            was found in the Redis database.

        """
        blob = self._client.get(self._key)
        return None if blob is None else json.loads(blob)

    def _save_to_resource(self):
        """Write the data to a Redis database."""
        self._client.set(
            self._key, json.dumps(self, cls=SyncedCollectionJSONEncoder).encode()
        )

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


class RedisDict(RedisCollection, SyncedDict):
    r"""A dict-like data structure that synchronizes with a persistent Redis database.

    Examples
    --------
    >>> doc = RedisDict('data')
    >>> doc['foo'] = "bar"
    >>> assert doc['foo'] == "bar"
    >>> assert 'foo' in doc
    >>> del doc['foo']
    >>> doc['foo'] = dict(bar=True)
    >>> doc
    {'foo': {'bar': True}}

    Parameters
    ----------
    client : redis.Redis, optional
        A redis client (Default value = None).
    key : str, optional
        The key of the  collection (Default value = None).
    data : :class:`collections.abc.Mapping`, optional
        The initial data passed to :class:`RedisDict`. If ``None``, defaults to
        ``{}`` (Default value = None).
    parent : RedisCollection, optional
        A parent instance of :class:`RedisCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`RedisDict` object behaves like a :class:`dict`, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying database, copying a :class:`RedisDict` instance may
    exhibit unexpected behavior. If a true copy is required, you should use the
    call operator to get a dictionary representation, and if necessary
    construct a new :class:`RedisDict` instance.

    """

    _validators = (require_string_key,)

    def __init__(self, client=None, key=None, data=None, parent=None, *args, **kwargs):
        super().__init__(
            client=client, key=key, data=data, parent=parent, *args, **kwargs
        )


class RedisList(RedisCollection, SyncedList):
    r"""A list-like data structure that synchronizes with a persistent Redis database.

    Only non-string sequences are supported by this class.

    Examples
    --------
    >>> synced_list = RedisList('data')
    >>> synced_list.append("bar")
    >>> assert synced_list[0] == "bar"
    >>> assert len(synced_list) == 1
    >>> del synced_list[0]


    Parameters
    ----------
    client : redis.Redis, optional
        A Redis client (Default value = None).
    key : str, optional
        The key of the  collection (Default value = None).
    data : non-str :class:`collections.abc.Sequence`, optional
        The initial data passed to :class:`RedisList`. If ``None``, defaults to
        ``[]`` (Default value = None).
    parent : RedisCollection, optional
        A parent instance of :class:`RedisCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`RedisList` object behaves like a :class:`list`, there are
    important distinctions to remember. In particular, because operations are
    reflected as changes to an underlying database, copying a
    :class:`RedisList` instance may exhibit unexpected behavior. If a true copy
    is required, you should use the call operator to get a dictionary
    representation, and if necessary construct a new :class:`RedisList`
    instance.
    """

    def __init__(self, client=None, key=None, data=None, parent=None, *args, **kwargs):
        super().__init__(
            client=client, key=key, data=data, parent=parent, *args, **kwargs
        )
