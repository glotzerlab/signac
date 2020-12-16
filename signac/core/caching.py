# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the caching feature to SyncedCollection API."""
import uuid
import logging
import pickle
from collections.abc import MutableMapping

logger = logging.getLogger(__name__)


def get_cache():
    """Return the cache.

    This method returns an instance of :class:`~RedisCache` if a Redis server
    is available, or otherwise an instance of :class:`dict` for an in-memory
    cache.

    Returns
    -------
    cache
        An instance of :class:`~RedisCache` if redis-server is available, otherwise :class:`dict`.
    """
    try:
        import redis

        REDIS = True
    except ImportError as error:
        logger.debug(str(error))
        REDIS = False
    if REDIS:
        try:
            # try to connect to server
            cache = redis.Redis()
            test_key = str(uuid.uuid4())
            cache.set(test_key, 0)
            assert cache.get(test_key) == b"0"  # Redis stores data as bytes
            cache.delete(test_key)
            logger.info("Using Redis cache.")
            return RedisCache(cache)
        except (redis.exceptions.ConnectionError, AssertionError) as error:
            logger.debug(str(error))
    logger.info("Redis not available.")
    return {}


class RedisCache(MutableMapping):
    """Redis backend based cache.

    The RedisCache is a :class:`~collections.abc.MutableMapping`. It uses redis-server
    to store data by using redis client. Redis client only accepts data as bytes, strings
    or numbers (ints, longs and floats) and returns response as bytes. So, this uses pickle
    serialization to convert data into bytes.
    """

    def __init__(self, client):
        self._client = client

    def __setitem__(self, key, value):
        self._client[key] = pickle.dumps(value)

    def __getitem__(self, key):
        return pickle.loads(self._client[key])

    def __delitem__(self, key):
        self._client.delete(key)

    def __contains__(self, key):
        return key in self._client

    def __iter__(self):
        for key in self._client.keys():
            yield key.decode()

    def __len__(self):
        return len(self._client.keys())
