# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the caching feature to  SyncedCollection API."""
import uuid
import logging

logger = logging.getLogger(__name__)


def get_cache(redis_kwargs=None):
    """Return the cache.

    This method returns Redis client if available else return an instance of ``dict``.

    Redis client only accepts data as bytes, strings or numbers (ints, longs and floats).
    Attempting to specify a key or a value as any other type will raise a exception.
    All responses are returned as bytes. 

    Returns
    -------
    Cache
        Redis client if available, otherwise instance of dict.
    """
    try:
        import redis
        try:
            # try to connect to server
            Cache = redis.Redis()
            test_key = str(uuid.uuid4())
            Cache.set(test_key, 0)
            assert Cache.get(test_key) == b'0'  # redis store data as bytes
            Cache.delete(test_key)
        except (redis.exceptions.ConnectionError, AssertionError) as error:
            logger.debug(str(error))
            Cache = None
    except ImportError as error:
        logger.debug(str(error))
        Cache = None
    if Cache is None:
        logger.info("Redis not available.")
        Cache = dict()
    else:
        logger.info("Using redis cache.")
    return Cache
