# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the caching feature to  SyncedCollection API."""
import uuid
import logging

logger = logging.getLogger(__name__)


def get_cache(redis_kwargs=None):
    """Return the cache.

    This mehtod returns Redis client if availalbe else return instance of :class:`MemCache`.
    Redis client only accepts data as bytes, strings or numbers (ints, longs and floats).
    Attempting to specify a key or a value as any other type will raise a exception.
    All responses are returned as bytes in Python 3 and str in Python 2. If all string
    responses from a client should be decoded, the user can specify.
    `decode_responses=True` in `redis_kwargs`.

    Parameters
    ----------
    redis_kwargs: dict
        Kwargs passed to `redis.Redis` (Default value = None).

    Returns
    -------
    CACHE
    """
    try:
        import redis
        try:
            # try to connect to server
            redis_kwargs = {} if redis_kwargs is None else redis_kwargs
            CACHE = redis.Redis()
            test_key = str(uuid.uuid4())
            CACHE.set(test_key, 0)
            assert CACHE.get(test_key) == b'0'  # redis store data as bytes
            CACHE.delete(test_key)
        except (redis.exceptions.ConnectionError, AssertionError) as error:
            logger.debug(str(error))
            CACHE = None
    except ImportError as error:
        logger.debug(str(error))
    if CACHE is None:
        logger.info("Redis not available.")
        CACHE = dict()
    else:
        logger.info("Using redis cache.")
    return CACHE
