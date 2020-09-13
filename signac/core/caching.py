# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the caching feature to  SyncedCollection API."""
import uuid
import logging

logger = logging.getLogger(__name__)
REDIS = None

def is_redis_available():
    """Return True if redis-server is available, False otherwise"""
    global REDIS
    if REDIS is None:
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
                assert cache.get(test_key) == b'0'  # redis store data as bytes
                cache.delete(test_key)
            except (redis.exceptions.ConnectionError, AssertionError) as error:
                logger.debug(str(error))
                REDIS = False
    return REDIS
        


def get_cache():
    """Return the cache.

    This method returns Redis client if available else return an instance of ``dict``.

    Redis client only accepts data as bytes, strings or numbers (ints, longs and floats).
    Attempting to specify a key or a value as any other type will raise a exception.
    All responses are returned as bytes.

    Returns
    -------
    cache
        Redis client if available, otherwise dictionary.
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
            assert cache.get(test_key) == b'0'  # redis store data as bytes
            cache.delete(test_key)
            logger.info("Using redis cache.")
            return cache
        except (redis.exceptions.ConnectionError, AssertionError) as error:
            logger.debug(str(error))
    logger.info("Redis not available.")
    return dict()
