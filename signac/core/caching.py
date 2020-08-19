# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the caching feature to  SyncedCollection API."""
import uuid
import logging
from abc import abstractmethod

from .synced_collection import SyncedCollection
logger = logging.getLogger(__name__)


def get_cache(redis_kwargs=None, mem_cache_kwargs=None):
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
    mem_cache_kwargs: dict
        Kwargs passed to :class:`MemCache` (Default value = None).

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
            REDIS_Cache = True
        except (redis.exceptions.ConnectionError, AssertionError) as error:
            logger.debug(str(error))
            REDIS_Cache = False
    except ImportError as error:
        logger.debug(str(error))
        REDIS_Cache = False
    if not REDIS_Cache:
        logger.info("Redis not available.")
        mem_cache_kwargs = {} if mem_cache_kwargs is None else mem_cache_kwargs
        CACHE = MemCache(**mem_cache_kwargs)
    else:
        logger.info("Using redis cache.")
    return CACHE


class MemCache(dict):
    "Implements the in-memory cache"

    def __init__(self, *args, cache_miss_warning_threshold=500, **kwargs):
        self._misses = 0
        self._warned = False
        self._miss_warning_threshold = cache_miss_warning_threshold
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        try:
            super().__getitem__(key)
        except KeyError:
            self._misses += 1
            if not self._warned and self._misses > self._miss_warning_threshold:
                logger.debug("High number of cache misses.")
            self._warned = True
            raise


class CachedSyncedCollection(SyncedCollection):
    """Implement caching for SyncedCollection API."""

    def __init__(self, cache, **kwargs):
        self._cache = cache
        super().__init__(**kwargs)
        self._is_cached = True

    # methods required for cache implementation
    @abstractmethod
    def _read_from_cache(self):
        """Read the data from cache."""
        pass

    @abstractmethod
    def _write_to_cache(self, data=None):
        """Write the data to cache."""
        pass

    # overwriting sync and load methods to add caching mechanism
    def sync(self):
        """Synchronize the data with the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                # write the data to backend and update the cache
                self._sync_to_backend()
                self._write_to_cache()
            else:
                self._parent.sync()

    def load(self):
        """Load the data from the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                # fetch data from cache
                data = self._read_from_cache()
                if data is None:
                    # if no data in cache load the data from backend
                    # and update the cache
                    data = self._load_from_backend()
                    self._write_to_cache(data)
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent.load()

    # Cache invalidation
    def refresh_cache(self):
        """Load the data from backend and update the cache."""
        if self._parent is None:
            data = self._load()
            self._write_to_cache(data)
        else:
            self._parent.refresh_cache()
