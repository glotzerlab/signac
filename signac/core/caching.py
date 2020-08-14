# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement the caching feature to  SyncedCollection API."""
import uuid
import logging
from abc import abstractmethod

from .synced_collection import SyncedCollection
logger = logging.getLogger(__name__)

CACHE = None


def get_cache():
    """Return the refernce to the global cache."""
    global CACHE
    if CACHE is None:
        try:
            import redis
            try:
                # try to connect to server
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
            CACHE = dict()
        else:
            logger.info("Using redis cache.")
    return CACHE


class CachedSyncedCollection(SyncedCollection):
    """Implement caching for SyncedCollection API."""

    def __init__(self, cache=None, **kwargs):
        self._cache = get_cache() if cache is None else cache
        super().__init__(**kwargs)

    # methods required for cache implementation
    @abstractmethod
    def _read_from_cache(self):
        """Read the data from cache."""
        pass

    @abstractmethod
    def _write_to_cache(self):
        """Write the data to cache."""
        pass

    # overwriting sync and load methods to add caching mechanism
    def sync(self):
        """Synchronize the data with the underlying backend."""
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                # write the data to backend and file
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
                    data = self._load()
                    self._write_to_cache(data)
                with self._suspend_sync():
                    self._update(data)
            else:
                self._parent.load()

    def refresh_cache(self):
        """Load the data from backend and update the cache"""
        data = self._load()
        self._write_to_cache(data)
