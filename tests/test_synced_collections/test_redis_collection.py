# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import uuid

import pytest
from synced_collection_test import SyncedDictTest, SyncedListTest

from signac.synced_collections.backends.collection_redis import RedisDict, RedisList

try:
    import redis

    try:
        # Try to connect to server
        redis_client = redis.Redis()
        test_key = str(uuid.uuid4())
        redis_client.set(test_key, 0)
        if redis_client.get(test_key) == b"0":  # Redis stores data as bytes
            raise RuntimeError("Cache access check failed.")
        redis_client.delete(test_key)
        REDIS = True
    except (redis.exceptions.ConnectionError, RuntimeError):
        REDIS = False
except ImportError:
    REDIS = False


class RedisCollectionTest:

    _key = "test"

    def store(self, synced_collection, data):
        synced_collection.client.set(synced_collection.key, json.dumps(data).encode())

    @pytest.fixture
    def synced_collection(self, request):
        request.addfinalizer(redis_client.flushall)
        yield self._collection_type(key=self._key, client=redis_client)

    @pytest.fixture
    def synced_collection_positional(self, request):
        """Fixture that initializes the object using positional arguments."""
        request.addfinalizer(redis_client.flushall)
        yield self._collection_type(redis_client, self._key)

    def test_key(self, synced_collection):
        assert synced_collection.key == self._key


@pytest.mark.skipif(
    not REDIS, reason="test requires the redis package and running redis-server"
)
class TestRedisDict(RedisCollectionTest, SyncedDictTest):
    _collection_type = RedisDict


@pytest.mark.skipif(
    not REDIS, reason="test requires the redis package and running redis-server"
)
class TestRedisList(RedisCollectionTest, SyncedListTest):
    _collection_type = RedisList
