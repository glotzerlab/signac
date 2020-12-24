# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import json
import uuid

from signac.core.synced_collections.collection_redis import RedisCollection
from signac.core.synced_collections.collection_redis import RedisDict
from signac.core.synced_collections.collection_redis import RedisList
from synced_collection_test import SyncedDictTest, SyncedListTest

try:
    import redis
    try:
        # try to connect to server
        RedisClient = redis.Redis()
        test_key = str(uuid.uuid4())
        RedisClient.set(test_key, 0)
        assert RedisClient.get(test_key) == b'0'  # redis store data as bytes
        RedisClient.delete(test_key)
        REDIS = True
    except (redis.exceptions.ConnectionError, AssertionError):
        REDIS = False
except ImportError:
    REDIS = False


class RedisCollectionTest:

    _backend = 'signac.core.synced_collections.collection_redis'
    _backend_collection = RedisCollection

    def store(self, data):
        self._client.set(self._name, json.dumps(data).encode())

    @pytest.fixture(autouse=True)
    def synced_collection(self, request):
        self._client = RedisClient
        request.addfinalizer(self._client.flushall)
        self._name = 'test'
        self._backend_kwargs = {'name': self._name, 'client': self._client}
        yield self._collection_type(**self._backend_kwargs)

    @pytest.fixture
    def synced_collection_positional(self, request):
        """Fixture that initializes the object using positional arguments."""
        self._client = RedisClient
        request.addfinalizer(self._client.flushall)
        self._name = 'test'
        yield self._collection_type(self._client, self._name)


@pytest.mark.skipif(
    not REDIS,
    reason='test requires the redis package and running redis-server')
class TestRedisDict(RedisCollectionTest, SyncedDictTest):
    _collection_type = RedisDict


@pytest.mark.skipif(
    not REDIS,
    reason='test requires the redis package and running redis-server')
class TestRedisList(RedisCollectionTest, SyncedListTest):
    _collection_type = RedisList
