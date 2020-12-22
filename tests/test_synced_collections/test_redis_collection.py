# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import json
import uuid

from signac.core.synced_collections.collection_redis import RedisDict
from signac.core.synced_collections.collection_redis import RedisList
from test_synced_collection import TestJSONDict
from test_synced_collection import TestJSONList

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


@pytest.mark.skipif(not REDIS, reason='test requires the redis package and running redis-server')
class TestRedisDict(TestJSONDict):

    @pytest.fixture
    def synced_dict(self, request):
        self._client = RedisClient
        request.addfinalizer(self._client.flushall)
        self._name = 'test'
        yield RedisDict(name=self._name, client=self._client)

    def store(self, data):
        self._client.set(self._name, json.dumps(data).encode())


@pytest.mark.skipif(not REDIS, reason='test requires the redis package and running redis-server')
class TestRedisList(TestJSONList):

    @pytest.fixture
    def synced_list(self, request):
        self._client = RedisClient
        request.addfinalizer(self._client.flushall)
        self._name = 'test'
        yield RedisList(name=self._name, client=self._client)

    def store(self, data):
        self._client.set(self._name, json.dumps(data).encode())
