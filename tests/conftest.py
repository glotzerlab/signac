import uuid

import pytest


@pytest.fixture
def testdata():
    return str(uuid.uuid4())


@pytest.fixture
def find_filter():
    return [
        {"a": 0},
        {"a.b": 0},
        {"a.b": {"$lt": 42}},
        {"a.b.$lt": 42},
        {"$or": [{"a.b": 41}, {"a.b.$lt": 42}]},
        {"$or": [{"a.b": 42}, {"a.b.$lt": 42}]},
        {"$and": [{"a.b": 42}, {"a.b.$lt": 42}]},
        {"$and": [{"a.b": 0}, {"a.b.$lt": 42}]},
        {"$and": [{"a.b.$gte": 0}, {"a.b.$lt": 42}]},
        {"$not": {"a.b": 0}},
        {"$and": [{"a.b.$gte": 0}, {"$not": {"a.b.$lt": 42}}]},
        {"$not": {"$not": {"a.b": 0}}},
        {"a.b": {"$in": [0, 1]}},
        {"a.b": {"$nin": [0, 1]}},
        {"$not": {"a.b": {"$in": [0, 1]}}},
        {"a.b": {"$exists": True}},
        {"a.b": {"$exists": False}},
        {"a": {"$exists": True}},
        {"a": {"$exists": False}},
        {"c": {"$regex": r"^\d$"}},
        {"c": {"$type": "str"}},
        {"d": {"$type": "list"}},
        {"a.b": {"$where": "lambda x: x < 10"}},
        {"a.b": {"$where": "lambda x: isinstance(x, int)"}},
        {"a": {"$regex": "[a][b][c]"}},
    ]
