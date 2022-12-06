# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
from contextlib import redirect_stderr
from io import StringIO
from itertools import chain
from json import JSONDecodeError

import pytest

from signac.filterparse import parse_filter_arg, parse_simple

FILTERS = [
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


VALUES = {"1": 1, "1.0": 1.0, "abc": "abc", "true": True, "false": False, "null": None}

ARITHMETIC_EXPRESSIONS = [
    {"$eq": 0},
    {"$ne": 0},
    {"$lt": 0},
    {"$gt": 0},
    {"$lte": 0},
    {"$gte": 0},
]


ARRAY_EXPRESSIONS = [
    {"$in": []},
    {"$in": [0, 1, 2]},
    {"$in": ["a", "b", "c"]},
    {"$nin": []},
    {"$nin": [0, 1, 2]},
    {"$nin": ["a", "b", "c"]},
]


class TestFindCommandLineInterface:
    @staticmethod
    def _parse(args):
        with redirect_stderr(StringIO()):
            return parse_filter_arg(args)

    def test_interpret_json(self):
        def _assert_equal(q):
            assert q == self._parse([json.dumps(q)])

        for f in FILTERS:
            _assert_equal(f)

    def test_interpret_simple(self):
        assert self._parse(["a"]) == {"a": {"$exists": True}}
        assert next(parse_simple(["a"])) == ("a", {"$exists": True})

        for s, v in VALUES.items():
            assert self._parse(["a", s]) == {"a": v}
        for f in FILTERS:
            f_ = f.copy()
            key, value = f.popitem()
            if key.startswith("$"):
                continue
            assert self._parse([key, json.dumps(value)]) == f_

    def test_interpret_mixed_key_value(self):
        for expr in chain(ARITHMETIC_EXPRESSIONS, ARRAY_EXPRESSIONS):
            assert self._parse(["a", json.dumps(expr)]) == {"a": expr}

    def test_invalid_json(self):
        with pytest.raises(JSONDecodeError):
            self._parse(['{"x": True}'])
