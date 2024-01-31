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

    @pytest.mark.usefixtures("find_filter")
    def test_interpret_json(self, find_filter):
        def _assert_equal(q):
            # TODO: full code path not tested with this test.
            # _assert_equal and _find_expression, are not tested
            assert q == self._parse([json.dumps(q)])

        for f in find_filter:
            _assert_equal(f)

    @pytest.mark.usefixtures("find_filter")
    def test_interpret_simple(self, find_filter):
        assert self._parse(["a"]) == {"a": {"$exists": True}}
        assert next(parse_simple(["a"])) == ("a", {"$exists": True})

        for s, v in VALUES.items():
            assert self._parse(["a", s]) == {"a": v}
        for f in find_filter:
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
