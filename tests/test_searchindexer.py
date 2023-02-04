import pytest

from signac._search_indexer import _SearchIndexer
from signac.errors import InvalidKeyError

n = 42
N = 100

ARITHMETIC_DOCS = {str(i): {"a": i} for i in range(N)}

ARITHMETIC_EXPRESSIONS = [
    ({"$eq": n}, 1),
    ({"$ne": n}, N - 1),
    ({"$lt": n}, n),
    ({"$gt": n}, N - n - 1),
    ({"$lte": n}, n + 1),
    ({"$gte": n}, N - n),
]


ARRAY_EXPRESSIONS = [
    ({"$in": []}, 0),
    ({"$in": [0, 1, 2]}, 3),
    ({"$in": ["a", "b", "c"]}, 0),
    ({"$nin": []}, N),
    ({"$nin": [0, 1, 2]}, N - 3),
    ({"$nin": ["a", "b", "c"]}, N),
]

LOGICAL_EXPRESSIONS = [
    ({"$and": []}, ValueError),
    ({"$and": {}}, ValueError),
    ({"$and": ""}, ValueError),
    ({"a": {"$and": []}}, KeyError),
    ({"a": {"$and": [{"b": 0}]}}, KeyError),
    ({"$and": [{"a": n}]}, 1),
    ({"$and": [{"$not": {"a": n}}]}, N - 1),
    ({"$and": [{"a": n}, {"a": n + 1}]}, 0),
    ({"$and": [{"a": n}, {"$not": {"a": n}}]}, 0),
    ({"$or": []}, ValueError),
    ({"a": {"$or": []}}, KeyError),
    ({"a": {"$or": [{"b": 0}]}}, KeyError),
    ({"$or": [{"$not": {"a": n}}]}, N - 1),
    ({"$or": [{"a": n}, {"a": n + 1}]}, 2),
    ({"$or": [{"a": n}, {"$not": {"a": n}}]}, N),
]

INVALID_SYNTAX_EXPRESSIONS = [
    ({"a": {"$$lt": N}}, KeyError),  # Bad operator expression
    ({"a": {"lt$": N}}, KeyError),  # Bad operator placement
    ({"a": {"$exists": N}}, ValueError),  # Value of $exists must be boolean
    ({"a": {"$nonexistent": N}}, KeyError),  # Unknown expression-operator
    ({"a": {"$type": N}}, ValueError),  # Invalid $type argument
    (["a"], ValueError),  # Not a valid filter (not a dict)
]


class TestSearchIndexer:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.c = _SearchIndexer()

    def test_init(self):
        assert len(self.c) == 0

    def test_init_with_list_with_ids_sequential(self):
        docs = {str(i): {"a": i} for i in range(10)}
        self.c = _SearchIndexer(docs)
        assert len(self.c) == len(docs)
        for _id in docs:
            assert _id in self.c

    def test_init_with_list_with_ids_non_sequential(self):
        docs = {f"{i ** 3:032d}": {"a": i} for i in range(10)}
        self.c = _SearchIndexer(docs)
        assert len(self.c) == len(docs)
        for _id in docs:
            assert _id in self.c

    def test_int_float_equality(self):
        docs = {"int": {"a": 1}, "float": {"a": 1.0}}
        self.c = _SearchIndexer(docs)
        assert len(self.c.find()) == 2
        assert len(self.c.find(dict(a=1))) == 2
        assert len(self.c.find(dict(a=1.0))) == 2

    def test_copy(self):
        docs = {str(i): {"a": i} for i in range(10)}
        self.c = _SearchIndexer(docs)
        c2 = _SearchIndexer(self.c)
        assert len(self.c) == len(c2)
        for doc in c2.values():
            assert len(self.c.find(doc)) == 1

    def test_insert_and_remove(self):
        doc = {"a": 0}
        self.c["0"] = doc
        assert len(self.c) == 1
        assert self.c["0"] == doc
        assert list(self.c.find()) == ["0"]
        del self.c["0"]
        assert len(self.c) == 0
        with pytest.raises(KeyError):
            assert self.c["0"]

    def test_contains(self):
        doc = {"a": 0}
        assert "0" not in self.c
        self.c["0"] = doc
        assert "0" in self.c
        del self.c["0"]
        assert "0" not in self.c

    def test_update(self):
        docs = {str(i): {"a": i} for i in range(10)}
        self.c.update(docs)
        assert len(self.c) == len(docs)

    def test_update_collision(self):
        docs = {str(i): {"a": i} for i in range(10)}
        self.c.update(docs)
        # Update the first ten, insert the second ten
        new_docs = {str(i): {"a": i * 2} for i in range(20)}
        self.c.update(new_docs)
        assert len(self.c) == len(new_docs)
        assert self.c["0"] == {"a": 0}

    def test_index(self):
        docs = {str(i): {"a": i} for i in range(10)}
        self.c.update(docs)
        index = self.c.build_index("a")
        assert len(index) == len(self.c)
        for value, _ids in index.items():
            for _id in _ids:
                assert self.c[_id]["a"] == value

    def test_clear(self):
        assert len(self.c) == 0
        self.c["0"] = {"a": 0}
        assert len(self.c) == 1
        self.c.clear()
        assert len(self.c) == 0

    def test_iteration(self):
        assert len(self.c) == 0
        assert len(self.c.find()) == 0
        self.c["0"] = {"a": 0}
        assert len(self.c) == 1
        assert len(self.c.find()) == 1
        self.c.clear()
        docs = {str(i): {"a": i} for i in range(10)}
        self.c.update(docs)
        assert len(self.c) == len(docs)
        assert len(self.c.find()) == len(docs)
        assert {doc["a"] for doc in docs.values()} == {
            self.c[doc]["a"] for doc in self.c.find()
        }

    def test_find_id(self):
        assert len(self.c.find()) == 0
        assert len(self.c.find({"_id": "0"})) == 0
        docs = {str(i): {"a": i} for i in range(10)}
        self.c.update(docs)
        assert len(self.c.find()) == len(docs)
        assert len(self.c.find({"_id": "0"})) == 1

    def test_find_integer(self):
        assert len(self.c.find()) == 0
        assert list(self.c.find()) == []
        assert len(self.c.find({"a": 0})) == 0
        docs = {str(i): {"a": i} for i in range(10)}
        self.c.update(docs)
        assert len(self.c.find()) == len(docs)
        assert len(self.c.find({"a": 0})) == 1
        assert len(self.c.find({"a": 0.0})) == 1
        assert list(self.c.find({"a": 0})) == ["0"]
        assert len(self.c.find({"a": -1})) == 0
        assert len(self.c.find({"a.b": 0})) == 0
        assert len(self.c.find({"a": {"$type": "int"}})) == 10
        assert len(self.c.find({"a": {"$type": "float"}})) == 0
        del self.c["0"]
        assert len(self.c.find({"a": 0})) == 0

    def test_find_float(self):
        assert len(self.c.find()) == 0
        assert list(self.c.find()) == []
        assert len(self.c.find({"a": 0})) == 0
        docs = {str(i): {"a": float(i)} for i in range(10)}
        self.c.update(docs)
        assert len(self.c.find()) == len(docs)
        assert len(self.c.find({"a": 0})) == 1
        assert len(self.c.find({"a": 0.0})) == 1
        assert list(self.c.find({"a": 0.0})) == ["0"]
        assert len(self.c.find({"a": -1})) == 0
        assert len(self.c.find({"a.b": 0})) == 0
        assert len(self.c.find({"a": {"$type": "int"}})) == 0
        assert len(self.c.find({"a": {"$type": "float"}})) == 10
        del self.c["0"]
        assert len(self.c.find({"a": 0})) == 0

    def test_find_list(self):
        assert len(self.c.find()) == 0
        assert list(self.c.find()) == []
        assert len(self.c.find({"a": []})) == 0
        docs = {"0": {"a": []}}
        self.c.update(docs)
        assert len(self.c.find()) == 1
        assert len(self.c.find({"a": []})) == 1
        for i, v in enumerate((None, 1, "1", {"b": 1}), 1):
            docs = {str(i): {"a": [v]}}
            self.c.update(docs)
            assert len(self.c.find({"a": [v]})) == 1

    def test_find_int_float(self):
        docs = {"int": {"a": 1}, "float": {"a": 1.0}}
        self.c.update(docs)
        assert len(self.c.find({"a": {"$type": "float"}})) == 1
        assert len(self.c.find({"a": {"$type": "int"}})) == 1
        assert list(self.c.find({"a": {"$type": "float"}})) == ["float"]
        assert list(self.c.find({"a": {"$type": "int"}})) == ["int"]

    def test_docs_with_dots(self):
        # Dots are not allowed in keys of the document. Here, we explicitly
        # test to ensure that an error is raised if invalid keys are found.
        self.c["0"] = {"a.b": 0}

        # These searches will not trigger an error:
        self.c.find()
        self.c.find({"a": 0})

        # This search triggers an error:
        with pytest.raises(InvalidKeyError):
            self.c.find({"a.b": 0})

    def test_find_types(self):
        # Note: All of the iterables will be normalized to lists!
        t = [1, 1.0, "1", [1], tuple([1])]
        for i, t in enumerate(t):
            self.c.clear()
            doc = self.c[str(i)] = {"a": t}
            assert list(self.c.find(doc)) == [str(i)]

    def test_find_nested(self):
        docs = {str(i): {"a": {"b": i}} for i in range(10)}
        self.c.update(docs)
        assert len(self.c.find()) == len(docs)
        assert len(self.c.find({"a.b": 0})) == 1
        assert len(self.c.find({"a": {"b": 0}})) == 1
        assert list(self.c.find({"a.b": 0})) == ["0"]
        del self.c["0"]
        assert len(self.c.find({"a.b": 0})) == 0
        assert len(self.c.find({"a": {"b": 0}})) == 0

    def test_nested_lists(self):
        docs = {str(i): {"a": [[[i]]]} for i in range(10)}
        self.c.update(docs)
        assert len(self.c.find()) == len(docs)
        assert len(self.c.find({"a": [[[-1]]]})) == 0
        assert len(self.c.find({"a": [[[0]]]})) == 1

    def test_find_exists_operator(self):
        assert len(self.c) == 0
        data = {
            "a": True,
            "b": "b",
            "c": 0,
            "d": 0.1,
            "e": {"a": 0},
            "f": {"a": "b"},
            "g": [0, "a", True],
        }

        # Test without data
        for key in data:
            assert len(self.c.find({key: {"$exists": False}})) == len(self.c)
            assert len(self.c.find({key: {"$exists": True}})) == 0
            assert len(self.c.find({f"{key}.$exists": False})) == len(self.c)
            assert len(self.c.find({f"{key}.$exists": True})) == 0

        # Test for nested cases
        assert len(self.c.find({"e.a.$exists": True})) == 0
        assert len(self.c.find({"e.a.$exists": False})) == 0
        assert len(self.c.find({"e.a": {"$exists": True}})) == 0
        assert len(self.c.find({"e.a": {"$exists": False}})) == 0
        assert len(self.c.find({"f.a.$exists": True})) == 0
        assert len(self.c.find({"f.a.$exists": False})) == 0
        assert len(self.c.find({"f.a": {"$exists": True}})) == 0
        assert len(self.c.find({"f.a": {"$exists": False}})) == 0

        # Test with data
        for i, (key, value) in enumerate(data.items()):
            self.c[str(i)] = {key: value}

        # Heterogeneous nesting
        self.c[str(len(self.c))] = {"e": -1}

        for key in data:
            n = 2 if key == "e" else 1
            assert len(self.c.find({key: {"$exists": False}})) == len(self.c) - n
            assert len(self.c.find({key: {"$exists": True}})) == n
            assert len(self.c.find({f"{key}.$exists": False})) == len(self.c) - n
            assert len(self.c.find({f"{key}.$exists": True})) == n

        # Test for nested cases
        assert len(self.c.find({"e.$exists": True})) == 2
        assert len(self.c.find({"e.a.$exists": True})) == 1
        assert len(self.c.find({"e.a.$exists": False})) == len(self.c) - 1
        assert len(self.c.find({"e.a": {"$exists": True}})) == 1
        assert len(self.c.find({"e.a": {"$exists": False}})) == len(self.c) - 1
        assert len(self.c.find({"f.a.$exists": True})) == 1
        assert len(self.c.find({"f.a.$exists": False})) == len(self.c) - 1
        assert len(self.c.find({"f.a": {"$exists": True}})) == 1
        assert len(self.c.find({"f.a": {"$exists": False}})) == len(self.c) - 1

    def test_find_arithmetic_operators(self):
        assert len(self.c) == 0
        for expr, n in ARITHMETIC_EXPRESSIONS:
            assert len(self.c.find({"a": expr})) == 0
        self.c.update(ARITHMETIC_DOCS)
        assert len(self.c) == len(ARITHMETIC_DOCS)
        for expr, n in ARITHMETIC_EXPRESSIONS:
            assert len(self.c.find({"a": expr})) == n

    def test_find_near(self):
        assert len(self.c) == 0
        # find 0 items in empty collection
        assert len(self.c.find({"a": {"$near": [10]}})) == 0
        assert len(self.c.find({"a": {"$near": [10, 100]}})) == 0
        assert len(self.c.find({"a": {"$near": [10, 100, 100]}})) == 0
        assert len(self.c.find({"a": {"$near": (10)}})) == 0
        assert len(self.c.find({"a": {"$near": (10, 100)}})) == 0
        assert len(self.c.find({"a": {"$near": (10, 100, 100)}})) == 0
        self.c.update(ARITHMETIC_DOCS)
        assert len(self.c) == len(ARITHMETIC_DOCS)
        # test known cases with lists and tuples
        assert len(self.c.find({"a": {"$near": [10]}})) == 1
        assert len(self.c.find({"a": {"$near": (10)}})) == 1
        assert len(self.c.find({"a": {"$near": [10]}})) == len(
            self.c.find({"a": {"$near": (10)}})
        )
        assert len(self.c.find({"a": {"$near": [10]}})) == len(
            self.c.find({"a": {"$near": 10}})
        )
        assert len(self.c.find({"a": {"$near": [10, 0.5]}})) == 16
        assert len(self.c.find({"a": {"$near": (10, 0.5)}})) == 16
        assert len(self.c.find({"a": {"$near": [10, 0.5, 0.0]}})) == 16
        assert len(self.c.find({"a": {"$near": (10, 0.5, 0.0)}})) == 16
        # increasing abs_tol should increase # of jobs found
        assert len(self.c.find({"a": {"$near": [10, 0.5, 11]}})) > len(
            self.c.find({"a": {"$near": [10, 0.5]}})
        )
        assert len(self.c.find({"a": {"$near": [10.5, 0.005]}})) == 0
        assert len(self.c.find({"a": {"$near": (10.5, 0.005)}})) == 0
        # test with lists that are too long
        with pytest.raises(ValueError):
            self.c.find({"a": {"$near": [10, 0.5, 1, 1]}})
        with pytest.raises(ValueError):
            self.c.find({"a": {"$near": [10, 0.5, 1, 1, 5]}})
        with pytest.raises(ValueError):
            self.c.find({"a": {"$near": (10, 0.5, 1, 1)}})
        with pytest.raises(ValueError):
            self.c.find({"a": {"$near": (10, 0.5, 1, 1, 5)}})

    def test_find_array_operators(self):
        assert len(self.c) == 0
        for expr, n in ARRAY_EXPRESSIONS:
            assert len(self.c.find({"a": expr})) == 0
        self.c.update(ARITHMETIC_DOCS)
        assert len(self.c) == len(ARITHMETIC_DOCS)
        for expr, n in ARRAY_EXPRESSIONS:
            assert len(self.c.find({"a": expr})) == n

    def test_find_regular_expression(self):
        assert len(self.c) == 0
        assert len(self.c.find({"a": {"$regex": "foo"}})) == 0
        assert len(self.c.find({"a": {"$regex": "hello"}})) == 0
        docs = {"0": {"a": "hello world"}}
        self.c.update(docs)
        assert len(self.c.find({"a": {"$regex": "foo"}})) == 0
        assert len(self.c.find({"a": {"$regex": "hello"}})) == 1
        assert len(self.c.find({"a": {"$regex": "hello world"}})) == 1

    def test_find_type_expression(self):
        assert len(self.c) == 0
        types = [
            (1, "int"),
            (1.0, "float"),
            ("1.0", "str"),
            (True, "bool"),
            (None, "null"),
        ]
        for v, t in types:
            assert len(self.c.find({"a": {"$type": t}})) == 0
        for i, (v, t) in enumerate(types):
            self.c[str(i)] = {str(i): v}
        assert len(self.c) == len(types)
        for i, (v, t) in enumerate(types):
            assert len(self.c.find({str(i): {"$type": t}})) == 1

    def test_find_type_integer_values_identical_keys(self):
        docs = {"int": {"a": 1}, "float": {"a": 1.0}}
        self.c = _SearchIndexer(docs)
        assert len(self.c.find({"a": {"$type": "int"}})) == 1
        assert len(self.c.find({"a": {"$type": "float"}})) == 1

    def test_find_where_expression(self):
        assert len(self.c) == 0
        assert len(self.c.find({"a": {"$where": "lambda x: x < 42"}})) == 0
        self.c.update(ARITHMETIC_DOCS)
        assert len(self.c.find({"a": {"$where": "lambda x: x < 42"}})) == 42

    def test_find_logical_operators(self):
        assert len(self.c) == 0
        for expr, expectation in LOGICAL_EXPRESSIONS:
            if not isinstance(expectation, int):
                with pytest.raises(expectation):
                    self.c.find(expr)
            else:
                assert len(self.c.find(expr)) == 0
        self.c.update(ARITHMETIC_DOCS)
        assert len(self.c) == len(ARITHMETIC_DOCS)
        for expr, expectation in LOGICAL_EXPRESSIONS:
            if not isinstance(expectation, int):
                with pytest.raises(expectation):
                    self.c.find(expr)
            else:
                assert len(self.c.find(expr)) == expectation
                assert len(self.c.find({"$not": expr})) == N - expectation
                assert len(self.c.find({"$not": {"$not": expr}})) == expectation

    def test_find_invalid_syntax(self):
        self.c.update(ARITHMETIC_DOCS)
        assert len(self.c) == len(ARITHMETIC_DOCS)
        for expr, expectation in INVALID_SYNTAX_EXPRESSIONS:
            with pytest.raises(expectation):
                self.c.find(expr)
