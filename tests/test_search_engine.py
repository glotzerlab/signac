# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest

from signac.core.search_engine import DocumentSearchEngine as DSE

TEST_INDEX = [
    {'a': 0},
    {'b': 1.0},
    {'a': 0, 'b': 1.0},
    {'a': 0, 'b': 1.0, 'c': 'abc'},
    {'a': 0, 'b': 1.0, 'c': 'abc', 'd': {'e': True}},
    {'a': 0, 'f': [0, 1]},
    {'a': 0, 'g': {'h': [0, 1.0, 'abc'], 'i': 'xyz'}},
    ]


@unittest.skip("refactored")
class DocumentSearchEngineTest(unittest.TestCase):

    def test_index(self):
        return [dict(_id=i, **doc) for i, doc in enumerate(TEST_INDEX)]

    def test_empty_constructor(self):
        self.assertEqual(len(DSE()), 0)

    def test_build_index(self):
        ti = self.test_index()
        e = DSE(ti)
        self.assertEqual(len(e), len(ti))

    def test_build_index_limited(self):
        ti = self.test_index()
        e = DSE(ti, include=None)
        self.assertEqual(len(e), len(ti))
        self.assertTrue(e._filter_supported({'b': 0}))
        e = DSE(ti, include={'a': True})
        self.assertFalse(e._filter_supported({'b': 0}))
        e = DSE(ti, include={'b': False})
        self.assertFalse(e._filter_supported({'b': 0}))
        e = DSE(ti, include={'d': True})
        self.assertFalse(e._filter_supported({'b': 0}))
        self.assertTrue(e._filter_supported({'d': {'e': 0}}))
        e = DSE(ti, include={'d': {'e': True}})
        self.assertTrue(e._filter_supported({'d': {'e': 0}}))
        e = DSE(ti, include={'d': {'e': False}})
        self.assertFalse(e._filter_supported({'d': {'e': 0}}))
        self.assertFalse(e._filter_supported({'d': {'f': 0}}))

    def test_find(self):
        ti = self.test_index()
        TI = TEST_INDEX
        e = DSE(ti, include=None)
        self.assertEqual(len(e), len(ti))
        QUERIES = [
            (TI[0], 6),
            (TI[1], 4),
            (TI[2], 3),
            (TI[3], 2),
            (TI[4], 1),
            (TI[5], 1)]
        for q, num in QUERIES:
            self.assertEqual(len(list(e.find(q))), num)
        QUERIES = [
            ({'a': 0}, {0, 2, 3, 4, 5, 6}),
            ({'a': 0, 'f': 0}, {}),
            ({'a': 1}, {}),
            ({'b': 1.0}, {1, 2, 3, 4}),
            ({'a': 1, 'b': 1.0}, {}),
            ({'a': None, 'b': 1.0}, {}),
            ({'c': 'abc'}, {3, 4}),
            ({'d': {'e': False}}, {}),
            ({'d': {'e': True}}, {4}),
            ({'a': 0, 'f': [0, 1]}, {5}),
            ({'f': [0, 1]}, {5}),
            ({'f': [0, 1, 2]}, {}),
            ({'f': [0, 1.0, 2]}, {}),
            ({'f': [0, 1.0]}, {}),
            ({'g': {'h': [0, 1.0, 'abc']}}, {6}),
            ({'g': {'h': [0, 1, 'abc']}}, {}),
            ({'g': {'i': 'xyz'}}, {6}),
        ]
        for q, result in QUERIES:
            self.assertEqual(set(e.find(q)), set(result))

    def test_illegal_filters(self):
        q_invalid = [{'a': [0, {'b': 1}]}]
        q_not_indexed = {'b': 0}
        ti = self.test_index()
        e = DSE(ti, include={'a': True})
        with self.assertRaises(ValueError):
            e.check_filter(q_invalid)
        with self.assertRaises(ValueError):
            list(e.find(q_invalid))
        with self.assertRaises(RuntimeError):
            e.check_filter(q_not_indexed)
        with self.assertRaises(RuntimeError):
            list(e.find(q_not_indexed))


if __name__ == '__main__':
    unittest.main()
