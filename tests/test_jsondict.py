# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import uuid
from tempfile import TemporaryDirectory

import pytest

from signac import JSONDict
from signac.errors import InvalidKeyError, KeyTypeError

FN_DICT = "jsondict.json"


def testdata():
    return str(uuid.uuid4())


class TestJSONDictBase:
    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="jsondict_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self._fn_dict = os.path.join(self._tmp_dir.name, FN_DICT)


class TestJSONDict(TestJSONDictBase):
    def get_json_dict(self):
        return JSONDict(filename=self._fn_dict)

    def get_testdata(self):
        return str(uuid.uuid4())

    def test_init(self):
        self.get_json_dict()

    def test_set_get(self):
        jsd = self.get_json_dict()
        key = "setget"
        d = self.get_testdata()
        jsd.clear()
        assert not bool(jsd)
        assert len(jsd) == 0
        assert key not in jsd
        jsd[key] = d
        assert bool(jsd)
        assert len(jsd) == 1
        assert key in jsd
        assert jsd[key] == d
        assert jsd.get(key) == d

    def test_set_get_explicit_nested(self):
        jsd = self.get_json_dict()
        key = "setgetexplicitnested"
        d = self.get_testdata()
        jsd.setdefault("a", {})
        child1 = jsd["a"]
        child2 = jsd["a"]
        assert child1 == child2
        assert isinstance(child1, type(child2))
        assert id(child1) == id(child2)
        assert not child1
        assert not child2
        child1[key] = d
        assert child1
        assert child2
        assert key in child1
        assert key in child2
        assert child1 == child2
        assert child1[key] == d
        assert child2[key] == d

    def test_copy_value(self):
        jsd = self.get_json_dict()
        key = "copy_value"
        key2 = "copy_value2"
        d = self.get_testdata()
        assert key not in jsd
        assert key2 not in jsd
        jsd[key] = d
        assert key in jsd
        assert jsd[key] == d
        assert key2 not in jsd
        jsd[key2] = jsd[key]
        assert key in jsd
        assert jsd[key] == d
        assert key2 in jsd
        assert jsd[key2] == d

    def test_iter(self):
        jsd = self.get_json_dict()
        key1 = "iter1"
        key2 = "iter2"
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = {key1: d1, key2: d2}
        jsd.update(d)
        assert key1 in jsd
        assert key2 in jsd
        for i, key in enumerate(jsd):
            assert key in d
            assert d[key] == jsd[key]
        assert i == 1

    def test_delete(self):
        jsd = self.get_json_dict()
        key = "delete"
        d = self.get_testdata()
        jsd[key] = d
        assert len(jsd) == 1
        assert jsd[key] == d
        del jsd[key]
        assert len(jsd) == 0
        with pytest.raises(KeyError):
            jsd[key]
        jsd[key] = d
        assert len(jsd) == 1
        assert jsd[key] == d
        del jsd.delete
        assert len(jsd) == 0
        with pytest.raises(KeyError):
            jsd[key]

    def test_update(self):
        jsd = self.get_json_dict()
        key = "update"
        d = {key: self.get_testdata()}
        jsd.update(d)
        assert len(jsd) == 1
        assert jsd[key] == d[key]

    def test_clear(self):
        jsd = self.get_json_dict()
        key = "clear"
        d = self.get_testdata()
        jsd[key] = d
        assert len(jsd) == 1
        assert jsd[key] == d
        jsd.clear()
        assert len(jsd) == 0

    def test_reopen(self):
        jsd = self.get_json_dict()
        key = "reopen"
        d = self.get_testdata()
        jsd[key] = d
        del jsd  # possibly unsafe
        jsd2 = self.get_json_dict()
        assert len(jsd2) == 1
        assert jsd2[key] == d

    def test_copy_as_dict(self):
        jsd = self.get_json_dict()
        key = "copy"
        d = self.get_testdata()
        jsd[key] = d
        copy = dict(jsd)
        del jsd
        assert key in copy
        assert copy[key] == d

    def test_reopen2(self):
        jsd = self.get_json_dict()
        key = "reopen"
        d = self.get_testdata()
        jsd[key] = d
        del jsd  # possibly unsafe
        jsd2 = self.get_json_dict()
        assert len(jsd2) == 1
        assert jsd2[key] == d

    def test_write_invalid_type(self):
        class Foo:
            pass

        jsd = self.get_json_dict()
        key = "write_invalid_type"
        d = self.get_testdata()
        jsd[key] = d
        assert len(jsd) == 1
        assert jsd[key] == d
        d2 = Foo()
        with pytest.raises(TypeError):
            jsd[key + "2"] = d2
        assert len(jsd) == 1
        assert jsd[key] == d

    def test_buffered_read_write(self):
        jsd = self.get_json_dict()
        jsd2 = self.get_json_dict()
        assert jsd == jsd2
        key = "buffered_read_write"
        d = self.get_testdata()
        d2 = self.get_testdata()
        assert len(jsd) == 0
        assert len(jsd2) == 0
        with jsd.buffered:
            jsd[key] = d
            assert jsd[key] == d
            assert len(jsd) == 1
            assert len(jsd2) == 0
        assert len(jsd) == 1
        assert len(jsd2) == 1
        with jsd2.buffered:
            jsd2[key] = d2
            assert len(jsd) == 1
            assert jsd[key] == d
            assert jsd2[key] == d2
        assert jsd[key] == d2
        assert jsd2[key] == d2
        with jsd.buffered:
            del jsd[key]
            assert key not in jsd
        assert key not in jsd

    def test_keys_with_dots(self):
        jsd = self.get_json_dict()
        with pytest.raises(InvalidKeyError):
            jsd["a.b"] = None

    def test_keys_valid_type(self):
        jsd = self.get_json_dict()

        class MyStr(str):
            pass

        # Only strings are permitted as keys
        for key in ("key", MyStr("key")):
            d = jsd[key] = self.get_testdata()
            assert str(key) in jsd
            assert jsd[str(key)] == d

    def test_keys_invalid_type(self):
        jsd = self.get_json_dict()

        class A:
            pass

        for key in (1, True, False, None, 0.0, A(), (1, 2, 3)):
            with pytest.raises(KeyTypeError):
                jsd[key] = self.get_testdata()
        for key in ([], {}):
            with pytest.raises(TypeError):
                jsd[key] = self.get_testdata()


class TestJSONDictWriteConcern(TestJSONDict):
    def get_json_dict(self):
        return JSONDict(filename=self._fn_dict, write_concern=True)


class TestJSONDictNestedData(TestJSONDict):
    def get_testdata(self):
        return dict(a=super().get_testdata())


class TestJSONDictNestedDataWriteConcern(
    TestJSONDictNestedData, TestJSONDictWriteConcern
):

    pass
