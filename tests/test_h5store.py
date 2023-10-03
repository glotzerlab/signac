# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import platform
import random
import string
import subprocess
import sys
from array import array
from collections.abc import Mapping
from contextlib import closing, contextmanager
from functools import partial
from itertools import chain
from multiprocessing.pool import ThreadPool
from platform import python_implementation
from tempfile import TemporaryDirectory
from time import time

import pytest

from signac.errors import InvalidKeyError
from signac.h5store import H5Store, H5StoreAlreadyOpenError, H5StoreClosedError

PYPY = "PyPy" in platform.python_implementation()

try:
    import h5py  # noqa

    H5PY = True
except ImportError:
    H5PY = False

try:
    import pandas  # noqa
    import tables  # noqa

    PANDAS_AND_TABLES = True
except ImportError:
    PANDAS_AND_TABLES = False

try:
    import numpy  # noqa

    NUMPY = True
except ImportError:
    NUMPY = False

FN_STORE = "signac_test_h5store.h5"


WINDOWS = sys.platform == "win32"


@pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
@pytest.mark.skipif(PYPY, reason="h5py not reliable on PyPy platform")
class TestH5StoreBase:
    @pytest.fixture(autouse=True)
    def setUp_base_h5Store(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_test_h5store_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self._fn_store = os.path.join(self._tmp_dir.name, FN_STORE)
        self._fn_store_other = os.path.join(self._tmp_dir.name, "other_" + FN_STORE)

    def get_h5store(self, **kwargs):
        return H5Store(filename=self._fn_store, **kwargs)

    @contextmanager
    def open_h5store(self, **kwargs):
        with self.get_h5store(**kwargs) as h5s:
            yield h5s

    def get_other_h5store(self, **kwargs):
        return H5Store(filename=self._fn_store_other, **kwargs)

    @contextmanager
    def open_other_h5store(self, **kwargs):
        with self.get_other_h5store(**kwargs) as h5s:
            yield h5s

    def get_testdata(self, size=None):
        if size is None:
            size = 1024
        return "".join(random.choice(string.ascii_lowercase) for i in range(size))

    def assertEqual(self, a, b):
        if hasattr(a, "shape"):
            if not NUMPY:
                raise pytest.skip("This test requires the numpy package.")
            numpy.testing.assert_array_equal(a, b)
        else:
            assert a == b


class TestH5StoreOpen(TestH5StoreBase):
    def test_open(self):
        h5s = self.get_h5store()
        h5s.open()
        h5s.close()

    def test_open_read_only(self):
        with self.open_h5store() as h5s:
            h5s["foo"] = "bar"

        with self.open_h5store(mode="r") as h5s:
            assert h5s.mode == "r"
            assert "foo" in h5s
            self.assertEqual(h5s["foo"], "bar")

    def test_open_write_only(self):
        with self.open_h5store(mode="w") as h5s:
            assert h5s.mode == "r+"
            h5s["foo"] = "bar"
            assert "foo" in h5s
            self.assertEqual(h5s["foo"], "bar")

    def test_open_write_and_read_only(self):
        with self.open_h5store(mode="w") as h5s_w:
            with self.open_h5store(mode="r") as h5s_r:
                assert h5s_w.mode == "r+"
                assert h5s_r.mode == "r+"
                assert "foo" not in h5s_r
                assert "foo" not in h5s_w

                h5s_w["foo"] = "bar"
                assert "foo" in h5s_r
                self.assertEqual(h5s_r["foo"], "bar")
                assert "foo" in h5s_w
                self.assertEqual(h5s_r["foo"], "bar")


class TestH5Store(TestH5StoreBase):
    valid_types = {
        "int": 123,
        "float": 123.456,
        "string": "lorem ipsum including unicõdé 🎨",
        "none": None,
        "float_array": array("f", [-1.5, 0, 1.5]),
        "double_array": array("d", [-1.5, 0, 1.5]),
        "int_array": array("i", [-1, 0, 1]),
        "uint_array": array("I", [0, 1, 2]),
        "dict": {"a": 1, "b": None, "c": "test"},
        "bytes": b"\x73\x69\x67\x6E\x61\x63\x00\xF0\x9F\x8E\xA8",
    }

    if NUMPY:
        valid_types.update(
            {
                "numpy_int_array": numpy.array([-1, 0, 1], dtype=numpy.int_),
                "numpy_float_array": numpy.array([-1.5, 0, 1.5], dtype=numpy.float_),
                "numpy_complex_array": numpy.array(
                    [-1.5 + 3.14j, 0, 1.5 - 5.67j], dtype=numpy.complex_
                ),
                # Note that NumPy's string type is handled kind of like a char
                # array or bytes, not like Python str. numpy.unicode_ uses a
                # four byte encoding and is explicitly not supported by h5py.
                "numpy_string_array": numpy.array(
                    [
                        b"abcde",
                        b"\x73\x69\x67\x6E\x61\x63\x00\xF0\x9F\x8E\xA8",
                    ],
                    dtype=numpy.string_,
                ),
                "numpy_void_array": numpy.array(
                    [
                        b"abcdefghijk",
                        b"\x73\x69\x67\x6E\x61\x63\x00\xF0\x9F\x8E\xA8",
                    ],
                    dtype="V",
                ),
            }
        )

    def test_init(self):
        self.get_h5store()

    def test_invalid_filenames(self):
        with pytest.raises(ValueError):
            H5Store(None)
        with pytest.raises(ValueError):
            H5Store("")
        with pytest.raises(ValueError):
            H5Store(123)

    def test_set_get(self):
        with self.open_h5store() as h5s:
            key = "setget"
            d = self.get_testdata()
            h5s.clear()
            assert not bool(h5s)
            assert len(h5s) == 0
            assert key not in h5s
            with pytest.raises(KeyError):
                h5s[key]
            d_ = h5s[key] = d
            self.assertEqual(d_, d)
            assert bool(h5s)
            assert len(h5s) == 1
            assert key in h5s
            self.assertEqual(h5s[key], d)
            self.assertEqual(h5s.get(key), d)
            assert h5s.get("nonexistent", "default") == "default"

    def test_set_get_explicit_nested(self):
        with self.open_h5store() as h5s:
            key = "setgetexplicitnested"
            d = self.get_testdata()
            assert "a" not in h5s
            ret = h5s.setdefault("a", dict())
            assert "a" in h5s
            self.assertEqual(ret, h5s["a"])
            assert hasattr(ret, "_store")  # is an H5Group object
            child1 = h5s["a"]
            child2 = h5s["a"]
            self.assertEqual(child1, child2)
            assert type(child1) is type(child2)
            assert not child1
            assert not child2
            child1[key] = d
            assert child1
            assert child2
            assert key in child1
            assert key in child2
            self.assertEqual(child1, child2)
            self.assertEqual(child1[key], d)
            self.assertEqual(child2[key], d)

    def test_setdefault_group(self):
        with self.open_h5store() as h5s:
            group_key = "setdefault_group"
            key = "setdefault"
            d = self.get_testdata()
            assert group_key not in h5s
            h5s[group_key] = {}
            assert group_key in h5s
            assert len(h5s[group_key]) == 0
            assert key not in h5s[group_key]
            h5s[group_key].setdefault(key, d)
            assert len(h5s[group_key]) == 1
            assert key in h5s[group_key]
            self.assertEqual(h5s[group_key][key], d)
            d2 = self.get_testdata()
            h5s[group_key].setdefault(key, d2)
            self.assertEqual(h5s[group_key][key], d)

    def test_repr(self):
        with self.open_h5store() as h5s:
            key = "test_repr"
            assert repr(h5s) == repr(eval(repr(h5s)))
            h5s[key] = self.get_testdata()
        assert repr(h5s) == repr(eval(repr(h5s)))

    def test_str(self):
        with self.open_h5store() as h5s:
            key = "test_repr"
            h5s[key] = self.get_testdata()
            str(h5s)  # open
        str(h5s)  # closed

    def test_len(self):
        h5s = self.get_h5store()
        assert len(h5s) == 0
        h5s["test_len"] = True
        assert len(h5s) == 1

    def test_contains(self):
        h5s = self.get_h5store()
        assert "test_contains" not in h5s
        h5s["test_contains"] = True
        assert "test_contains" in h5s

    def test_copy_value(self):
        with self.open_h5store() as h5s:
            key = "copy_value"
            key2 = "copy_value2"
            d = self.get_testdata()
            h5s[key] = d
            assert key in h5s
            self.assertEqual(h5s[key], d)
            assert key2 not in h5s
            h5s[key2] = h5s[key]
            assert key in h5s
            self.assertEqual(h5s[key], d)
            assert key2 in h5s
            self.assertEqual(h5s[key2], d)

    def test_iter(self):
        with self.open_h5store() as h5s:
            key1 = "iter1"
            key2 = "iter2"
            d1 = self.get_testdata()
            d2 = self.get_testdata()
            d = {key1: d1, key2: d2}
            h5s.update(d)
            assert key1 in h5s
            assert key2 in h5s
            for i, key in enumerate(h5s):
                assert key in d
                self.assertEqual(d[key], h5s[key])
            assert i == 1

    def test_delete(self):
        with self.open_h5store() as h5s:
            key = "delete"
            d = self.get_testdata()
            h5s[key] = d
            assert len(h5s) == 1
            self.assertEqual(h5s[key], d)
            del h5s[key]
            assert len(h5s) == 0
            with pytest.raises(KeyError):
                h5s[key]

    def test_delete_attr(self):
        with self.open_h5store() as h5s:
            key = "delete"
            d = self.get_testdata()
            h5s[key] = d
            assert len(h5s) == 1
            self.assertEqual(h5s[key], d)
            delattr(h5s, key)
            assert len(h5s) == 0
            with pytest.raises(KeyError):
                h5s[key]

    def test_delete_nested(self):
        with self.open_h5store() as h5s:
            key1 = "group_key"
            key2 = "delete"
            d = self.get_testdata()
            h5s[key1] = {key2: d}
            assert len(h5s) == 1
            assert len(h5s[key1]) == 1
            self.assertEqual(h5s[key1][key2], d)
            del h5s[key1][key2]
            assert len(h5s) == 1
            assert len(h5s[key1]) == 0
            self.assertEqual(h5s[key1], {})
            with pytest.raises(KeyError):
                h5s[key1][key2]

    def test_update(self):
        with self.open_h5store() as h5s:
            key = "update"
            d = {key: self.get_testdata()}
            h5s.update(d)
            assert len(h5s) == 1
            self.assertEqual(h5s[key], d[key])

    def test_clear(self):
        with self.open_h5store() as h5s:
            h5s.clear()
            key = "clear"
            d = self.get_testdata()
            h5s[key] = d
            assert len(h5s) == 1
            self.assertEqual(h5s[key], d)
            h5s.clear()
            assert len(h5s) == 0

    def test_reopen(self):
        with self.open_h5store() as h5s:
            key = "reopen"
            d = self.get_testdata()
            h5s[key] = d
        with self.open_h5store() as h5s:
            assert len(h5s) == 1
            self.assertEqual(h5s[key], d)

    def test_open_twice(self):
        h5s = self.get_h5store()
        h5s.open()
        try:
            with pytest.raises(H5StoreAlreadyOpenError):
                h5s.open()
        finally:
            h5s.close()

    def test_open_reentry(self):
        with self.open_h5store() as h5s:
            with h5s:
                pass

    def test_reopen_explicit_open_close(self):
        h5s = self.get_h5store().open()
        key = "reopen"
        d = self.get_testdata()
        h5s[key] = d
        h5s.close()
        h5s.open()
        assert len(h5s) == 1
        self.assertEqual(h5s[key], d)
        h5s.close()

    def test_write_valid_types(self):
        with self.open_h5store() as h5s:
            for k, v in self.valid_types.items():
                h5s[k] = v
                self.assertEqual(h5s[k], v)

    def test_assign_valid_types_within_identical_file(self):
        with self.open_h5store() as h5s:
            for k, v in self.valid_types.items():
                h5s[k] = v
                self.assertEqual(h5s[k], v)
                h5s[k] = h5s[k]
                self.assertEqual(h5s[k], v)

                k_other = k + "-other"
                h5s[k_other] = h5s[k]
                self.assertEqual(h5s[k], v)
                self.assertEqual(h5s[k_other], v)
                self.assertEqual(h5s[k], h5s[k_other])

    def test_assign_valid_types_within_same_file(self):
        with self.open_h5store() as h5s:
            with self.open_h5store() as same_h5s:
                for k, v in self.valid_types.items():
                    # First, store v under k
                    h5s[k] = v
                    self.assertEqual(h5s[k], v)

                    # Assign the same value under the same key.
                    try:
                        same_h5s[k] = h5s[k]
                    except H5StoreClosedError:
                        pass
                    self.assertEqual(h5s[k], v)
                    self.assertEqual(same_h5s[k], v)
                    self.assertEqual(h5s[k], same_h5s[k])

                    # Assign the same value, under a different key.
                    other_key = k + "-other"
                    try:
                        same_h5s[other_key] = h5s[k]
                    except H5StoreClosedError:
                        pass
                    self.assertEqual(h5s[other_key], v)
                    self.assertEqual(same_h5s[other_key], v)
                    self.assertEqual(h5s[k], same_h5s[other_key])

                    # Deleting the value assigned to the alternative key should have
                    # no effect on the value stored under the original key, regardless
                    # whether it was copied by reference (hard-link) or copied by
                    # value (true copy).
                    del same_h5s[other_key]
                    self.assertEqual(h5s[k], v)
                    self.assertEqual(same_h5s[k], v)
                    self.assertEqual(same_h5s[k], h5s[k])

    def test_assign_valid_types_between_files(self):
        with self.open_h5store() as h5s:
            with self.open_other_h5store() as other_h5s:
                for k, v in self.valid_types.items():
                    h5s[k] = v
                    self.assertEqual(h5s[k], v)
                    try:
                        other_h5s[k] = h5s[k]
                    except (OSError, RuntimeError) as error:
                        # Type of error may depend on platform or software versions
                        assert (
                            str(error)
                            == "Unable to create link (interfile hard links are not allowed)"
                        )
                        assert isinstance(v, (array, numpy.ndarray))
                        other_h5s[k] = h5s[k][()]
                    self.assertEqual(h5s[k], v)
                    self.assertEqual(other_h5s[k], v)
                    self.assertEqual(other_h5s[k], h5s[k])

    def test_write_invalid_type(self):
        class Foo:
            pass

        with self.open_h5store() as h5s:
            key = "write_invalid_type"
            d = self.get_testdata()
            h5s[key] = d
            assert len(h5s) == 1
            self.assertEqual(h5s[key], d)
            d2 = Foo()
            with pytest.raises(TypeError):
                h5s[key + "2"] = d2
            assert len(h5s) == 1
            self.assertEqual(h5s[key], d)

    def test_keys_with_dots(self):
        with pytest.raises(InvalidKeyError):
            with self.open_h5store() as h5s:
                key = "a.b"
                d = self.get_testdata()
                h5s[key] = d
                self.assertEqual(h5s[key], d)

    def test_keys_with_slashes(self):
        # HDF5 uses slashes for nested keys internally
        with self.open_h5store() as h5s:
            key = "a/b"
            d = self.get_testdata()
            h5s[key] = d
            self.assertEqual(h5s[key], d)
            self.assertEqual(h5s["a"]["b"], d)

    def test_value_none(self):
        with self.get_h5store() as h5s:
            key = "a"
            d = None
            h5s[key] = d
            self.assertEqual(h5s[key], d)

    def test_set_get_attr_sync(self):
        with self.get_h5store() as h5s:
            assert len(h5s) == 0
            assert "a" not in h5s
            with pytest.raises(AttributeError):
                h5s.a
            a = 0
            h5s.a = a
            assert len(h5s) == 1
            assert "a" in h5s
            self.assertEqual(h5s.a, a)
            self.assertEqual(h5s["a"], a)
            a = 1
            h5s.a = a
            assert len(h5s) == 1
            assert "a" in h5s
            self.assertEqual(h5s.a, a)
            self.assertEqual(h5s["a"], a)

            def check_nested(a, b):
                assert len(h5s) == 1
                assert len(h5s.a) == 1
                assert "a" in h5s
                assert "b" in h5s.a
                self.assertEqual(h5s.a, a)
                self.assertEqual(h5s["a"]["b"], b)
                self.assertEqual(h5s.a.b, b)
                self.assertEqual(h5s["a"], a)

            h5s.a = {"b": 0}
            check_nested({"b": 0}, 0)
            h5s.a.b = 1
            check_nested({"b": 1}, 1)
            h5s["a"] = {"b": 2}
            check_nested({"b": 2}, 2)
            h5s["a"]["b"] = 3
            check_nested({"b": 3}, 3)

    def test_modify_nested(self):
        with self.get_h5store() as h5s:
            h5s.a = dict(b=True)
            a = h5s.a
            a["b"] = False
            assert not h5s.a["b"]

    def test_invalid_attr(self):
        h5s = self.get_h5store()
        with pytest.raises(AttributeError):
            h5s.a
        with pytest.raises(AttributeError):
            h5s._a
        with pytest.raises(AttributeError):
            h5s.__a__

    def test_attr_reference_modification(self):
        with self.get_h5store() as h5s:
            assert len(h5s) == 0
            assert "a" not in h5s
            with pytest.raises(AttributeError):
                h5s.a
            pairs = [(0, 1), (0.0, 1.0), ("0", "1"), (False, True)]
            dict_pairs = [(dict(c=a), dict(c=b)) for a, b in pairs]
            for A, B in chain(pairs, dict_pairs):
                h5s.a = A
                a = h5s.a
                self.assertEqual(a, A)
                self.assertEqual(h5s.a, A)
                a = B
                self.assertEqual(a, B)
                self.assertEqual(h5s.a, A)
                a = h5s["a"]
                self.assertEqual(a, A)
                self.assertEqual(h5s.a, A)
                a = B
                self.assertEqual(a, B)
                self.assertEqual(h5s.a, A)

                # with nested values
                h5s["a"] = dict(b=A)
                self.assertEqual(h5s.a.b, A)
                b = h5s.a.b
                self.assertEqual(b, A)
                self.assertEqual(h5s.a.b, A)
                b = B
                self.assertEqual(b, B)
                self.assertEqual(h5s.a.b, A)
                b = h5s["a"]["b"]
                self.assertEqual(b, A)
                self.assertEqual(h5s.a.b, A)
                b = B
                self.assertEqual(b, B)
                self.assertEqual(h5s.a.b, A)
                b = h5s["a"].b
                self.assertEqual(b, A)
                self.assertEqual(h5s.a.b, A)
                b = B
                self.assertEqual(b, B)
                self.assertEqual(h5s.a.b, A)


class TestH5StoreNestedData(TestH5Store):
    def get_testdata(self, size=None):
        return dict(a=super().get_testdata(size))

    def test_repr(self):
        from signac.h5store import H5Group, H5Store  # noqa:F401

        with self.open_h5store() as h5s:
            key = "test_repr"
            assert repr(h5s) == repr(eval(repr(h5s)))
            h5s[key] = self.get_testdata()
            assert repr(h5s[key]) == repr(eval(repr(h5s[key])))
        assert repr(h5s) == repr(eval(repr(h5s)))


class TestH5StoreBytesData(TestH5Store):
    def get_testdata(self, size=None):
        return super().get_testdata(size=size).encode()


class TestH5StoreClosed(TestH5Store):
    valid_types = {
        "int": 123,
        "float": 123.456,
        "string": "foobar",
        "none": None,
        "dict": {"a": 1, "b": None, "c": "test"},
    }

    @contextmanager
    def open_h5store(self, **kwargs):
        yield self.get_h5store(**kwargs)


class TestH5StoreNestedDataClosed(TestH5StoreNestedData, TestH5StoreClosed):
    pass


@pytest.mark.skipif(not PANDAS_AND_TABLES, reason="requires pandas and pytables")
@pytest.mark.skipif(not NUMPY, reason="requires numpy package")
class TestH5StorePandasData(TestH5Store):
    def get_testdata(self, size=None):
        if size is None:
            size = 1024
        return pandas.DataFrame(
            numpy.random.rand(8, size),
            index=[string.ascii_letters[i] for i in range(8)],
        )

    def assertEqual(self, a, b):
        if isinstance(a, Mapping):
            assert isinstance(b, Mapping)
            super().assertEqual(a.keys(), b.keys())
            for key in a:
                self.assertEqual(a[key], b[key])
        else:
            try:
                return (a == b).all()
            except (AttributeError, ValueError):
                return super().assertEqual(a, b)
            else:
                assert isinstance(a, pandas.DataFrame)


@pytest.mark.skipif(not PANDAS_AND_TABLES, reason="requires pandas and pytables")
@pytest.mark.skipif(not NUMPY, reason="requires numpy package")
class TestH5StoreNestedPandasData(TestH5StorePandasData):
    def get_testdata(self, size=None):
        if size is None:
            size = 1024
        return dict(
            df=pandas.DataFrame(
                numpy.random.rand(8, size),
                index=[string.ascii_letters[i] for i in range(8)],
            )
        )


class TestH5StoreMultiThreading(TestH5StoreBase):
    @pytest.mark.skip(
        reason="This test fails randomly on CI. "
        "See https://github.com/glotzerlab/signac/pull/307"
    )
    def test_multithreading(self):
        def set_x(x):
            self.get_h5store()["x"] = x

        with closing(ThreadPool(2)) as pool:
            pool.map(set_x, range(100))
        pool.join()

        assert self.get_h5store()["x"] in set(range(100))

    @pytest.mark.skip(
        reason="This test fails randomly on CI. "
        "See https://github.com/glotzerlab/signac/pull/307"
    )
    def test_multithreading_with_error(self):
        def set_x(x):
            self.get_h5store()["x"] = x
            if x == 50:
                raise RuntimeError()

        with pytest.raises(RuntimeError):
            with closing(ThreadPool(2)) as pool:
                pool.map(set_x, range(100))
        pool.join()

        assert self.get_h5store()["x"] in set(range(100))


def _read_from_h5store(filename, **kwargs):
    with H5Store(filename, **kwargs) as h5s:
        list(h5s)


class TestH5StoreMultiProcessing(TestH5StoreBase):
    def test_single_writer_multiple_reader_same_process(self):
        with self.open_h5store() as writer:
            with self.open_h5store():  # second writer
                with self.open_h5store(mode="r") as reader1:
                    with self.open_h5store(mode="r") as reader2:
                        writer["test"] = True
                        assert writer["test"]
                        assert reader1["test"]
                        assert reader2["test"]

    @pytest.mark.skipif(
        python_implementation() != "CPython", reason="SWMR mode not available."
    )
    def test_single_writer_multiple_reader_same_instance(self):
        from multiprocessing import Process

        def read():
            p = Process(
                target=_read_from_h5store,
                args=(self._fn_store,),
                kwargs=(dict(mode="r", swmr=True, libver="latest")),
            )
            p.start()
            p.join()
            # Ensure the process succeeded
            assert p.exitcode == 0

        with self.open_h5store(libver="latest") as writer:
            writer.file.swmr_mode = True
            read()
            writer["test"] = True
            read()

    def test_multiple_reader_different_process_no_swmr(self):
        read_cmd = (
            r'python -c "from signac.h5store import H5Store; '
            r"h5s = H5Store({}, mode=\"r\"); list(h5s); "
            r'h5s.close()"'
        ).format(repr(self._fn_store))

        with self.open_h5store():
            pass  # create file

        try:
            with self.open_h5store(mode="r"):  # single reader
                subprocess.check_output(read_cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            print("\n", error.output.decode(), file=sys.stderr)
            raise

    def test_single_writer_multiple_reader_different_process_no_swmr(self):
        read_cmd = (
            r'python -c "from signac.h5store import H5Store; '
            r"h5s = H5Store({}, mode=\"r\"); list(h5s); "
            r'h5s.close()"'
        ).format(repr(self._fn_store))

        with self.open_h5store():  # single writer
            with pytest.raises(subprocess.CalledProcessError):
                subprocess.check_output(read_cmd, shell=True, stderr=subprocess.DEVNULL)

    @pytest.mark.skipif(
        python_implementation() != "CPython", reason="SWMR mode not available."
    )
    def test_single_writer_multiple_reader_different_process_swmr(self):
        read_cmd = (
            r'python -c "from signac.h5store import H5Store; '
            r"h5s = H5Store({}, mode=\"r\", swmr=True); list(h5s); "
            r'h5s.close()"'
        ).format(repr(self._fn_store))

        with self.open_h5store(libver="latest") as writer:
            with pytest.raises(subprocess.CalledProcessError):
                subprocess.check_output(read_cmd, shell=True, stderr=subprocess.DEVNULL)

        try:
            with self.open_h5store(libver="latest") as writer:
                writer.file.swmr_mode = True
                subprocess.check_output(read_cmd, shell=True, stderr=subprocess.STDOUT)
                writer["test"] = True
                subprocess.check_output(read_cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            print("\n", error.output.decode(), file=sys.stderr)
            raise


@pytest.mark.skipif(not NUMPY, reason="requires numpy package")
@pytest.mark.skipif(
    python_implementation() != "CPython", reason="Optimized for CPython."
)
class TestH5StorePerformance(TestH5StoreBase):
    max_slowdown_vs_native_factor = 1.25

    @pytest.fixture
    def setUp(self, setUp_base_h5Store):
        value = self.get_testdata()
        times = numpy.zeros(200)
        for i in range(len(times)):
            start = time()
            with h5py.File(self._fn_store, mode="a") as h5file:
                if i:
                    del h5file["_baseline"]
                h5file.create_dataset("_baseline", data=value, shape=None)
            times[i] = time() - start
        self.baseline_time = times

    def assertSpeed(self, times):
        msg = "\n{:>10}\t{:>8}\t{:>8}\t{:>4}\n".format(
            "", "Measurement", "Benchmark", "Factor"
        )

        def format_row(text, reducer):
            return "{:<10}\t{:.2e}\t{:.2e}\t{:.3}\n".format(
                text,
                reducer(times),
                reducer(self.baseline_time),
                reducer(times) / reducer(self.baseline_time),
            )

        msg += format_row("mean", numpy.mean)
        msg += format_row("median", numpy.median)
        msg += format_row("25 percentile", partial(numpy.percentile, q=25))
        msg += format_row("75 percentile", partial(numpy.percentile, q=75))
        assert (
            numpy.percentile(times, 25) / numpy.percentile(self.baseline_time, 75)
            < self.max_slowdown_vs_native_factor
        ), msg

    @pytest.mark.skipif(
        WINDOWS, reason="This test fails for an unknown reason on Windows."
    )
    @pytest.mark.skip(
        reason="This test fails randomly on CI. "
        "See https://github.com/glotzerlab/signac/pull/307"
    )
    def test_speed_get(self, setUp):
        times = numpy.zeros(200)
        key = "test_speed_get"
        value = self.get_testdata()
        self.get_h5store()[key] = value
        self.assertEqual(self.get_h5store()[key], value)  # sanity check
        for i in range(len(times)):
            start = time()
            self.get_h5store()[key]
            times[i] = time() - start
        self.assertSpeed(times)

    @pytest.mark.skipif(
        WINDOWS, reason="This test fails for an unknown reason on Windows."
    )
    @pytest.mark.skip(
        reason="This test fails randomly on CI. "
        "See https://github.com/glotzerlab/signac/pull/307"
    )
    def test_speed_set(self, setUp):
        times = numpy.zeros(200)
        key = "test_speed_set"
        value = self.get_testdata()
        for i in range(len(times)):
            start = time()
            self.get_h5store()[key] = value
            times[i] = time() - start
        self.assertEqual(self.get_h5store()[key], value)  # sanity check
        self.assertSpeed(times)


class TestH5StorePerformanceNestedData(TestH5StorePerformance):
    max_slowdown_vs_native_factor = 1.75

    def get_testdata(self, size=None):
        return dict(a=super().get_testdata(size))

    @pytest.fixture
    def setUp(self, setUp_base_h5Store):
        value = TestH5StorePerformance.get_testdata(self)
        times = numpy.zeros(200)
        for i in range(len(times)):
            start = time()
            with h5py.File(self._fn_store, mode="a") as h5file:
                if i:
                    del h5file["_basegroup"]
                h5file.create_group("_basegroup").create_dataset(
                    "_baseline", data=value, shape=None
                )
            times[i] = time() - start
        self.baseline_time = times
