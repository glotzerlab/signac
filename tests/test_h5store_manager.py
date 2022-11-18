# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import pickle
from tempfile import TemporaryDirectory

import pytest

from signac.h5store import H5StoreManager

try:
    import h5py  # noqa

    H5PY = True
except ImportError:
    H5PY = False


@pytest.mark.skipif(not H5PY, reason="test requires the h5py package")
class TestH5StoreManager:
    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="h5store_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self.store = H5StoreManager(prefix=self._tmp_dir.name)
        with open(os.path.join(self._tmp_dir.name, "other_file.txt"), "w") as file:
            file.write(r"blank\n")

    def test_repr(self):
        assert eval(repr(self.store)) == self.store

    def test_str(self):
        assert eval(str(self.store)) == self.store

    def test_set(self):
        assert len(self.store) == 0
        assert "test" not in self.store
        for value in ("", [], {}):
            with pytest.raises(ValueError):
                self.store["test"] = value
        for value in (True, 0, 0.0, 1, 1.0, None):
            with pytest.raises(TypeError):
                self.store["test"] = value
        for value in "abc":
            with pytest.raises(ValueError):
                self.store["test"] = value

        # Assigning a dictionary is the intended use case
        self.store["test"] = dict(foo=True)
        assert len(self.store) == 1
        assert "test" in self.store

    def test_set_iterable(self):
        assert len(self.store) == 0
        assert "test" not in self.store
        self.store["test"] = list(dict(foo=True).items())
        assert len(self.store) == 1
        assert "test" in self.store

    def test_set_get(self):
        assert len(self.store) == 0
        assert "test" not in self.store
        self.store["test"]["foo"] = "bar"
        assert "test" in self.store
        assert len(self.store) == 1
        assert "foo" in self.store["test"]

    def test_del(self):
        assert len(self.store) == 0
        assert "test" not in self.store
        self.store["test"]["foo"] = "bar"
        assert "test" in self.store
        assert len(self.store) == 1
        assert "foo" in self.store["test"]
        with pytest.raises(KeyError):
            del self.store["invalid"]
        del self.store["test"]
        assert len(self.store) == 0
        assert "test" not in self.store

    def test_iteration(self):
        keys = ["foo", "bar", "baz"]
        for key in keys:
            self.store[key] = dict(test=True)
        assert list(sorted(keys)) == list(sorted(self.store))
        assert list(sorted(keys)) == list(sorted(self.store.keys()))

    def test_contains(self):
        keys = ["foo", "bar", "baz"]
        for key in keys:
            assert key not in self.store
        for key in keys:
            self.store[key] = dict(test=True)
        for key in keys:
            assert key in self.store

    def test_pickle(self):
        assert pickle.loads(pickle.dumps(self.store)) == self.store
