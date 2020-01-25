# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import uuid
from copy import copy, deepcopy
from itertools import chain
from collections.abc import MutableMapping

from signac.core.attrdict import SyncedAttrDict as SAD
from signac.core.synceddict import _SyncedDict
import pytest


class _SyncPoint(object):

    def __init__(self):
        self.reset()

    def reset(self):
        self._loaded = 0
        self._saved = 0

    @property
    def loaded(self):
        ret = self._loaded
        self._loaded = 0
        return ret

    @property
    def saved(self):
        ret = self._saved
        self._saved = 0
        return ret

    def load(self):
        self._loaded += 1

    def save(self):
        self._saved += 1


class TestSyncedAttrDict():

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.sync_point = _SyncPoint()

    def get_testdata(self):
        return str(uuid.uuid4())

    def get_sad(self, initialdata=None):
        return SAD(initialdata, parent=self.sync_point)

    def assert_no_read_write(self):
        assert self.sync_point.loaded == 0
        assert self.sync_point.saved == 0

    def assert_only_read(self, num=1):
        assert self.sync_point.loaded == num
        assert self.sync_point.saved == 0

    def assert_only_write(self, num=1):
        assert self.sync_point.loaded == 0
        assert self.sync_point.saved == num

    def assert_read_write(self, num_read=1, num_write=1):
        assert self.sync_point.loaded == num_read
        assert self.sync_point.saved == num_write

    def test_init(self):
        SAD()
        SAD(dict(a=0))
        self.get_sad()

    def test_is_object_and_mapping(self):
        assert isinstance(_SyncedDict(), object)
        assert isinstance(_SyncedDict(), MutableMapping)
        assert isinstance(self.get_sad(), _SyncedDict)

    def test_str(self):
        sad = self.get_sad()
        assert str(sad) == str(dict(sad()))
        sad['a'] = 0
        assert str(sad) == str(dict(sad()))
        sad['a'] = {'b': 0}
        assert str(sad) == str(dict(sad()))

    def test_repr(self):
        sad = self.get_sad()
        assert repr(sad) == repr(dict(sad()))
        sad['a'] = 0
        assert repr(sad) == repr(dict(sad()))
        sad['a'] = {'b': 0}
        assert repr(sad) == repr(dict(sad()))

    def test_call(self):
        sad = self.get_sad()
        sad['a'] = 0
        assert sad == dict(a=0)
        assert sad() == dict(a=0)

    def test_set_get(self):
        sad = self.get_sad()
        key = 'setget'
        d = self.get_testdata()
        assert not bool(sad)
        assert len(sad) == 0
        assert key not in sad
        assert not (key in sad)
        sad[key] = d
        assert bool(sad)
        assert len(sad) == 1
        assert key in sad
        assert key in sad
        assert sad[key] == d
        assert sad.get(key) == d

    def test_set_get_explicit_nested(self):
        sad = self.get_sad()
        key = 'setgetexplicitnested'
        d = self.get_testdata()
        assert not bool(sad)
        assert len(sad) == 0
        assert key not in sad
        assert not (key in sad)
        sad.setdefault('a', dict())
        child1 = sad['a']
        child2 = sad['a']
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
        sad = self.get_sad()
        key = 'copy_value'
        key2 = 'copy_value2'
        d = self.get_testdata()
        assert key not in sad
        assert key2 not in sad
        sad[key] = d
        assert key in sad
        assert sad[key] == d
        assert key2 not in sad
        sad[key2] = sad[key]
        assert key in sad
        assert sad[key] == d
        assert key2 in sad
        assert sad[key2] == d

    def test_copy_as_dict_sync(self):
        sad = self.get_sad()
        self.assert_no_read_write()
        sad['a'] = {'b': 0, 'c': [0]}
        self.assert_read_write()
        sad2 = copy(sad)
        sad3 = deepcopy(sad)
        sad4 = sad()
        assert sad == sad2
        self.assert_only_read(3)
        assert sad == sad3
        self.assert_only_read(1)
        assert sad == sad4
        self.assert_only_read(1)
        sad.a.b = 1
        self.assert_read_write(2, 1)
        assert sad.a.b == 1
        self.assert_only_read(2)
        assert sad2.a.b == 1
        self.assert_only_read(2)
        assert sad3.a.b == 0
        self.assert_only_read(0)
        sad.a.c[0] = 1
        self.assert_read_write(3, 1)
        assert sad.a.c[0] == 1
        self.assert_only_read(3)
        assert sad2.a.c[0] == 1
        self.assert_only_read(3)
        assert sad3.a.c[0] == 0
        self.assert_only_read(0)
        assert sad4['a']['c'][0] == 0
        self.assert_only_read(0)

    def test_iter(self):
        sad = self.get_sad()
        key1 = 'iter1'
        key2 = 'iter2'
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = {key1: d1, key2: d2}
        sad.update(d)
        assert key1 in sad
        assert key2 in sad
        for i, key in enumerate(sad):
            assert key in d
            assert d[key] == sad[key]
        assert i == 1

    def test_delete(self):
        sad = self.get_sad()
        key = 'delete'
        d = self.get_testdata()
        sad[key] = d
        assert len(sad) == 1
        assert sad[key] == d
        del sad[key]
        assert len(sad) == 0
        with pytest.raises(KeyError):
            sad[key]

    def test_update(self):
        sad = self.get_sad()
        key = 'update'
        d = {key: self.get_testdata()}
        sad.update(d)
        assert len(sad) == 1
        assert sad[key] == d[key]

    def test_clear(self):
        sad = self.get_sad()
        key = 'clear'
        d = self.get_testdata()
        sad[key] = d
        assert len(sad) == 1
        assert sad[key] == d
        sad.clear()
        assert len(sad) == 0

    def test_copy_as_dict(self):
        sad = self.get_sad()
        key = 'copy'
        d = self.get_testdata()
        sad[key] = d
        copy = dict(sad)
        assert copy == sad
        assert copy == sad()
        del sad
        assert key in copy
        assert copy[key] == d

    def test_set_get_sync(self):
        sad = self.get_sad()
        self.assert_no_read_write()
        key = 'setget'
        d = self.get_testdata()
        assert not bool(sad)
        self.assert_only_read()
        assert len(sad) == 0
        self.assert_only_read()
        assert key not in sad
        self.assert_only_read()
        assert not (key in sad)
        self.assert_only_read()
        sad[key] = d
        self.assert_read_write()

    def test_iter_sync(self):
        sad = self.get_sad()
        self.assert_no_read_write()
        key1 = 'iter1'
        key2 = 'iter2'
        d1 = self.get_testdata()
        d2 = self.get_testdata()
        d = {key1: d1, key2: d2}
        sad.update(d)
        self.assert_read_write()
        assert key1 in sad
        self.assert_only_read()
        assert key2 in sad
        self.assert_only_read()
        for i, key in enumerate(sad):
            assert key in d
            assert d[key] == sad[key]
        assert i == 1
        self.assert_only_read(3)

    def test_delete_sync(self):
        sad = self.get_sad()
        key = 'delete'
        d = self.get_testdata()
        sad[key] = d
        self.assert_read_write()
        assert len(sad) == 1
        self.assert_only_read()
        assert sad[key] == d
        self.assert_only_read()
        del sad[key]
        self.assert_read_write()
        assert len(sad) == 0
        self.assert_only_read()
        with pytest.raises(KeyError):
            sad[key]
            self.assert_only_read()

    def test_update_sync(self):
        sad = self.get_sad()
        key = 'update'
        d = {key: self.get_testdata()}
        sad.update(d)
        self.assert_read_write()
        assert len(sad) == 1
        self.assert_only_read()
        assert sad[key] == d[key]
        self.assert_only_read()

    def test_clear_sync(self):
        sad = self.get_sad()
        key = 'clear'
        d = self.get_testdata()
        sad[key] = d
        self.assert_read_write()
        sad.clear()
        self.assert_only_write()
        assert len(sad) == 0
        self.assert_only_read()

    def test_copy(self):
        sad = self.get_sad()
        key = 'copy'
        d = self.get_testdata()
        sad[key] = d
        self.assert_read_write()
        copy = dict(sad)
        self.assert_only_read(2)
        assert copy == sad
        self.assert_only_read()
        assert copy == sad()
        self.assert_only_read()
        del sad
        assert key in copy
        assert copy[key] == d

    def test_set_get_attr_sync(self):
        sad = self.get_sad()
        assert len(sad) == 0
        self.assert_only_read()
        assert 'a' not in sad
        self.assert_only_read()
        with pytest.raises(AttributeError):
            sad.a
        self.assert_only_read()
        a = 0
        sad.a = a
        self.assert_read_write()
        assert len(sad) == 1
        self.assert_only_read()
        assert 'a' in sad
        self.assert_only_read()
        assert sad.a == a
        self.assert_only_read()
        assert sad['a'] == a
        self.assert_only_read()
        assert sad()['a'] == a
        self.assert_only_read()
        a = 1
        sad.a = a
        self.assert_read_write()
        assert len(sad) == 1
        self.assert_only_read()
        assert 'a' in sad
        self.assert_only_read()
        assert sad.a == a
        self.assert_only_read()
        assert sad['a'] == a
        self.assert_only_read()
        assert sad()['a'] == a
        self.assert_only_read()

        def check_nested(a, b):
            assert len(sad) == 1
            self.assert_only_read()
            assert len(sad.a) == 1
            self.assert_only_read(2)
            assert 'a' in sad
            self.assert_only_read()
            assert 'b' in sad.a
            self.assert_only_read(2)
            assert sad.a == a
            self.assert_only_read(2)
            assert sad['a']['b'] == b
            self.assert_only_read(2)
            assert sad.a.b == b
            self.assert_only_read(2)
            assert sad.a() == a
            self.assert_only_read(2)
            assert sad['a'] == a
            self.assert_only_read(2)
            assert sad()['a'] == a
            self.assert_only_read(1)
            assert sad()['a']['b'] == b
            self.assert_only_read(1)
            assert sad['a']()['b'] == b
            self.assert_only_read(2)

        sad.a = {'b': 0}
        self.assert_read_write()
        check_nested({'b': 0}, 0)
        sad.a.b = 1
        self.assert_read_write(2, 1)
        check_nested({'b': 1}, 1)
        sad['a'] = {'b': 2}
        self.assert_read_write()
        check_nested({'b': 2}, 2)
        sad['a']['b'] = 3
        self.assert_read_write(2, 1)
        check_nested({'b': 3}, 3)

    def test_attr_reference_modification(self):
        sad = self.get_sad()
        assert len(sad) == 0
        assert 'a' not in sad
        with pytest.raises(AttributeError):
            sad.a
        pairs = [(0, 1), (0.0, 1.0), ('0', '1'), (False, True)]
        dict_pairs = [(dict(c=a), dict(c=b)) for a, b in pairs]
        for A, B in chain(pairs, dict_pairs):
            sad.a = A
            a = sad.a
            assert a == A
            assert sad.a == A
            a = B
            assert a == B
            assert sad.a == A
            a = sad['a']
            assert a == A
            assert sad.a == A
            a = B
            assert a == B
            assert sad.a == A

            # with nested values
            sad['a'] = dict(b=A)
            assert sad.a.b == A
            b = sad.a.b
            assert b == A
            assert sad.a.b == A
            b = B
            assert b == B
            assert sad.a.b == A
            b = sad['a']['b']
            assert b == A
            assert sad.a.b == A
            b = B
            assert b == B
            assert sad.a.b == A
            b = sad['a'].b
            assert b == A
            assert sad.a.b == A
            b = B
            assert b == B
            assert sad.a.b == A

    def test_list_modification(self):
        sad = self.get_sad()
        sad['a'] = [1, 2, 3]
        self.assert_read_write()
        assert len(sad.a) == 3
        self.assert_only_read()
        assert sad['a'] == [1, 2, 3]
        self.assert_only_read()
        assert sad.a == [1, 2, 3]
        self.assert_only_read()
        sad['a'].append(4)
        self.assert_read_write(2, 1)
        assert len(sad.a) == 4
        self.assert_only_read()
        assert sad['a'] == [1, 2, 3, 4]
        self.assert_only_read()
        assert sad.a == [1, 2, 3, 4]
        self.assert_only_read()
        sad.a.insert(0, 0)
        self.assert_read_write(2, 1)
        assert len(sad.a) == 5
        self.assert_only_read()
        assert sad['a'] == [0, 1, 2, 3, 4]
        self.assert_only_read()
        assert sad.a == [0, 1, 2, 3, 4]
        self.assert_only_read()
        del sad.a[0]
        self.assert_read_write(2)
        assert len(sad.a) == 4
        self.assert_only_read()
        assert sad.a.pop() is not None
        self.assert_read_write(2)
        assert len(sad.a) == 3
        self.assert_only_read()

    def test_suspend_sync(self):
        sad = self.get_sad()
        assert len(sad) == 0
        self.assert_only_read()
        with sad._suspend_sync():
            sad['a'] = 0
            assert len(sad) == 1
        self.assert_only_read(0)
        assert len(sad) == 1
        self.assert_only_read()
        with sad._suspend_sync():
            with sad._suspend_sync():
                sad['a'] = 1
                assert len(sad) == 1
        self.assert_only_read(0)
        assert len(sad) == 1
        self.assert_only_read()

    def test_nested_types_dict_conversion(self):
        """Ensure that calling methods like items and values does not
        change the type of nested dictionaries."""
        sad = self.get_sad({'a': {'b': 1}})
        assert type(sad['a']) is SAD
        sad.items()
        assert type(sad['a']) is SAD
        sad.values()
        assert type(sad['a']) is SAD
        sad._as_dict()
        assert type(sad['a']) is SAD


