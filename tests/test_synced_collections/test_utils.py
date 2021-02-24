# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import os
from collections.abc import Collection, MutableSequence

import pytest

from signac.synced_collections import SyncedList
from signac.synced_collections.backends.collection_json import JSONDict
from signac.synced_collections.numpy_utils import NumpyConversionWarning
from signac.synced_collections.utils import (
    AbstractTypeResolver,
    SyncedCollectionJSONEncoder,
)

try:
    import numpy

    NUMPY = True
except ImportError:
    NUMPY = False


def test_type_resolver():
    resolver = AbstractTypeResolver(
        {
            "dict": lambda obj: isinstance(obj, dict),
            "tuple": lambda obj: isinstance(obj, tuple),
            "str": lambda obj: isinstance(obj, str),
            "mutablesequence": lambda obj: isinstance(obj, MutableSequence),
            "collection": lambda obj: isinstance(obj, Collection),
            "set": lambda obj: isinstance(obj, set),
        }
    )

    assert resolver.get_type({}) == "dict"
    assert resolver.get_type((0, 1)) == "tuple"
    assert resolver.get_type("abc") == "str"
    assert resolver.get_type([]) == "mutablesequence"

    # Make sure that order matters; collection should be found before list.
    assert resolver.get_type(set()) == "collection"


def test_json_encoder(tmpdir):
    # Raw dictionaries should be encoded transparently.
    data = {"foo": 1, "bar": 2, "baz": 3}
    json_str_data = '{"foo": 1, "bar": 2, "baz": 3}'
    assert json.dumps(data) == json_str_data
    assert json.dumps(data, cls=SyncedCollectionJSONEncoder) == json_str_data
    assert json.dumps(data, cls=SyncedCollectionJSONEncoder) == json.dumps(data)

    fn = os.path.join(tmpdir, "test_json_encoding.json")
    synced_data = JSONDict(fn)
    synced_data.update(data)
    with pytest.raises(TypeError):
        json.dumps(synced_data)
    assert json.dumps(synced_data, cls=SyncedCollectionJSONEncoder) == json_str_data

    if NUMPY:
        # Test both scalar and array numpy types since they could have
        # different problems.
        array = numpy.array(3)
        with pytest.warns(NumpyConversionWarning):
            synced_data["foo"] = array
        assert isinstance(synced_data["foo"], int)

        array = numpy.random.rand(3)
        with pytest.warns(NumpyConversionWarning):
            synced_data["foo"] = array
        assert isinstance(synced_data["foo"], SyncedList)
        assert (
            json.loads(json.dumps(synced_data, cls=SyncedCollectionJSONEncoder))
            == synced_data()
        )
