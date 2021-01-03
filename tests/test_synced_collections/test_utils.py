# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import os
from collections.abc import Collection, MutableSequence
from tempfile import TemporaryDirectory

import pytest

from signac.core.synced_collections.collection_json import JSONDict
from signac.core.synced_collections.synced_list import SyncedList
from signac.core.synced_collections.utils import AbstractTypeResolver, SCJSONEncoder

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


def test_json_encoder():
    def encode_flat_dict(d):
        """A limited JSON-encoding method for a flat dict or SyncedDict of ints."""
        if hasattr(d, "_data"):
            d = d._data

        return "{" + ", ".join(f'"{k}": {v}' for k, v in d.items()) + "}"

    # Raw dictionaries should be encoded transparently.
    data = {"foo": 1, "bar": 2, "baz": 3}
    assert json.dumps(data) == encode_flat_dict(data)
    assert json.dumps(data, cls=SCJSONEncoder) == json.dumps(data)

    with TemporaryDirectory() as tmp_dir:
        fn = os.path.join(tmp_dir, "test_json_encoding.json")
        synced_data = JSONDict(fn)
        synced_data.update(data)
        with pytest.raises(TypeError):
            json.dumps(synced_data)
        assert json.dumps(synced_data, cls=SCJSONEncoder) == encode_flat_dict(
            synced_data
        )

        if NUMPY:
            array = numpy.random.rand(3)
            synced_data["foo"] = array
            assert isinstance(synced_data["foo"], SyncedList)
            assert (
                json.loads(json.dumps(synced_data, cls=SCJSONEncoder)) == synced_data()
            )
