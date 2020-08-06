# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements Zarr-backend.

This implements the Zarr-backend for SyncedCollection API by
implementing sync and load methods.
"""
from .synced_collection import SyncedCollection
from .syncedattrdict import SyncedAttrDict
from .synced_list import SyncedList


class ZarrCollection(SyncedCollection):
    """Implement sync and load using a Zarr backend."""

    backend = __name__  # type: ignore

    def __init__(self, name=None, store=None, **kwargs):
        import zarr
        import numcodecs  # zarr depends on numcodecs

        self._root = zarr.group(store=store)
        self._name = name
        self._object_codec = numcodecs.JSON()
        super().__init__(**kwargs)
        if (name is None) == (self._parent is None):
            raise ValueError(
                "Illegal argument combination, one of the two arguments, "
                "parent or name must be None, but not both.")

    def _load(self):
        """Load the data from a Radis-database."""
        try:
            dataset = self._root[self._name]
            data = dataset[0]
        except KeyError:
            data = None
        return data

    def _sync(self):
        """Write the data from Radis-database."""
        data = self.to_base()
        # Serialize data:
        dataset = self._root.require_dataset(
            self._name, overwrite=True, shape=1, dtype='object', object_codec=self._object_codec)
        dataset[0] = data


class ZarrDict(ZarrCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent Zarr-database.

    The ZarrDict inherits from :class:`~core.collection_api.ZarrCollection`
    and :class:`~core.syncedattrdict.SyncedAttrDict`.

    .. code-block:: python

        doc = ZarrDict('data')
        doc['foo'] = "bar"
        assert doc.foo == doc['foo'] == "bar"
        assert 'foo' in doc
        del doc['foo']

    .. code-block:: python

        >>> doc['foo'] = dict(bar=True)
        >>> doc
        {'foo': {'bar': True}}
        >>> doc.foo.bar = False
        {'foo': {'bar': False}}

    .. warning::

        While the ZarrDict object behaves like a dictionary, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a ZarrDict instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new ZarrDict
        instance: `new_dict = ZarrDict(old_dict.to_base())`.

    Parameters
    ----------
    name: str, optional
        The name of the  collection (Default value = None).
    data: mapping, optional
        The intial data pass to ZarrDict. Defaults to `dict()`.
    store: mapping
        A zarr store to synchronise the data
    parent: object, optional
        A parent instance of ZarrDict or None (Default value = None).
    """

    pass


class ZarrList(ZarrCollection, SyncedList):
    """A non-string sequence interface to a persistent Zarr file.

    The ZarrDict inherits from :class:`~core.collection_api.ZarrCollection`
    and :class:`~core.syncedlist.SyncedList`.

    .. code-block:: python

        synced_list = ZarrList('data')
        synced_list.append("bar")
        assert synced_list[0] == "bar"
        assert len(synced_list) == 1
        del synced_list[0]

    .. warning::

        While the ZarrList object behaves like a list, there are
        important distinctions to remember. In particular, because operations
        are reflected as changes to an underlying database, copying (even deep
        copying) a ZarrList instance may exhibit unexpected behavior. If a
        true copy is required, you should use the `to_base()` method to get a
        dictionary representation, and if necessary construct a new ZarrList
        instance: `new_list = ZarrList(old_list.to_base())`.

    Parameters
    ----------
    name: str
        The name of the  collection.
    data: mapping, optional
        The intial data pass to ZarrList. Defaults to `list()`.
    store: mapping
        A zarr store to synchronise the data
    parent: object, optional
        A parent instance of ZarrList or None (Default value = None).
    """

    pass


SyncedCollection.register(ZarrDict, ZarrList)
