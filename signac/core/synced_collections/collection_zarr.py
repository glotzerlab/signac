# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements Zarr-backend.

This implements the Zarr-backend for SyncedCollection API by
implementing sync and load methods.
"""
from copy import deepcopy

from .synced_collection import SyncedCollection
from .synced_attr_dict import SyncedAttrDict
from .synced_list import SyncedList


class ZarrCollection(SyncedCollection):
    """Implement sync and load using a Zarr backend."""

    _backend = __name__  # type: ignore

    def __init__(self, group=None, name=None, parent=None, **kwargs):
        # TODO: Give a clearer error if the import fails.
        import numcodecs  # zarr depends on numcodecs

        self._root = group
        self._object_codec = numcodecs.JSON()
        self._name = name
        super().__init__(parent=parent, **kwargs)

    def _load_from_resource(self):
        """Load the data from zarr-store."""
        try:
            return self._root[self._name][0]
        except KeyError:
            return None

    def _save_to_resource(self):
        """Write the data to zarr-store."""
        data = self.to_base()
        dataset = self._root.require_dataset(
            self._name, overwrite=True, shape=1, dtype='object', object_codec=self._object_codec)
        dataset[0] = data

    def __deepcopy__(self, memo):
        if self._parent is not None:
            # TODO: Do we really want a deep copy of a nested collection to
            # deep copy the parent? Perhaps we should simply disallow this?
            return type(self)(group=None, name=None, data=self.to_base(),
                              parent=deepcopy(self._parent, memo))
        else:
            return type(self)(group=deepcopy(self._root, memo), name=self._name, data=None,
                              parent=None)


class ZarrDict(ZarrCollection, SyncedAttrDict):
    """A dict-like mapping interface to a persistent Zarr-database.

    The ZarrDict inherits from :class:`~core.synced_collection.ZarrCollection`
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
    group: object, optional
        A zarr.hierarchy.Group instance (Default value = None).
    data: mapping, optional
        The intial data pass to ZarrDict. Defaults to `dict()`.
    name: str, optional
        The name of the collection (Default value = None).
    parent: object, optional
        A parent instance of ZarrDict or None (Default value = None).
    """
    def __init__(self, group=None, name=None, data=None, parent=None, *args, **kwargs):
        self._validate_constructor_args({'group': group, 'name': name}, data, parent)
        super().__init__(group=group, name=name, data=data, parent=parent,
                         *args, **kwargs)



class ZarrList(ZarrCollection, SyncedList):
    """A non-string sequence interface to a persistent Zarr file.

    The ZarrList inherits from :class:`~core.synced_collection.ZarrCollection`
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

    group: object, optional
        A zarr.hierarchy.Group instance (Default value = None).
    data: non-str Sequence, optional
        The intial data pass to ZarrList. Defaults to `list()`.
    name: str, optional
        The name of the  collection (Default value = None).
    parent: object, optional
        A parent instance of ZarrList or None (Default value = None).
    """
    def __init__(self, group=None, name=None, data=None, parent=None, *args, **kwargs):
        self._validate_constructor_args({'group': group, 'name': name}, data, parent)
        super().__init__(group=group, name=name, data=data, parent=parent,
                         *args, **kwargs)


SyncedCollection.register(ZarrDict, ZarrList)
