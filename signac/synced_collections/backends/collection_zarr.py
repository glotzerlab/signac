# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements a Zarr :class:`~.SyncedCollection` backend."""
from copy import deepcopy

from .. import SyncedCollection, SyncedDict, SyncedList
from ..validators import require_string_key

try:
    import numcodecs

    ZARR = True
except ImportError:
    ZARR = False


class ZarrCollection(SyncedCollection):
    r"""A :class:`~.SyncedCollection` that synchronizes with a Zarr group.

    Since Zarr is designed for storage of array-like data, this backend implements
    synchronization by storing the collection in a 1-element object array. The user
    provides the group within which to store the data and the name of the data in
    the group.

    **Thread safety**

    The :class:`ZarrCollection` is not thread-safe.

    Parameters
    ----------
    group : zarr.hierarchy.Group
        The Zarr group in which to store data.
    name : str
        The name under which this collection is stored in the Zarr group.
    codec : numcodecs.abc.Codec
        The encoding mechanism for the data. If not provided, defaults to JSON
        encoding (Default value: None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    """

    _backend = __name__  # type: ignore

    def __init__(self, group=None, name=None, codec=None, *args, **kwargs):
        if not ZARR:
            raise RuntimeError(
                "The Zarr package must be installed to use the ZarrCollection."
            )

        super().__init__(**kwargs)
        self._group = group
        self._name = name
        self._object_codec = numcodecs.JSON() if codec is None else codec

    def _load_from_resource(self):
        """Load the data from the Zarr group.

        Returns
        -------
        Collection or None
            An equivalent unsynced collection satisfying :meth:`~.is_base_type` that
            contains the data in the Zarr group. Will return None if associated
            data is not found in the Zarr group.

        """
        try:
            return self._group[self._name][0]
        except KeyError:
            return None

    def _save_to_resource(self):
        """Write the data to Zarr group."""
        data = self._to_base()
        dataset = self._group.require_dataset(
            self._name,
            overwrite=True,
            shape=1,
            dtype="object",
            object_codec=self._object_codec,
        )
        dataset[0] = data

    def __deepcopy__(self, memo):
        if self._root is not None:
            return type(self)(
                group=None,
                name=None,
                data=self._to_base(),
                parent=deepcopy(self._root, memo),
            )
        else:
            return type(self)(
                group=deepcopy(self._group, memo),
                name=self._name,
                data=None,
                parent=None,
            )

    @property
    def codec(self):
        """numcodecs.abc.Codec: The encoding method used for the data."""
        return self._object_codec

    @codec.setter
    def codec(self, new_codec):
        self._object_codec = new_codec

    @property
    def group(self):
        """zarr.hierarchy.Group: The Zarr group storing the data."""
        return self._group

    @property
    def name(self):
        """str: The name of this data in the Zarr group."""
        return self._name


class ZarrDict(ZarrCollection, SyncedDict):
    r"""A dict-like data structure that synchronizes with a Zarr group.

    Examples
    --------
    >>> doc = ZarrDict('data')
    >>> doc['foo'] = "bar"
    >>> assert doc['foo'] == "bar"
    >>> assert 'foo' in doc
    >>> del doc['foo']
    >>> doc['foo'] = dict(bar=True)
    >>> doc
    {'foo': {'bar': True}}

    Parameters
    ----------
    group : zarr.hierarchy.Group, optional
        The group in which to store data (Default value = None).
    name : str, optional
        The name of the collection (Default value = None).
    data : :class:`collections.abc.Mapping`, optional
        The initial data passed to :class:`ZarrDict`. If ``None``, defaults to
        ``{}`` (Default value = None).
    parent : ZarrCollection, optional
        A parent instance of :class:`ZarrCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`ZarrDict` object behaves like a :class:`dict`, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying database, copying (even deep copying) a
    :class:`ZarrDict` instance may exhibit unexpected behavior. If a true copy is
    required, you should use the call operator to get a dictionary
    representation, and if necessary construct a new :class:`ZarrDict` instance.

    """

    _validators = (require_string_key,)

    def __init__(self, group=None, name=None, data=None, parent=None, *args, **kwargs):
        super().__init__(
            group=group, name=name, data=data, parent=parent, *args, **kwargs
        )


class ZarrList(ZarrCollection, SyncedList):
    r"""A list-like data structure that synchronizes with a Zarr group.

    Only non-string sequences are supported by this class.

    Examples
    --------
    >>> synced_list = ZarrList('data')
    >>> synced_list.append("bar")
    >>> assert synced_list[0] == "bar"
    >>> assert len(synced_list) == 1
    >>> del synced_list[0]

    Parameters
    ----------
    group : zarr.hierarchy.Group, optional
        The group in which to store data (Default value = None).
    name : str, optional
        The name of the collection (Default value = None).
    data : non-str :class:`collections.abc.Sequence`, optional
        The initial data passed to :class:`ZarrList`. If ``None``, defaults to
        ``[]`` (Default value = None).
    parent : ZarrCollection, optional
        A parent instance of :class:`ZarrCollection` or ``None``. If ``None``,
        the collection owns its own data (Default value = None).
    \*args :
        Positional arguments forwarded to parent constructors.
    \*\*kwargs :
        Keyword arguments forwarded to parent constructors.

    Warnings
    --------
    While the :class:`ZarrList` object behaves like a :class:`list`, there are important
    distinctions to remember. In particular, because operations are reflected
    as changes to an underlying database, copying (even deep copying) a
    :class:`ZarrList` instance may exhibit unexpected behavior. If a true copy is
    required, you should use the call operator to get a dictionary
    representation, and if necessary construct a new :class:`ZarrList` instance.

    """

    def __init__(self, group=None, name=None, data=None, parent=None, *args, **kwargs):
        super().__init__(
            group=group, name=name, data=data, parent=parent, *args, **kwargs
        )
