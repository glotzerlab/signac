# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define a framework for synchronized objects implementing the Collection interface.

Synchronization of standard Python data structures with a persistent data store is
important for a number of applications. While tools like `h5py` and `zarr` offer
dict-like interfaces to underlying files, these APIs serve to provide a familiar
wrapper around access patterns specific to these backends. Moreover, these formats
are primarily geared towards the provision of high-performance storage for large
array-like data. Storage of simpler data types, while possible, is generally
more difficult and requires additional work from the user.

Synced collections fills this gap, introducing a new abstract base class that extends
`collections.abc.Collection` to add transparent synchronization protocols. The package
implements its own versions of standard data structures like dicts and lists, and
it offers support for storing these data structures into various data formats. The
synchronization mechanism is completely transparent to the user; for example, a
`JSONDict` initialized pointing to a particular file can be modified like a normal
dict, and all changes will be automatically persisted to a JSON file.
"""

from .data_types import SyncedCollection, SyncedDict, SyncedList

__all__ = ["SyncedCollection", "SyncedDict", "SyncedList"]
