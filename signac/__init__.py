# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The signac framework aids in the management of large and heterogeneous data spaces.

It provides a simple and robust data model to create a well-defined, indexable
storage layout for data and metadata. This makes it easier to operate on large
data spaces, streamlines post-processing and analysis, and makes data
collectively accessible.
"""

from synced_collections.backends.collection_json import BufferedJSONAttrDict as JSONDict

from . import errors, sync
from .diff import diff_jobs
from .h5store import H5Store, H5StoreManager
from .project import Project, TemporaryProject, get_job, get_project, init_project
from .version import __version__

# Alias some properties related to buffering into the signac namespace.
buffered = JSONDict.buffer_backend
is_buffered = JSONDict.backend_is_buffered
get_current_buffer_size = JSONDict.get_current_buffer_size
get_buffer_capacity = JSONDict.get_buffer_capacity
set_buffer_capacity = JSONDict.set_buffer_capacity

__all__ = [
    "__version__",
    "errors",
    "sync",
    "Project",
    "TemporaryProject",
    "get_project",
    "init_project",
    "get_job",
    "diff_jobs",
    "buffered",
    "is_buffered",
    "get_buffer_capacity",
    "get_current_buffer_size",
    "set_buffer_capacity",
    "JSONDict",
    "H5Store",
    "H5StoreManager",
]
