# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The signac framework aids in the management of large and heterogeneous data spaces.

It provides a simple and robust data model to create a well-defined, indexable
storage layout for data and metadata. This makes it easier to operate on large
data spaces, streamlines post-processing and analysis, and makes data
collectively accessible.
"""

from . import cite, contrib, db, errors, sync, testing, warnings
from .contrib import (
    Collection,
    MainCrawler,
    MasterCrawler,
    Project,
    RegexFileCrawler,
    SignacProjectCrawler,
    TemporaryProject,
    export,
    export_one,
    export_pymongo,
    export_to_mirror,
    fetch,
)
from .contrib import filesystems as fs
from .contrib import get_job, get_project, index, index_files, init_project
from .core.h5store import H5Store, H5StoreManager
from .core.jsondict import JSONDict
from .core.jsondict import buffer_reads_writes as buffered
from .core.jsondict import flush_all as flush
from .core.jsondict import get_buffer_load, get_buffer_size
from .core.jsondict import in_buffered_mode as is_buffered
from .db import get_database
from .diff import diff_jobs
from .version import __version__

__all__ = [
    "__version__",
    "contrib",
    "db",
    "errors",
    "warnings",
    "sync",
    "cite",
    "Project",
    "TemporaryProject",
    "get_project",
    "init_project",
    "get_job",
    "diff_jobs",
    "get_database",
    "fetch",
    "export_one",
    "export",
    "export_to_mirror",
    "Collection",
    "export_pymongo",
    "fs",
    "index_files",
    "index",
    "RegexFileCrawler",
    "MainCrawler",
    "MasterCrawler",
    "SignacProjectCrawler",
    "buffered",
    "is_buffered",
    "flush",
    "get_buffer_size",
    "get_buffer_load",
    "JSONDict",
    "H5Store",
    "H5StoreManager",
    "testing",
]
