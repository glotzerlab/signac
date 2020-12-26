# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Contrib submodule containing Project class and indexing features."""

import logging

from . import indexing
from .collection import Collection
from .indexing import (
    BaseCrawler,
    JSONCrawler,
    MainCrawler,
    MasterCrawler,
    RegexFileCrawler,
    SignacProjectCrawler,
    export,
    export_one,
    export_pymongo,
    export_to_mirror,
    fetch,
    fetched,
    index,
    index_files,
)
from .project import Project, TemporaryProject, get_job, get_project, init_project

logger = logging.getLogger(__name__)


__all__ = [
    "indexing",
    "Project",
    "TemporaryProject",
    "get_project",
    "init_project",
    "get_job",
    "BaseCrawler",
    "RegexFileCrawler",
    "JSONCrawler",
    "SignacProjectCrawler",
    "MainCrawler",
    "MasterCrawler",
    "fetch",
    "fetched",
    "export_one",
    "export",
    "export_to_mirror",
    "export_pymongo",
    "index_files",
    "index",
    "Collection",
]


try:
    import mpi4py  # noqa
except ImportError:
    logger.debug("Failed to import mpi4py. MPIPool will not be available.")
else:
    from .mpipool import MPIPool  # noqa

    __all__.extend(["MPIPool"])
