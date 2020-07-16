# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Contrib submodule containing Project class and indexing features."""

import logging

from . import indexing
from .project import Project
from .project import TemporaryProject
from .project import get_project, init_project, get_job
from .indexing import BaseCrawler
from .indexing import RegexFileCrawler
from .indexing import JSONCrawler
from .indexing import SignacProjectCrawler
from .indexing import MainCrawler
from .indexing import MasterCrawler
from .indexing import fetch
from .indexing import fetched
from .indexing import export_one
from .indexing import export
from .indexing import export_to_mirror
from .indexing import export_pymongo
from .indexing import index_files
from .indexing import index
from .collection import Collection

logger = logging.getLogger(__name__)


__all__ = [
    'indexing',
    'Project', 'TemporaryProject', 'get_project', 'init_project', 'get_job',
    'BaseCrawler', 'RegexFileCrawler', 'JSONCrawler', 'SignacProjectCrawler',
    'MainCrawler', 'MasterCrawler', 'fetch', 'fetched',
    'export_one', 'export', 'export_to_mirror', 'export_pymongo',
    'index_files', 'index',
    'Collection',
]


try:
    import mpi4py  # noqa
except ImportError:
    logger.debug("Failed to import mpi4py. MPIPool will not be available.")
else:
    from .mpipool import MPIPool  # noqa
    __all__.extend(['MPIPool'])
