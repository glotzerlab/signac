# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import absolute_import
import logging

from . import formats
from . import indexing
from .project import Project, get_project, init_project
from .indexing import BaseCrawler
from .indexing import RegexFileCrawler
from .indexing import JSONCrawler
from .indexing import SignacProjectCrawler
from .indexing import MasterCrawler
from .indexing import fetch
from .indexing import fetch_one  # deprecated
from .indexing import fetched
from .indexing import export_one
from .indexing import export
from .indexing import export_to_mirror
from .indexing import export_pymongo

logger = logging.getLogger(__name__)


__all__ = [
    'formats', 'indexing',
    'Project', 'get_project', 'init_project',
    'BaseCrawler', 'RegexFileCrawler', 'JSONCrawler', 'SignacProjectCrawler',
    'MasterCrawler', 'fetch', 'fetch_one', 'fetched',
    'export_one', 'export', 'export_to_mirror', 'export_pymongo',
]


try:
    import mpi4py  # noqa
except ImportError:
    logger.debug("Failed to import mpi4py. MPIPool will not be available.")
else:
    from .mpipool import MPIPool  # noqa
    __all__.extend(['MPIPool'])
