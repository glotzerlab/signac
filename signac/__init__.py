# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The signac framework aids in the management of large and
heterogeneous data spaces.

It provides a simple and robust data model to create a
well-defined indexable storage layout for data and metadata.
This makes it easier to operate on large data spaces,
streamlines post-processing and analysis and makes data
collectively accessible."""

from __future__ import absolute_import
from . import contrib
from . import db
from . import gui
from .contrib import Project, get_project, fetch, fetch_one
from .db import get_database

__version__ = '0.3.0'

__all__ = ['__version__', 'contrib', 'db', 'gui',
           'Project', 'get_project', 'get_database', 'fetch', 'fetch_one']
