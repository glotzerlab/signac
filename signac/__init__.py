"""
signac aids in the management, access and analysis of large-scale
computational investigations.

The framework provides a simple data model, which helps to organize
data production and post-processing as well as distribution among collaboratos.
"""
from __future__ import absolute_import
from . import contrib
from . import db
from . import gui

__version__ = '0.2.1'

__all__ = ['__version__', 'contrib', 'db', 'gui']
