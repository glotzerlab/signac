# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Graphical User Interface (GUI) for configuration and database inspection.

The GUI is a leight-weight interface which makes the configuration
of the signac framework and data inspection more straight-forward."""
import warnings
try:
    import PySide  # noqa
    import pymongo  # noqa
except ImportError as error:
    msg = "{}. The signac gui is not available.".format(error)
    warnings.warn(msg, ImportWarning)

    def main():
        """Start signac-gui.

        The gui requires PySide and pymongo."""
        raise ImportError(msg)
else:
    from .gui import main

__all__ = ['main']
