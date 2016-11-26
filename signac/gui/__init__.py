# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Graphical User Interface (GUI) for configuration and database inspection.

The GUI is a leight-weight interface which makes the configuration
of the signac framework and data inspection more straight-forward."""
import logging

logger = logging.getLogger(__name__)

try:
    import PySide  # noqa
    import pymongo  # noqa
except Exception as error:
    msg = 'The signac gui is not available, because of an error: "{}".'
    logger.debug(msg.format(error))

    def main():
        """Start signac-gui.

        The gui requires PySide and pymongo."""
        import PySide  # noqa
        import pymongo  # noqa
else:
    from .gui import main

__all__ = ['main']
