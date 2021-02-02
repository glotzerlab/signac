"""Set of utility functions for migration-related tasks."""

import os

from ..utility import _mkdir_p


def _migrate_internal_file(old_file, new_file):
    """Migrate files based on the 1.x names to the new 2.x schema.

    Various files used internally by signac are placed at the project root in
    signac 1.x but will be placed in a .signac directory in 2.x. This function
    provides a standardized migration protocol.

    Parameters
    ----------
    old_file : str
        The original filename.
    new_file : str
        The new filename.

    """
    if os.path.exists(old_file):
        _mkdir_p(os.path.dirname(new_file))
        os.rename(old_file, new_file)
        print("Moved {old_file}->{new_file}.")
