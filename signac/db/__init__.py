# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging
import warnings

try:
    import pymongo  # noqa
except ImportError:
    warnings.warn(
        "Failed to import pymongo. " "get_database will not be available.",
        ImportWarning,
    )

    def get_database(*args, **kwargs):
        """Get a database handle.

        This function is only available if pymongo is installed."""
        raise ImportError("You need to install pymongo to use `get_database()`.")


else:
    if pymongo.version_tuple[0] < 3:
        logging.getLogger(__name__).warn(
            "Your pymongo installation (version {}) is no longer "
            "supported by signac. Consider updating.".format(pymongo.version)
        )
    from .database import get_database


__all__ = ["get_database"]
