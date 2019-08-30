# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from deprecation import deprecated

from signac.db import __version__


@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The database package is deprecated.")
class SignacDeprecationWarning(UserWarning):
    """Indicates the deprecation of a signac feature, API or behavior.

    This class indicates a user-relevant deprecation and is therefore
    a UserWarning, not a DeprecationWarning which is hidden by default.
    """
    pass


__all__ = ['SignacDeprecationWarning']
