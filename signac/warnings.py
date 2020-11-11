# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.


"""Module for signac deprecation warnings."""


class SignacDeprecationWarning(UserWarning):
    """Indicates the deprecation of a signac feature, API or behavior.

    This class indicates a user-relevant deprecation and is therefore
    a UserWarning, not a DeprecationWarning which is hidden by default.
    """

    pass


__all__ = ["SignacDeprecationWarning"]
