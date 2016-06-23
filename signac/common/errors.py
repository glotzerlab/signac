# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.


class Error(Exception):
    pass


class ConfigError(Error, RuntimeError):
    pass


class AuthenticationError(Error, RuntimeError):

    def __str__(self):
        if len(self.args) > 0:
            return "Failed to authenticate with host '{}'.".format(
                self.args[0])
        else:
            return "Failed to authenticate with host."
