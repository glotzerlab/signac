# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the MIT License.


class Error(Exception):
    pass


class ConfigError(Error, RuntimeError):
    pass


class AuthenticationError(Error, RuntimeError):
    pass
