# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the MIT License.
class DatabaseError(BaseException):
    pass


class ConnectionFailure(RuntimeError):
    pass


class ConfigError(RuntimeError):
    pass
