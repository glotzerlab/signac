class DatabaseError(BaseException):
    pass


class ConnectionFailure(RuntimeError):
    pass


class ConfigError(RuntimeError):
    pass
