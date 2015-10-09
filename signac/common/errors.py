class Error(Exception): pass

class ConfigError(Error, RuntimeError): pass
