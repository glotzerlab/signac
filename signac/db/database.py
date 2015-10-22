try:
    from ..common import host
except ImportError:
    def get_database(name, hostname=None, config=None):
        import pymongo  # noqa -- This will always fail.
else:
    def get_database(name, hostname=None, config=None):
        return host.get_database(name=name, hostname=hostname, config=config)
