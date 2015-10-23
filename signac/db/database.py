from ..common import host

def get_database(name, hostname=None, config=None):
    return host.get_database(name=name, hostname=hostname, config=config)
