from ..common import host


def get_database(name, hostname=None, config=None):
    """Get a database handle.

    :param name: The name of the database to get.
    :type name: str
    :param hostname: The name of the configured host.
                     Defaults to the first configured host, or the
                     host specified by `default_host`.
    :type hostname: str
    :param config: The config object to retrieve the host
                   configuration from.
    :type config: :class:`.common.config.Config`
    :returns: The database handle.
    :rtype: :class:`pymongo.database.Database`
    """
    return host.get_database(name=name, hostname=hostname, config=config)
