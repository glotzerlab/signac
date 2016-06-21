# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from ..common import host


def get_database(name, hostname=None, config=None):
    """Get a database handle.

    The database handle is an instance of :class:`~pymongo.database.Database`,
    which provides access to the document collections within one database.

    .. code::

        db = signac.db.get_database('MyDatabase')
        docs = db.my_collection.find()

    Please note, that a collection which did not exist at the point of access,
    will automatically be created.

    :param name: The name of the database to get.
    :type name: str
    :param hostname: The name of the configured host.
                     Defaults to the first configured host, or the
                     host specified by `default_host`.
    :type hostname: str
    :param config: The config object to retrieve the host
                   configuration from.
                   Defaults to the global configuration.
    :type config: :class:`.common.config.Config`
    :returns: The database handle.
    :rtype: :class:`pymongo.database.Database`

    .. seealso:: https://api.mongodb.org/python/current/api/pymongo/database.html
    """
    return host.get_database(name=name, hostname=hostname, config=config)
