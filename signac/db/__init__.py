"""This package contains the signac.db database implementation.

signac.db is designed to support and encourage the collaboration in the context of computational and experimental data.
"""
import warnings

import gridfs

from ..common.config import load_config
from ..common.host import get_client

# namespace extension
from ..contrib.conversion import DBMethod, BasicFormat, Adapter
from ..contrib import formats, methods


def connect(host=None, config=None):
    """Access the signac database:

    :param host: The mongoDB database backend host name, defaults to the configured default host.
    :type host: str
    :param config: The signac configuration, defaults to the local environment configuration.
    :type config: A signac configuration object.

    Access the database with:

        import signac
        db = signac.db.connect()

    .. seealso:: To get more information on how to search and modify database entries, use help(db) or visit https://bitbucket.org/glotzer/signac/wiki/latest/signacdb
    """
    # local import to load modules only if required
    from . import database
    if config is None:
        config = load_config()
    client = get_client(hostname=host, config=config)
    db = client[config['signacdb']['database']]

    def gridfs_callback(project_id):
        return gridfs.GridFS(client[project_id])
    return database.Database(db=db, get_gridfs=gridfs_callback, config=config)


def access_compmatdb(host=None, config=None):
    """Access the signac.db database:

    :param host: The mongoDB database backend host url, defaults to the configured host.
    :type host: str
    :param config: The signac configuration, defaults to the local environment configuration.
    :type config: A signac configuration object.

    .. warn:: This function is deprecated. Use: signac.db.connect()

    Access the database with:

        import signac
        db = signac.db.connect()

    To get more information on how to search and modify database entries, use:

        help(db)

    or visit:

        https://bitbucket.org/glotzer/signac/wiki/latest/signacdb
    """
    warnings.warn(
        "This function is deprecated. Use signac.db.connect() instead.", DeprecationWarning)
    return connect(host=host, config=config)
