"""This package contains the CompMatDB database implementation.

The computation materials database (CompMatDB) is a database system to support and encourage the collaboration in the context of computational and experimental data.
"""
import warnings

import gridfs

from ..core.config import load_config
from ..core.dbclient_connector import DBClientConnector

# namespace extension
from .conversion import DBMethod, BasicFormat, Adapter
from . import formats, methods

def connect(host = None, config = None):
    """Access the signac database:
        
    :param host: The mongoDB database backend host url, defaults to the configured host.
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
    if host is None:
        host = config.get('signacdb_host', config.get('compmatdb_host', config['database_host']))
    connector = DBClientConnector(config)
    connector.connect(host)
    connector.authenticate()
    db = connector.client[config['database_signacdb']]
    def gridfs_callback(project_id):
        return gridfs.GridFS(connector.client[project_id])
    return database.Database(db=db, get_gridfs=gridfs_callback, config=config)

def access_compmatdb(host = None, config = None):
    """Access the CompMatDB database:
        
    :param host: The mongoDB database backend host url, defaults to the configured host.
    :type host: str
    :param config: The signac configuration, defaults to the local environment configuration.
    :type config: A signac configuration object.

    .. warn:: This function is deprecated. Use: signac.db.connect()

    Access the database with:
        
        import signac
        db = signac.db.access_signacdb()

    To get more information on how to search and modify database entries, use:

        help(db)

    or visit:
    
        https://bitbucket.org/glotzer/signac/wiki/latest/signacdb
    """
    warnings.warn("This function is deprecated. Use signac.db.connect() instead.", DeprecationWarning)
    return connect(host=host, config=config)
