"""This package contains the CompMatDB database implementation.

The computation materials database (CompMatDB) is a database system to support and encourage the collaboration in the context of computational and experimental data.
"""
import gridfs

from ..core.config import load_config
from ..core.dbclient_connector import DBClientConnector

# namespace extension
from .conversion import DBMethod, BasicFormat, Adapter
from . import formats, methods

def access_compmatdb(host = None, config = None):
    """Access the CompMatDB database:
        
    :param host: The mongoDB database backend host url, defaults to the configured host.
    :type host: str
    :param config: The compdb configuration, defaults to the local environment configuration.
    :type config: A compdb configuration object.

    Access the database with:
        
        import compdb
        db = compdb.db.access_compmatdb()

    To get more information on how to search and modify database entries, use:

        help(db)

    or visit:
    
        https://bitbucket.org/glotzer/compdb/wiki/latest/compmatdb
    """
    # local import to load modules only if required
    from . import database
    if config is None:
        config = load_config()
    if host is None:
        host = config['compmatdb_host']
    connector = DBClientConnector(config)
    connector.connect(host)
    connector.authenticate()
    db = connector.client[config['database_compmatdb']]
    def gridfs_callback(project_id):
        return gridfs.GridFS(connector.client[project_id])
    return database.Database(db=db, get_gridfs=gridfs_callback, config=config)
