import logging
import os
import datetime

import pymongo
import gridfs

from ..core.config import load_config
from ..core.dbclient_connector import DBClientConnector

# namespace extension
from .conversion import DBMethod, BasicFormat, Adapter
from . import formats, methods

logger = logging.getLogger(__name__)

def access_compmatdb(host = None, config = None):
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
