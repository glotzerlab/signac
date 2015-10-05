import logging

from .config import load_config
from .connection import DBClientConnector

logger = logging.getLogger(__name__)

def get_connector(hostname = None, config = None):
    if config is None:
        config = load_config()
    if hostname is None:
        hostname = config['General']['default_host']
    host_config = config['hosts'][hostname]
    return DBClientConnector(host_config)

def get_client(hostname = None, config = None):
    connector = get_connector(hostname=hostname, config=config)
    connector.connect()
    connector.authenticate()
    return connector.client

def get_db(name, hostname = None, config = None):
    client = get_client(hostname=hostname, config=config)
    return client[name]
