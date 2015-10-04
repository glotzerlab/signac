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
