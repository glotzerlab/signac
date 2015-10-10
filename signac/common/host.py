import logging

from .config import load_config
from .errors import ConfigError
from .connection import DBClientConnector

logger = logging.getLogger(__name__)


def get_host_config(hostname=None, config=None):
    if config is None:
        config = load_config()
    if hostname is None:
        try:
            hostname = config['General']['default_host']
        except KeyError:
            try:
                hostname = config['hosts'].keys()[0]
            except (KeyError, IndexError):
                raise ConfigError("No hosts specified.")
    try:
        return config['hosts'][hostname]
    except KeyError:
        raise ConfigError("Host '{}' not configured.".format(hostname))


def get_connector(hostname=None, config=None):
    return DBClientConnector(get_host_config(hostname=hostname, config=config))


def get_client(hostname=None, config=None):
    connector = get_connector(hostname=hostname, config=config)
    connector.connect()
    connector.authenticate()
    return connector.client


def get_db(name, hostname=None, config=None):
    client = get_client(hostname=hostname, config=config)
    return client[name]
