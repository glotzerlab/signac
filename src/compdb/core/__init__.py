import logging
logger = logging.getLogger('core')

def valid_name(name):
    return not name.startswith('_compdb')

def _get_db(db_name):
    logger.warning("Deprecated.")
    from . config import read_config
    config = read_config()
    from pymongo import MongoClient
    client = MongoClient(config['database_host'])
    return client[db_name]

def get_db(db_name):
    logger.warning("Deprecated.")
    assert valid_name(db_name)
    return _get_db(db_name)
