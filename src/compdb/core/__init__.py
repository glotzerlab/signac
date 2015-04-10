import logging
logger = logging.getLogger('core')

def valid_name(name):
    return not name.startswith('compdb')

def _get_db(db_name):
    logger.warning("Deprecated.")
    from . config import load_config
    config = load_config()
    from pymongo import MongoClient
    client = MongoClient(config['database_host'])
    return client[db_name]

def get_db(db_name):
    logger.warning("Deprecated.")
    assert valid_name(db_name)
    return _get_db(db_name)
