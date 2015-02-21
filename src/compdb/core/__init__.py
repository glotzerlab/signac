from .config import CONFIG

def valid_name(name):
    return not name.startswith('_compdb')

def _get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient(CONFIG['database']['host'])
    return client[db_name]

def get_db(db_name):
    assert valid_name(db_name)
    return _get_db(db_name)
