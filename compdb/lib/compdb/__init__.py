from .config import CONFIG

def _get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient(CONFIG['database']['host'])
    return client[db_name]

