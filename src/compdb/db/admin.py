import logging
logger = logging.getLogger(__name__)

from .database import PYMONGO_3

def _clear_cache(db, filter):
    if PYMONGO_3:
        db._cache.delete_many(filter)
    else:
        db._cache.remove(filter)

def clear_cache_for_method(db, method):
    from . database import callable_spec
    logger.debug("Clearing cache for method '{}'.".format(method))
    _clear_cache(db, callable_spec(method))

def clear_cache(db, filter = None):
    if filter is None:
        logger.debug("Clearing cache.")
        db._cache.drop()
    else:
        logger.debug("Clearing chache for '{}'.".format(filter))
        docs = db.find(filter, ['_id'])
        f = {'doc_id': {'$in': list(doc['_id'] for doc in docs)}}
        print(f)
        _clear_cache(db, f)

def data_backend(db):
    return db._data
