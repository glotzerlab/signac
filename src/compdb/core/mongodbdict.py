import logging
logger = logging.getLogger(__name__)

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

class ReadOnlyMongoDBDict(object):

    def __init__(self, collection, _id):
        #self._host = host
        #self._db_name = db_name
        #self._collection_name = collection_name
        #self._collection = None
        self._id = _id
        #self._connect_timeout_ms = connect_timeout_ms
        self._collection = collection
        logger.debug("Opening MongoDBDict")
        #msg = "Opened MongoDBDict '{}' on '{}'."
        #logger.debug(msg.format(_id, collection_name))

    def _get_collection(self):
        return self._collection

    def _spec(self):
        return {'_id': self._id}

    def __getitem__(self, key):
        logger.debug("Getting '{}'".format(key))
        if PYMONGO_3:
            doc = self._get_collection().find_one(
                filter = self._spec(),
                projection = [key])
        else:
            doc = self._get_collection().find_one(
                self._spec(),
                fields = [key])
        if doc is None:
            raise KeyError(key)
        else:
            return doc[key]
    
    def __iter__(self):
        doc = self._get_collection().find_one(self._spec())
        if doc is None:
            return
        else:
            for key in doc:
                if key == '_id':
                    continue
                else:
                    yield key

    def __contains__(self, key):
        if PYMONGO_3:
            doc = self._get_collection().find_one(
                filter = self._spec(),
                projection = [key])
        else:
            doc = self._get_collection().find_one(
                self._spec(),
                fields = [key])
        if doc is None:
            return False
        else:
            return key in doc

    def get(self, key, default = None):
        try:
            return self.__getitem__(key) 
        except KeyError:
            return default

class MongoDBDict(ReadOnlyMongoDBDict):

    def __setitem__(self, key, value):
        msg = "Setting '{}'."
        logger.debug(msg.format(key))
        if PYMONGO_3:
            self._get_collection().update_one(
                filter = self._spec(),
                update = {'$set': {key: value}},
                upsert = True)
        else:
            self._get_collection().update(
                spec = self._spec(),
                document = {'$set': {key: value}},
                upsert = True)

    def __delitem__(self, key):
        if PYMONGO_3:
            result = self._get_collection().update_one(
                filter = self._spec(),
                update = {'$unset': {key: ''}})
        else:
            result = self._get_collection().update(
                spec = self._spec(),
                document = {
                    '$unset': {key: ''}
                })
            assert result['ok']

    def clear(self):
        self._get_collection().save(self._spec())

    def remove(self):
        self._get_collection().remove(self._spec())
