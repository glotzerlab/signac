import logging
logger = logging.getLogger('mongodbdict')

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

class ReadOnlyMongoDBDict(object):

    def __init__(self, host, db_name, collection_name, _id, connect_timeout_ms = None):
        self._host = host
        self._db_name = db_name
        self._collection_name = collection_name
        self._collection = None
        self._id = _id
        self._connect_timeout_ms = connect_timeout_ms
        msg = "Opened MongoDBDict '{}' on '{}'."
        logger.debug(msg.format(_id, collection_name))

    def _get_collection(self):
        from pymongo import MongoClient
        if self._collection is None:
            msg = "Connecting MongoDBDict (timeout={})."
            logger.debug(msg.format(self._connect_timeout_ms))
            if self._connect_timeout_ms is None:
                client = MongoClient(self._host)
            else:
                client = MongoClient(
                    self._host,
                    connectTimeoutMS = self._connect_timeout_ms,
                    serverSelectionTimeoutMS = int(1.5 * self._connect_timeout_ms))
            self._collection = client[self._db_name][self._collection_name]
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
        assert doc is not None
        yield from doc

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
