import logging
logger = logging.getLogger('mongodbdict')
logger.addHandler(logging.NullHandler)

class ReadOnlyMongoDBDict(object):

    def __init__(self, host, db_name, collection_name, _id):
        self._host = host
        self._db_name = db_name
        self._collection_name = collection_name
        self._collection = None
        self._id = _id
        msg = "Opened MongoDBDict '{}' on '{}'."
        logger.debug(msg.format(_id, collection_name))

    def _get_collection(self):
        from pymongo import MongoClient
        if self._collection is None:
            client = MongoClient(self._host)
            self._collection = client[self._db_name][self._collection_name]
        return self._collection

    def _spec(self):
        return {'_id': self._id}

    def __getitem__(self, key):
        doc = self._get_collection().find_one(
            self._spec(),
            fields = [key],
            )
        if doc is None:
            raise KeyError(key)
        else:
            return doc[key]
    
    def __iter__(self):
        doc = self._get_collection().find_one(self._spec())
        assert doc is not None
        yield from doc

    def __contains__(self, key):
        doc = self._get_collection().find_one(
            self._spec(),
            fields = [key],
            )
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
        msg = "Storing '{}'."
        logger.debug(msg.format(key))
        result = self._get_collection().update(
            spec = self._spec(),
            document = {'$set': {key: value}},
            upsert = True
            )

    def __delitem__(self, key):
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
