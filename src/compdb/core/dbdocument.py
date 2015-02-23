import logging
logger = logging.getLogger('dbdocument')

class ReadOnlyDBDocument(object):

    def __init__(self, collection, _id):
        self._collection = collection
        self._id = _id
        msg = "Opened DBDocument '{}' on '{}'."
        logger.debug(msg.format(_id, collection))

    def _spec(self):
        return {'_id': self._id}

    def __getitem__(self, key):
        doc = self._collection.find_one(
            self._spec(),
            fields = [key],
            )
        if doc is None:
            raise KeyError(key)
        else:
            return doc[key]
    
    def __iter__(self):
        doc = self._collection.find_one(self._spec())
        assert doc is not None
        yield from doc

    def __contains__(self, key):
        doc = self._collection.find_one(
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
    
class DBDocument(ReadOnlyDBDocument):

    def store(document):
        result = self._collections.update(
            spec = self._spec(),
            document = {'$set': {key: value}})
        assert result['ok']

    def __setitem__(self, key, value):
        msg = "Storing '{}'."
        logger.debug(msg.format(key))
        result = self._collection.update(
            spec = self._spec(),
            document = {'$set': {key: value}},
            upsert = True
            )
        assert result['ok']

    def __delitem__(self, key):
        result = self._collection.update(
            spec = self._spec(),
            document = {
                '$unset': {key: ''}
            })
        assert result['ok']

    def clear(self):
        self._collection.save(self._spec())

    def remove(self):
        self._collection.remove(self._spec())
