import itertools
import uuid


class SimpleCollection(object):

    def __init__(self):
        self._index = dict()

    def insert_one(self, doc):
        _id = doc.setdefault('_id', uuid.uuid4())
        self._index[_id] = doc

    def insert_many(self, docs):
        for doc in docs:
            self.insert_one(doc)

    def replace_one(self, filter, doc):
        if not list(filter.keys()) == ['_id']:
            raise NotImplementedError(
                "Simple collection can only be queried for _id.")
        self._index[filter['_id']] = doc

    def find(self, limit=0):
        if limit != 0:
            yield from itertools.islice(self._index.values(), limit)
        else:
            yield from self._index.values()

    def find_one(self):
        return next(self._index.values())
