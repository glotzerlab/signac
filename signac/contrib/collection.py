import sys

from signac.core.search_engine import DocumentSearchEngine
from signac.core.json import json


def _index(docs):
    return {doc['_id']: doc for doc in docs}


class _CollectionSearchResults(object):
    
    def __init__(self, collection, _ids):
        self._collection = collection
        self._ids = _ids

    def __iter__(self):
        return (self._collection[_id] for _id in self._ids)

    def __len__(self):
        return len(self._ids)

    count = __len__


class Collection(object):

    def __init__(self, docs=None):
        if docs is None:
            self._docs = dict()
        else:
            self._docs = _index(docs)
        self._engine = None

    def __iter__(self):
        return iter(self._docs.values())

    def __len__(self):
        return len(self._docs)

    def __getitem__(self, _id):
        return self._docs[_id]

    def __setitem__(self, _id, doc):
        self._docs[_id] = doc

    def find(self, filter):
        if self._engine is None:
            self._engine = DocumentSearchEngine(self._docs.values())
        results = self._engine.find(filter)
        return _CollectionSearchResults(self, results)

    def dump(self, file=sys.stdout):
        for doc in self._docs.values():
            file.write(json.dumps(doc) + '\n')

    @classmethod
    def from_file(cls, fd):
        if isinstance(fd, str):
            with open(fd) as file:
                return cls.from_file(file)
        else:
            docs = (json.loads(line) for line in fd)
            return cls(docs=docs)
