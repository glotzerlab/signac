import sys
import io
import logging

from signac.core.search_engine import DocumentSearchEngine
from signac.core.json import json


logger = logging.getLogger(__name__)


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
        self._file = io.StringIO()
        self._dirty = False
        self._engine = None
        if docs is None:
            self._docs = dict()
        else:
            self._docs = _index(docs)

    def _assert_open(self):
        if self._docs is None:
            raise RuntimeError("Trying to access closed {}.".format(
                type(self).__name__))

    def __str__(self):
        return "<{} file={}>".format(type(self).__name__, self._file)

    def __iter__(self):
        self._assert_open()
        return iter(self._docs.values())

    def __len__(self):
        self._assert_open()
        return len(self._docs)

    def __getitem__(self, _id):
        self._assert_open()
        return self._docs[_id]

    def __setitem__(self, _id, doc):
        self._assert_open()
        self._docs[_id] = doc
        self._dirty = True

    def _find(self, filter):
        self._assert_open()
        if len(filter) == 1 and '_id' in filter:
            return (filter['_id'], )
        if self._engine is None:
            self._engine = DocumentSearchEngine(self._docs.values())
        return self._engine.find(filter)

    def find(self, filter):
        return _CollectionSearchResults(self, self._find(filter))

    def replace_one(self, filter, doc, upsert=False):
        self._assert_open()
        if len(filter) == 1 and '_id' in filter:
            self[filter['_id']] = doc
        else:
            for _id in self._find(filter):
                self[_id] = doc
                break

    def dump(self, file=sys.stdout):
        self._assert_open()
        for doc in self._docs.values():
            file.write(json.dumps(doc) + '\n')

    @classmethod
    def _open(cls, file):
        docs = (json.loads(line) for line in file)
        collection = cls(docs=docs)
        collection._file = file
        return collection

    @classmethod
    def open(cls, filename, mode='r+'):
        logger.debug("Open collection '{}'.".format(filename))
        if filename == ':memory:':
            file = io.StringIO()
        else:
            file = open(filename, mode)
        return cls._open(file)

    def flush(self):
        self._assert_open()
        if self._dirty:
            if self._file is None:
                logger.debug("Flushed collection.")
            else:
                logger.debug("Flush collection to file '{}'.".format(self._file))
                self._file.truncate()
                self.dump(self._file)
                self._file.flush()
            self._dirty = False
        else:
            logger.debug("Flushed collection (no changes).")

    def close(self):
        if self._file is not None:
            self.flush()
            self._file.close()
            self._docs = None
            self._file = None

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()

    def __del__(self):
        self.close()
