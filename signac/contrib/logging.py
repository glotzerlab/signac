import logging
from . concurrency import DocumentLock

DOCUMENT_LOCK_ID = 0

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

class MongoDBHandler(logging.Handler):

    def __init__(self, collection, lock_id = None):
        if lock_id is None:
            lock_id = DOCUMENT_LOCK_ID
        self._collection = collection
        self._lock_id = lock_id
        lock_doc = {'_id': self._lock_id}
        self._collection.update(lock_doc, lock_doc, upsert = True)
        super().__init__()
    
    def _generate_doc(self, record):
        return dict(record.__dict__)

    def emit(self, record):
        try:
            return self._emit(record)
        except Exception as error:
            msg = "Error during attempt to log '{record}': {error}."
            print(msg.format(record=record, error=error))
            return False

    def _emit(self, record):
        record_doc = self._generate_doc(record)
        assert not '_id' in record_doc
        if PYMONGO_3:
            self._collection.insert_one(record_doc)
        else:
            self._collection.insert(record_doc)

    def lock(self):
        return DocumentLock(
            self._collection,
            self._lock_id)

def record_from_doc(doc):
    return logging.makeLogRecord(doc)

class ProjectHandler(logging.Handler):
    pass
