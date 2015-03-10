import logging
from . concurrency import DocumentLock

DOCUMENT_LOCK_ID = 0

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
        return record.__dict__

    def emit(self, record):
        self._collection.insert(self._generate_doc(record))

    def lock(self):
        return DocumentLock(
            self._collection,
            self._lock_id)

def record_from_doc(doc):
    return logging.makeLogRecord(doc)

class ProjectHandler(logging.Handler):
    pass
