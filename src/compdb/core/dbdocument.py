import logging
logger = logging.getLogger('compdb.core.dbdocument')

from queue import Queue

def queue_worker(stop_event, queue):
    while(not stop_event.is_set()):
        function, args, kwargs = queue.get()
        function(* args, ** kwargs)
        queue.task_done()

def mongodb_queue_worker(queue, host, db_name, collection_name):
    from pymongo import MongoClient
    client = MongoClient(host)
    db = client[db_name]
    collection = db[collection_name]
    while True:
        function, args, kwargs = queue.get()
        function(* args, ** kwargs)
        queue.task_done()

class BufferedExecutor(object):

    def __init__(self):
        from threading import Thread, Event
        self._queue = Queue()
        self._stop_event = Event()
        self._thread = Thread(
                target = queue_worker,
                args = (self._stop_event, self._queue)
                )
        self._thread.daemon = True

    def put(self, function, * args, ** kwargs):
        self._queue.put_nowait((function, args, kwargs))

    def start(self):
        if not self._thread.is_alive():
            self._stop_event.clear()
            self._thread.start()

    def stop(self):
        self._stop_event.set()

    def join(self):
        self._queue.join()

class ReadOnlyDBDocument(object):

    def __init__(self, collection, _id):
        self._collection = collection
        self._id = _id
        self._buffer = BufferedExecutor()
        self._buffer.start()
        msg = "Opened DBDocument '{}' on '{}'."
        logger.debug(msg.format(_id, collection))

    def _spec(self):
        return {'_id': self._id}

    def __getitem__(self, key):
        self._buffer.join()
        doc = self._collection.find_one(
            self._spec(),
            fields = [key],
            )
        if doc is None:
            raise KeyError(key)
        else:
            return doc[key]
    
    def __iter__(self):
        self._buffer.join()
        doc = self._collection.find_one(self._spec())
        assert doc is not None
        yield from doc

    def __contains__(self, key):
        self._buffer.join()
        doc = self._collection.find_one(
            self._spec(),
            fields = [key],
            )
        if doc is None:
            return False
        else:
            return key in doc

    def get(self, key, default = None):
        self._buffer.join()
        try:
            return self.__getitem__(key) 
        except KeyError:
            return default

class DBDocument(ReadOnlyDBDocument):

    def _setitem(self, key, value):
        msg = "Storing '{}'."
        logger.debug(msg.format(key))
        result = self._collection.update(
            spec = self._spec(),
            document = {'$set': {key: value}},
            upsert = True
            )

    def __setitem__(self, key, value):
        self._buffer.put(self._setitem, key, value)

    def _delitem(self, key):
        result = self._collection.update(
            spec = self._spec(),
            document = {
                '$unset': {key: ''}
            })
        assert result['ok']

    def __delitem__(self, key):
        self._buffer.put(self._delitem, key)

    def _clear(self):
        self._collection.save(self._spec())

    def clear(self):
        self._buffer.put(self._clear)

    def _remove(self):
        self._collection.remove(self._spec())

    def remove(self):
        self._buffer.put(self._remove)
