import logging
logger = logging.getLogger('compdb.core.dbdocument')

from queue import Queue

def sync_worker(stop_event, failed_sync_condition, queue, src, dst):
    from pymongo.errors import ConnectionFailure
    from queue import Empty
    while(not stop_event.is_set()):
        src.sync()
        try:
            key = queue.get(timeout = 1)
            dst[key] = src[key]
        except Empty:
            pass
        except KeyError:
            pass
        except ConnectionFailure as error:
            failed_sync_condition.set()
            stop_event.wait()
        except Exception as error:
            stop_event.wait()
        else:
            failed_sync_condition.clear()
            queue.task_done()

class ReadOnlyDBDocument(object):

    def __init__(self, host, db_name, collection_name, _id):
        from threading import Event, Condition
        from . mongodbdict import MongoDBDict
        #self._host = host
        #self._db_name = db_name
        #self._collection_name = collection_name
        #self._collection = None
        self._id = _id
        self._buffer = None
        self._mongodict = MongoDBDict(
            host, db_name, collection_name, _id)
        self._to_sync = Queue()
        self._stop_event = Event()
        self._failed_sync_condition = Event()
        self._sync_thread = None
        msg = "Opened DBDocument '{}' on '{}'."
        logger.debug(msg.format(_id, collection_name))

    def _buffer_fn(self):
        return '{}.sqlite'.format(self._id)

    def _get_buffer(self):
        if self._buffer is None:
            msg = "DBDocument not open!"
            raise RuntimeError(msg)
        return self._buffer

    def _join(self):
        self._buffer.sync()
        if not self._failed_sync_condition:
            self._to_sync.join()

    def open(self):
        from sqlitedict import SqliteDict
        from threading import Thread
        self._buffer = SqliteDict(self._buffer_fn())
        self._buffer.sync()
        for key in self._buffer.keys():
            self._to_sync.put(key)
        self._sync_thread = Thread(
            target = sync_worker, 
            args = (self._stop_event, self._failed_sync_condition,
                    self._to_sync, self._buffer, self._mongodict))
        self._stop_event.clear()
        self._sync_thread.start()
        return self

    def close(self):
        self._join()
        self._stop_event.set()
        self._sync_thread.join()
        if self._failed_sync_condition.is_set():
            self._buffer.close()
        else:
            self._buffer.terminate()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_val, traceback):   
        try:
            self.close()
        except:
            return False
        else:
            return True

    def __getitem__(self, key):
        self._join()
        return self._get_buffer()[key]
    
    def __iter__(self):
        return self._get_buffer().__iter__()

    def __contains__(self, key):
        return self._get_buffer().__contains__(key)

    def get(self, key, default = None):
        return self._get_buffer().get(key, default)

class DBDocument(ReadOnlyDBDocument):
    
    def __setitem__(self, key, value):
        self._get_buffer()[key] = value
        self._to_sync.put(key)

    def __delitem__(self, key):
        del self._get_buffer()[key]
        self._to_sync.put(key)

    def update(self, items=(), ** kwds):
        for key, value in kwds:
            self[key] = value
        for key, value in items:
            self[key] = value

    def clear(self):
        if self._buffer is not None:
            self._get_buffer().clear()

    def remove(self):
        self._mongodict.remove()
        if self._buffer is not None:
            self._get_buffer().terminate()
