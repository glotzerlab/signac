import logging
import os
from queue import Queue, Empty
from threading import Thread, Event, Condition

from pymongo.errors import ConnectionFailure
from sqlitedict import SqliteDict

from . mongodbdict import MongoDBDict
from ..common.host import get_db

logger = logging.getLogger(__name__)

def sync_worker(stop_event, synced_or_failed_event,
                error_condition, queue, src, dst):
    while(not stop_event.is_set()):
        #src.sync()
        try:
            synced_or_failed_event.clear()
            action, key = queue.get(timeout = 0.1)
            logger.debug("syncing: {} {}".format(action, key))
            if action == 'set':
                dst[key] = src[key]
            elif action == 'get':
                src[key] = dst[key]
            elif action == 'del':
                del dst[key]
            elif action == 'clr':
                if src:
                    src.clear()
            elif action == 'continue':
                continue
            else:
                raise RuntimeError("illegal sync action", action)
        except Empty:  # Only caught if we cleared the queue
            synced_or_failed_event.set()
            continue # Continue loop skipping 'task_done()'
        except KeyError: # This kind of error can be safely ignored
            pass
        except ConnectionFailure as error:
            logger.warning(error)           # This is not a problem, but
            error_condition.set()           # we need to know about this.
            synced_or_failed_event.set()
        except Exception as error:
            logger.error(error)             # This is likely a problem, but
            error_condition.set()           # the user will need to handle it.
            synced_or_failed_event.set()
        else:
            error_condition.clear()         # Handled the sync action without error.
        queue.task_done()

class ReadOnlyDBDocument(object):

    def __init__(self, hostname, db_name, collection_name, _id, rank = 0, connect_timeout_ms = None):
        self._id = _id
        self._rank = rank
        self._buffer = None
        collection = get_db(db_name, hostname=hostname)[collection_name]
        self._mongodict = MongoDBDict(collection, _id)
        self._sync_queue = Queue()
        self._stop_event = Event()
        self._synced_or_failed_event = Event()
        self._sync_error_condition = Event()
        self._sync_thread = None
        msg = "Opened DBDocument '{}' on '{}'."
        logger.debug(msg.format(_id, collection_name))

    def _buffer_fn(self):
        return '{}.{}.sqlite'.format(self._id, self._rank)

    def __str__(self):
        return "{}(buffer='{}')".format(
            type(self).__name__,
            self._buffer_fn(),
            )

    def _get_buffer(self):
        if self._buffer is None:
            msg = "DBDocument not open!"
            raise RuntimeError(msg)
        return self._buffer

    def _join(self, timeout = 5.0):
        if self._sync_error_condition.is_set():
            return False
        else:
            return self._synced_or_failed_event.wait(timeout = timeout)

    def open(self):
        logger.debug("Opening buffer...")
        self._buffer = SqliteDict(
            filename = self._buffer_fn(),
            tablename = 'dbdocument',
            autocommit = False)
        self._buffer.sync()
        #logger.debug(list(self._buffer.items()))
        logger.debug("Syncing buffer...")
        for key in self._buffer.keys():
            self._sync_queue.put(('set', key))
        self._sync_thread = Thread(
            target = sync_worker, 
            args = (self._stop_event, self._synced_or_failed_event,
                    self._sync_error_condition,
                    self._sync_queue, self._buffer, self._mongodict))
        self._stop_event.clear()
        self._sync_thread.start()
        return self

    def close(self, timeout = None):
        logger.debug("Closing and syncing...")
        #logger.debug(list(self._buffer.items()))
        self._join()
        self._stop_event.set()
        self._sync_thread.join(timeout = timeout)
        self._buffer.sync()
        if self._sync_thread.is_alive() or \
              self._sync_error_condition.is_set():
            logger.warning("Unable to sync to database.")
            self._buffer.close()
        else:
            logger.debug("Synced and closing.")
            self._buffer.close()
            # Deleting the underlying db file causes problems and
            # is probably unnecessary.
            #self._buffer.terminate() # Deleting the file causes

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
        self._sync_queue.put(('get', key))
        self._join()
        return self._get_buffer()[key]
    
    def __iter__(self):
        self._join()
        return self._get_buffer().__iter__()

    def __contains__(self, key):
        self._sync_queue.put(('get', key))
        self._join()
        return self._get_buffer().__contains__(key)

    def get(self, key, default = None):
        self._sync_queue.put(('get', key))
        self._join()
        return self._get_buffer().get(key, default)

    def items(self):
        self._join()
        return self._get_buffer().items()

class DBDocument(ReadOnlyDBDocument):
    
    def __setitem__(self, key, value):
        self._get_buffer()[key] = value
        self._sync_queue.put(('set', key))

    def __delitem__(self, key):
        del self._get_buffer()[key]
        self._sync_queue.put(('del', key))

    def update(self, items=(), ** kwds):
        for key, value in kwds:
            self[key] = value
        for key, value in items:
            self[key] = value

    def clear(self):
        if self._sync_thread is not None:
            if self._sync_thread.is_alive():
                self._sync_queue.put(('clr', None))

    def remove(self):
        try:
            self._mongodict.remove()
        except ConnectionFailure as error:
            logger.warning(error)
        try:
            self._buffer.terminate()
        except AttributeError:
            try:
                os.remove(self._buffer_fn())
            except OSError: pass
