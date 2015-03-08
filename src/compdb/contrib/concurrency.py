# -*- coding: utf-8 -*-
"""Provides classes to control concurrent access to mongodb.

This module provides lock classes, which behave similar to the locks from the threading module, but act on mongodb documents.
All classes are thread-safe.

Requires Python version 3.3 or higher.

Example:

  # To acquire a lock for this document we first instantiate it
  lock = DocumentLock(mongodb_collection, doc_id)
  try:
      lock.acquire() # will block until lock is acquired, but never throw
      lock.release()
  except DocumentLockError as error:
      # An exception will be thrown when an error during the release of a
      # lock occured, leaving the document in an undefined state.
      print(error)

  # The lock can be used as a context manager
  with lock:
      # lock is defnitely aquired
      pass

  # How to use reentrant locks
  lock = DocumentRLock(mc, doc_id)
  with lock: # Acquiring once
      with lock: # Acquiring twice
          pass
"""

# version check
import sys
if sys.version_info[0] < 3 or sys.version_info[1] < 3:
    msg = "This module requires Python version 3.3 or higher."
    raise ImportError(msg)

import logging
logger = logging.getLogger('concurrency')

try:
    from threading import TIMEOUT_MAX
except ImportError:
    TIMEOUT_MAX = 100000

LOCK_ID_FIELD = '_lock_id'
LOCK_COUNTER_FIELD = '_lock_counter'

from contextlib import contextmanager
@contextmanager
def acquire_timeout(lock, blocking, timeout):
    """Helping contextmanager to acquire a lock with timeout and release it on exit."""
    result = lock.acquire(blocking = blocking, timeout = timeout)
    yield result
    if result:
        lock.release()

class DocumentLockError(Exception):
    """Signifies an error during lock allocation or deallocation."""
    pass

class DocumentBaseLock(object):
    """The base class for Lock Objects.

    This class should not be instantiated directly.
    """

    def __init__(self, collection, document_id, blocking = True, timeout = -1):
        from uuid import uuid4
        from threading import Lock
        self._lock_id = uuid4()
        self._collection = collection
        self._document_id = document_id
        self._blocking = blocking
        self._timeout = timeout

        self._lock = Lock()
        self._wait = 0.1

    def acquire(self, blocking = True, timeout = -1):
        """Acquire a lock, blocking or non-blocking, with or without timeout.

        Note:
          A reentrant Lock such as DocumentRLock can be acquired multiple times by the same process.
          When the number of releases exceeds the number of acquires or the lock cannot be released, a DocumentLockError is raised.

        Args:
          blocking: When set to True (default), if lock is locked, block until it is unlocked, then lock.
          timeout: Time to wait in seconds to acquire lock. Can only be used when blocking is set to True. 

        Returns:
            Returns true when lock was successfully acquired, otherwise false.
        """
        if not blocking and timeout != -1:
            raise ValueError("Cannot set timeout if blocking is False.")
        if timeout > TIMEOUT_MAX:
            raise OverflowError("Maxmimum timeout is: {}".format(TIMEOUT_MAX))
        with acquire_timeout(self._lock, blocking, timeout) as lock:
            if blocking:
                #from multiprocessing import Process
                from threading import Thread, Event
                import time
                stop_event = Event()
                def try_to_acquire():
                    while(not stop_event.is_set()):
                        if self._acquire():
                            return True
                        stop_event.wait(max(1, timeout / 100))
                t_acq = Thread(target = try_to_acquire)
                t_acq.start()
                t_acq.join(timeout = None if timeout == -1 else timeout)
                if t_acq.is_alive():
                    stop_event.set()
                    #t_acq.terminate()
                    t_acq.join()
                    return False
                else:
                    return True
            else:
                return self._acquire()

    def release(self):
        """Release the lock.
        
        If lock cannot be released or the number of releases exceeds the number of acquires for a reentrant lock a DocumentLockError is raised.
        """
        self._release()

    def __enter__(self):
        """Use the lock as context manager.

        Unlike the acquire method this will raise an exception if it was not possible to acquire the lock.
        """
        blocked = self.acquire(
            blocking = self._blocking,
            timeout = self._timeout)
        if not blocked:
            msg = "Failed to lock document with id='{}'."
            raise DocumentLockError(msg.format(self._document_id))

    def __exit__(self, exception_type, exception_value, traceback):
        self.release()
        return False

class DocumentLock(DocumentBaseLock):
    
    def __init__(self, collection, document_id, blocking = True, timeout = -1):
        """Initialize a lock for a document with `_id` equal to `document_id` in the `collection`. 

        Args:
          collection: A mongodb collection, with pymongo API.
          document_id: The id of the document, which shall be locked.
        """
        super(DocumentLock, self).__init__(
            collection = collection,
            document_id = document_id,
            blocking = blocking,
            timeout = timeout)

    def _acquire(self):
        logger.debug("Acquiring lock...")
        result = self._collection.find_and_modify(
            query = {
                '_id': self._document_id,
                LOCK_ID_FIELD: {'$exists': False}},
            update = {
                '$set': {
                    LOCK_ID_FIELD: self._lock_id}})
        return result is not None

    def _release(self):
        logger.debug("Releasing lock...")
        result = self._collection.find_and_modify(
            query = {
                '_id': self._document_id,
                LOCK_ID_FIELD: self._lock_id},
            update = {
                '$unset': {LOCK_ID_FIELD: ''}},
                )
        if result is None:
            msg = "Failed to remove lock from document with id='{}', lock field was manipulated. Document state is undefined!"
            raise DocumentLockError(msg.format(self._document_id))

class DocumentRLock(DocumentBaseLock):
    
    def __init__(self, collection, document_id, blocking = True, timeout = -1):
        """Initialize a reentrant lock for a document with `_id` equal to `document_id` in the `collection`. 

        Args:
          collection: A mongodb collection, with pymongo API.
          document_id: The id of the document, which shall be locked.
        """
        super(DocumentRLock, self).__init__(
            collection = collection,
            document_id = document_id,
            blocking = blocking,
            timeout = timeout)

    def _acquire(self):
        result = self._collection.find_and_modify(
            query = {
                '_id':  self._document_id,
                '$or': [
                    {LOCK_ID_FIELD: {'$exists': False}},
                    {LOCK_ID_FIELD: self._lock_id}]},
            update = {
                '$set': {LOCK_ID_FIELD: self._lock_id},
                '$inc': {LOCK_COUNTER_FIELD: 1}},
            new = True,
                )
        if result is not None:
            return True
        else:
            return False

    def _release(self):
        # Trying full release
        result = self._collection.find_and_modify(
            query = {
                '_id': self._document_id,
                LOCK_ID_FIELD: self._lock_id,
                LOCK_COUNTER_FIELD: 1},
            update = {'$unset': {LOCK_ID_FIELD: '', 'lock_level': ''}})
        if result is not None:
            return

        # Trying partial release 
        result = self._collection.find_and_modify(
            query = {
                '_id':  self._document_id,
                LOCK_ID_FIELD: self._lock_id},
            update = {'$inc': {LOCK_COUNTER_FIELD: -1}},
            new = True)
        if result is None:
            msg = "Failed to remove lock from document with id='{}', lock field was manipulated or lock was released too many times. Document state is undefined!"
            raise DocumentLockError(msg.format(self._document_id))

def acquire_and_release(doc_id, wait):
    """Testing function, to test process concurrency this must be available on module level."""
    import time
    from pymongo import MongoClient
    client = MongoClient()
    db = client['testing']
    mc = db['document_lock']
    lock = DocumentLock(mc, doc_id)
    lock.acquire()
    time.sleep(wait)
    lock.release()
    return True

import unittest
class TestDocumentLocks(unittest.TestCase):

    def setUp(self):
        from pymongo import MongoClient 
        client = MongoClient()
        db = client['testing']
        self.mc = db['document_lock']

    def test_basic(self):
        doc_id = self.mc.insert({'a': 0})
        try:
            lock = DocumentLock(self.mc, doc_id)
            assert lock.acquire()
            lock.release()
            rlock = DocumentRLock(self.mc, doc_id)
            num_levels = 3
            for i in range(num_levels):
                assert rlock.acquire()
            for i in range(num_levels):
                rlock.release()
            try:
                rlock.release()
            except DocumentLockError as error:
                pass # expected
            else:
                assert False
        except DocumentLockError as error:
            raise
        finally:
            self.mc.remove(doc_id)

    # Check locks as context manager

    # Check nested locks
    def test_nested_locks(self):
        doc_id = self.mc.insert({'a': 0})
        try:
            with DocumentLock(self.mc, doc_id):
                with DocumentLock(self.mc, doc_id, blocking = False):
                    pass # should raise
        except DocumentLockError as error:
            pass # expected
        else:
            assert False
        finally:
            self.mc.remove(doc_id)

    # Check illegal modification during lock
    def test_document_corruption(self):
        doc_id = self.mc.insert({'a': 0})
        try:
            with DocumentLock(self.mc, doc_id):
                # modify lock attribute
                self.mc.update(
                    {'_id': doc_id},
                    {'$set': {LOCK_ID_FIELD: 0}})
        except DocumentLockError as error:
            pass # expected
        else:
            assert False
        finally:
            self.mc.remove(doc_id)

    def test_nested_locks_with_timeout(self):
        timeout = 1
        doc_id = self.mc.insert({'a': 0})
        try:
            with DocumentLock(self.mc, doc_id):
                with DocumentLock(self.mc, doc_id, timeout = timeout):
                    pass # should fail
        except DocumentLockError as error:
            pass # expected
        else:
            assert False
        finally:
            self.mc.remove(doc_id)

    def test_process_concurrency(self):
        doc_id = self.mc.insert({'a': 0})
        try:
            num_processes = 10
            num_locks = 100
            from multiprocessing import Pool
            with Pool(processes = num_processes) as pool:
                result = pool.starmap_async(
                    acquire_and_release,
                    [(doc_id, 0.01) for i in range(num_locks)])
                result = result.get(timeout = 5)
                assert result == [True] * num_locks
        except DocumentLockError as error:
            raise
        finally:
            self.mc.remove(doc_id)

    def test_thread_concurrency(self):
        doc_id = self.mc.insert({'a': 0})
        num_workers = 5
        num_locks = 20
        from concurrent.futures import ThreadPoolExecutor, as_completed
        try:
            with ThreadPoolExecutor(max_workers = num_workers) as executor:
                lock = DocumentLock(self.mc, doc_id)
                def lock_and_release():
                    import time
                    with lock:
                        time.sleep(0.01)
                    return True
                results = {executor.submit(lock_and_release) for i in range(num_locks)}
                for future in as_completed(results):
                    assert future.result()
        except DocumentLockError as error:
            raise
        finally:
            self.mc.remove(doc_id)

    def test_process_concurrency(self):
        doc_id = self.mc.insert({'a': 0})
        num_workers = 5
        num_locks = 20
        from concurrent.futures import ProcessPoolExecutor, as_completed
        try:
            with ProcessPoolExecutor(max_workers = num_workers) as executor:
                results = {executor.submit(lock_and_release, doc_id) for i in range(num_locks)}
                for future in as_completed(results):
                    assert future.result()
        except DocumentLockError as error:
            raise
        finally:
            self.mc.remove(doc_id)

def lock_and_release(doc_id):
    from pymongo import MongoClient
    client = MongoClient()
    db = client['testing']
    collection = db['document_lock']
    import time
    with DocumentLock(collection, doc_id):
        time.sleep(0.01)
    return True


if __name__ == '__main__':
    unittest.main()
