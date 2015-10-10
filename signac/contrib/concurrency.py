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
      # lock is definitely aquired
      pass

  # How to use reentrant locks
  lock = DocumentRLock(mc, doc_id)
  with lock: # Acquiring once
      with lock: # Acquiring twice
          pass
"""

import logging
import sys
import contextlib
import itertools
import uuid
from threading import Thread, Event, Lock
from math import tanh

import pymongo

logger = logging.getLogger(__name__)

# version check
if sys.version_info[0] < 3 or sys.version_info[1] < 3:
    msg = "This module requires Python version 3.3 or higher."
    raise ImportError(msg)

PYMONGO_3 = pymongo.version_tuple[0] == 3

try:
    from threading import TIMEOUT_MAX
except ImportError:
    TIMEOUT_MAX = 100000

LOCK_ID_FIELD = '_lock_id'
LOCK_COUNTER_FIELD = '_lock_counter'


@contextlib.contextmanager
def acquire_timeout(lock, blocking, timeout):
    """Helping contextmanager to acquire a lock with timeout and release it on exit."""
    result = lock.acquire(blocking=blocking, timeout=timeout)
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

    def __init__(self, collection, document_id, blocking=True, timeout=-1):
        self._lock_id = uuid.uuid4()
        self._collection = collection
        self._document_id = document_id
        self._blocking = blocking
        self._timeout = timeout

        self._lock = Lock()
        self._wait = 0.1

    def acquire(self, blocking=True, timeout=-1):
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
        logger.debug("Acquiring lock.")
        if not blocking and timeout != -1:
            raise ValueError("Cannot set timeout if blocking is False.")
        if timeout > TIMEOUT_MAX:
            raise OverflowError("Maxmimum timeout is: {}".format(TIMEOUT_MAX))
        with acquire_timeout(self._lock, blocking, timeout):
            if blocking:
                stop_event = Event()

                def try_to_acquire():
                    w = (tanh(0.05 * i) for i in itertools.count())
                    while(not stop_event.is_set()):
                        if self._acquire():
                            return True
                        stop_event.wait(max(0.001, next(w)))
                t_acq = Thread(target=try_to_acquire)
                t_acq.start()
                t_acq.join(timeout=None if timeout == -1 else timeout)
                if t_acq.is_alive():
                    stop_event.set()
                    # t_acq.terminate()
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

    def force_release(self):
        logger.debug("Force releasing lock.")
        if PYMONGO_3:
            self._collection.find_one_and_update(
                filter={'_id': self._document_id},
                update={'$unset': {LOCK_ID_FIELD: '', LOCK_COUNTER_FIELD: ''}})
        else:
            self._collection.find_and_modify(
                query={'_id': self._document_id},
                update={'$unset': {LOCK_ID_FIELD: '', LOCK_COUNTER_FIELD: ''}})

    def __enter__(self):
        """Use the lock as context manager.

        Unlike the acquire method this will raise an exception if it was not possible to acquire the lock.
        """
        blocked = self.acquire(
            blocking=self._blocking,
            timeout=self._timeout)
        if not blocked:
            msg = "Failed to lock document with id='{}'."
            raise DocumentLockError(msg.format(self._document_id))

    def __exit__(self, exception_type, exception_value, traceback):
        self.release()
        return False


class DocumentLock(DocumentBaseLock):

    def __init__(self, collection, document_id, blocking=True, timeout=-1):
        """Initialize a lock for a document with `_id` equal to `document_id` in the `collection`. 

        Args:
          collection: A mongodb collection, with pymongo API.
          document_id: The id of the document, which shall be locked.
        """
        super(DocumentLock, self).__init__(
            collection=collection,
            document_id=document_id,
            blocking=blocking,
            timeout=timeout)

    def _acquire(self):
        filter = {'_id': self._document_id, LOCK_ID_FIELD: {'$exists': False}}
        update = {'$set': {LOCK_ID_FIELD: self._lock_id}}
        if PYMONGO_3:
            result = self._collection.find_one_and_update(filter, update)
        else:
            result = self._collection.find_and_modify(filter, update)
        acquired = result is not None
        if acquired:
            logger.debug("Acquired.")
        return acquired

    def _release(self):
        logger.debug("Releasing lock.")
        filter = {'_id': self._document_id, LOCK_ID_FIELD: self._lock_id}
        update = {'$unset': {LOCK_ID_FIELD: ''}}
        if PYMONGO_3:
            result = self._collection.find_one_and_update(filter, update)
        else:
            result = self._collection.find_and_modify(filter, update)
        if result is None:
            msg = "Failed to remove lock from document with id='{}', lock field was manipulated. Document state is undefined!"
            raise DocumentLockError(msg.format(self._document_id))
        logger.debug("Released.")


class DocumentRLock(DocumentBaseLock):

    def __init__(self, collection, document_id, blocking=True, timeout=-1):
        """Initialize a reentrant lock for a document with `_id` equal to `document_id` in the `collection`. 

        Args:
          collection: A mongodb collection, with pymongo API.
          document_id: The id of the document, which shall be locked.
        """
        super(DocumentRLock, self).__init__(
            collection=collection,
            document_id=document_id,
            blocking=blocking,
            timeout=timeout)

    def _acquire(self):
        filter = {
            '_id':  self._document_id,
            '$or': [
                {LOCK_ID_FIELD: {'$exists': False}},
                {LOCK_ID_FIELD: self._lock_id}]}
        update = {
            '$set': {LOCK_ID_FIELD: self._lock_id},
            '$inc': {LOCK_COUNTER_FIELD: 1}}
        if PYMONGO_3:
            result = self._collection.find_one_and_update(filter, update)
        else:
            result = self._collection.find_and_modify(filter, update, new=True)
        return result is not None

    def _release(self):
        # Trying full release
        filter = {
            '_id': self._document_id,
            LOCK_ID_FIELD: self._lock_id,
            LOCK_COUNTER_FIELD: 1}
        update = {'$unset': {LOCK_ID_FIELD: '', 'lock_level': ''}}
        if PYMONGO_3:
            result = self._collection.find_one_and_update(filter, update)
        else:
            result = self._collection.find_and_modify(filter, update)
        if result is not None:
            return

        # Trying partial release
        filter = {
            '_id':  self._document_id,
            LOCK_ID_FIELD: self._lock_id}
        update = {'$inc': {LOCK_COUNTER_FIELD: -1}}
        if PYMONGO_3:
            result = self._collection.find_one_and_update(filter, update)
        else:
            result = self._collection.find_and_modify(filter, update, new=True)
        if result is None:
            msg = "Failed to remove lock from document with id='{}', lock field was manipulated or lock was released too many times. Document state is undefined!"
            raise DocumentLockError(msg.format(self._document_id))
