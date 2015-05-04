import logging
logger = logging.getLogger('compdb.mongodb_queue')

from queue import Queue

ID_COUNTER = 0
KEY_COUNTER = 'counter'
FILTER_COUNTER = {'_id': ID_COUNTER}
FILTER_COUNTER_DEC = {'_id': ID_COUNTER, KEY_COUNTER: {'$gt': 0}}
FILTER_NOT_COUNTER = {'_id': {'$ne': ID_COUNTER}}

class Empty(Exception):
    pass

class Full(Exception):
    pass

class MongoDBQueue(object):

    def __init__(self, collection):
        self._collection = collection

    def qsize(self):
        return self._collection.find(FILTER_NOT_COUNTER).count()

    def empty(self):
        return self.qsize() == 0

    def full(self):
        return False # There is no limit on the queue size implemented.

    def _get(self):
        from pymongo import ASCENDING
        return self._collection.find_one_and_delete(FILTER_NOT_COUNTER, sort = [('_id', ASCENDING)])

    def __contains__(self, _id):
        return self._collection.find_one({'_id': _id}) is not None

    def _num_open_tasks(self):
        result = self._collection.find_one(FILTER_COUNTER)
        if result is None:
            return 0
        else:
            open_tasks = int(result[KEY_COUNTER])
            if open_tasks < 0:
                raise ValueError()
            else:
                return open_tasks

    def put(self, item, block = True, timeout = None):
        # block and timeout are ignored, as in this implementation, the queue can never be full.
        result = self._collection.insert_one(item)
        self._collection.update_one(FILTER_COUNTER, {'$inc': {KEY_COUNTER: 1}}, upsert = True)
        return result.inserted_id

    def get(self, block = True, timeout = None):
        if block:
            from . utility import fetch
            try:
                return fetch(target = self._get, timeout = timeout)
            except TimeoutError:
                raise Empty()
        else:
            item = self._get()
            if item is None:
                raise Empty()
            else:
                return item 

    def get_nowait(self):
        return self.get(block = False)

    def task_done(self):
        result = self._collection.update_one(FILTER_COUNTER_DEC, {'$inc': {KEY_COUNTER: -1}})
        if result.modified_count != 1:
            raise ValueError()

    def join(self):
        from math import tanh
        from itertools import count
        import time
        w = (tanh(0.05 * i) for i in count())
        while True:
            if self._num_open_tasks() == 0:
                return
            time.sleep(max(0.001, next(w)))

    def peek(self):
        items = self._collection.find(FILTER_NOT_COUNTER)
        yield from items

    def clear(self):
        self._collection.delete_many(
            {'$or': [FILTER_COUNTER, FILTER_NOT_COUNTER]})
