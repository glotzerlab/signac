import unittest
from contextlib import contextmanager

from signac.core.mongodb_queue import MongoDBQueue, Empty
from signac.core.mongodb_executor import MongoDBExecutor

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

import warnings
warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='signac')

def testdata():
    import uuid
    return uuid.uuid4()

def get_item():
    return {'my_item': testdata()}

def get_collection_handle(_id):
    from signac.core.config import load_config
    from pymongo import MongoClient
    config = load_config()
    client = MongoClient(config['database_host'])
    db = client['testing']
    if _id is None:
        _id = uuid.uuid4()
    collection_name = 'test_mongodb_queue_{}'.format(_id)
    return db[collection_name]

@contextmanager
def get_collection(_id = None):
    if _id is None:
        import uuid
        _id = uuid.uuid4()
    collection = get_collection_handle(_id)
    try:
        yield collection
    except Exception:
        raise
    finally:
        collection.drop()

@contextmanager
def get_queue(id_queue = None):
    with get_collection(id_queue) as collection:
        yield MongoDBQueue(collection)

@contextmanager
def get_executor(id_queue = None, id_results = None):
    with get_queue(id_queue) as job_queue:
        with get_collection(id_results) as result_collection:
            yield MongoDBExecutor(job_queue, result_collection)

def get_executor_no_drop(id_queue = None, id_results = None):
    job_queue = MongoDBQueue(get_collection_handle(id_queue))
    result_collection = get_collection_handle(id_results)
    return MongoDBExecutor(job_queue, result_collection)

@unittest.skipIf(not PYMONGO_3, 'test requires pymongo version >= 3.0.x')
class MongoDBTest(unittest.TestCase):
    pass

class MongoDBQueueTest(MongoDBTest):

    def test_init(self):
        with get_collection() as collection:
            queue = MongoDBQueue(collection)

    def test_put_and_get(self):
        item = get_item()
        with get_queue() as queue:
            queue.put(item)
            item2 = queue.get()
            self.assertEqual(item, item2)

    def test_put_and_get_multiple(self):
        item1 = get_item()
        item2 = get_item()
        with get_queue() as queue:
            queue.put(item1)
            queue.put(item2)
            c_item1 = queue.get()
            c_item2 = queue.get()
            self.assertEqual(item1, c_item1)
            self.assertEqual(item2, c_item2)
    
    def test_put_get_done(self):
        item = get_item()
        with get_queue() as queue:
            queue.put(item)
            item2 = queue.get()
            self.assertEqual(item, item2)
            queue.task_done()
            with self.assertRaises(ValueError):
                queue.task_done()

    def test_join(self):
        from threading import Thread
        item1 = get_item()
        item2 = get_item()
        with get_queue() as queue:
            def short_wait():
                queue.join()
            queue.put(item1)
            queue.put(item2)
            t1 = Thread(target = short_wait)
            t1.daemon = True
            t1.start()
            t1.join(timeout = 0.1)
            self.assertTrue(t1.is_alive())
            c_item1 = queue.get()
            self.assertEqual(item1, c_item1)
            queue.task_done()
            t2 = Thread(target = short_wait)
            t2.daemon = True
            t2.start()
            t2.join(timeout = 0.1)
            self.assertTrue(t2.is_alive())
            c_item2 = queue.get()
            self.assertEqual(item2, c_item2)
            queue.task_done()
            t3 = Thread(target = short_wait)
            t3.daemon = True
            t3.start()
            t3.join(timeout = 1)
            self.assertFalse(t3.is_alive())

    def test_id(self):
        item1 = get_item()
        with get_queue() as queue:
            id1 = queue.put(item1)
            self.assertIn(id1, queue)
            c_item1= queue.get()
            self.assertEqual(item1, c_item1)
            self.assertNotIn(id1, queue)

    def test_contains(self):
        item1 = get_item()
        item2 = get_item()
        with get_queue() as queue:
            self.assertNotIn(item1, queue)
            self.assertNotIn(item2, queue)
            queue.put(item1)
            self.assertIn(item1, queue)
            self.assertNotIn(item2, queue)
            queue.put(item2)
            self.assertIn(item1, queue)
            self.assertIn(item2, queue)
            r_item1 = queue.get()
            self.assertEqual(item1, r_item1)
            self.assertNotIn(item1, queue)
            self.assertIn(item2, queue)
            r_item2 = queue.get()
            self.assertEqual(item2, r_item2)
            self.assertNotIn(item1, queue)
            self.assertNotIn(item2, queue)
            queue.task_done()
            queue.task_done()
            with self.assertRaises(ValueError):
                queue.task_done()

def my_function(x, y = 1):
    return x * x * y

def error_function(x, y = 1):
    raise RuntimeError(my_function(x, y = 1))

def execution_enter_loop(id_queue, id_results, timeout):
    executor = get_executor_no_drop(id_queue, id_results)
    try:
        executor.enter_loop(timeout)
    except Empty:
        pass
    return True

class MongoDBExecutorTest(MongoDBTest):
    
    def test_init(self):
        with get_executor() as executor:
            pass

    def test_encode_integrity(self):
        from signac.core.mongodb_executor import encode_callable, decode_callable
        fn = my_function
        args = (1, )
        kwargs = {'y': 2}
        r = fn(*args, **kwargs)
        encoded = encode_callable(my_function, args, kwargs)
        encoded2 = encode_callable(my_function, args, kwargs)
        self.assertEqual(encoded, encoded2)

    def test_encode_decode(self):
        from signac.core.mongodb_executor import encode_callable, decode_callable
        fn = my_function
        args = (1, )
        kwargs = {'y': 2}
        r = fn(*args, **kwargs)
        encoded = encode_callable(my_function, args, kwargs)
        # test without reload
        fn2, args2, kwargs2 = decode_callable(encoded, reload = False)
        r2 = fn2(*args2, **kwargs2)
        self.assertEqual(fn, fn2)
        self.assertEqual(args, args2)
        self.assertEqual(kwargs, kwargs2)
        self.assertEqual(r, r2)
        # test with reload
        fn2, args2, kwargs2 = decode_callable(encoded, reload = True)
        r2 = fn2(*args2, **kwargs2)
        self.assertNotEqual(fn, fn2)
        self.assertEqual(args, args2)
        self.assertEqual(kwargs, kwargs2)
        self.assertEqual(r, r2)

    def test_submit(self):
        with get_executor() as executor:
            executor.submit(my_function, 1)

    def test_submit_and_execute(self):
        with get_executor() as executor:
            future = executor.submit(my_function, x = 2, y = 4)
            with self.assertRaises(Empty):
                executor.enter_loop(timeout = 0.1)
            result = future.result(0.2)
            self.assertEqual(my_function(x = 2, y = 4), result)

    def test_submit_execute_and_fetch(self):
        from signac.core.mongodb_executor import encode_callable, decode_callable, encode, decode
        args = ()
        kwargs = {'x': 2, 'y': 4}
        expected_result = my_function(*args, **kwargs)
        with get_executor() as executor:
            future = executor.submit(my_function, x = 2, y = 4)
            with self.assertRaises(Empty):
                executor.enter_loop(timeout = 0.1)
            result = future.result(0.2)
            self.assertEqual(expected_result, result)
            raw = executor._fetch_result(encode_callable(my_function, args, kwargs))
            decoded = decode_callable(raw['item'])
            self.assertEqual(expected_result, decode(raw['result']))

    def test_submit_and_execute_with_error(self):
        with get_executor() as executor:
            future1 = executor.submit(my_function, x = 2, y = 4)
            future2 = executor.submit(error_function, x = 2, y = 4)
            with self.assertRaises(Empty):
                executor.enter_loop(timeout = 0.1)
            result1 = future1.result(0.1)
            self.assertEqual(my_function(x = 2, y = 4), result1)
            with self.assertRaises(RuntimeError):
                result2 = future2.result(0.1)

    def test_resubmit(self):
        with get_executor() as executor:
            future1 = executor.submit(my_function, 2, y = 4)
            future2 = executor.submit(error_function, 2, y = 4)
            with self.assertRaises(ValueError):
                executor.submit(my_function, 2, y = 4)
            with self.assertRaises(ValueError):
                executor.submit(error_function, 2, y = 4)
            with self.assertRaises(Empty):
                executor.enter_loop(timeout = 0.1)
            with self.assertRaises(ValueError):
                executor.submit(my_function, 2, y = 4)
            with self.assertRaises(ValueError):
                executor.submit(error_function, 2, y = 4)
            result1 = future1.result(0.1)
            self.assertEqual(my_function(2, y = 4), result1)
            with self.assertRaises(RuntimeError):
                result2 = future2.result(0.1)
                
    def test_multiprocessing(self):
        from multiprocessing import Pool
        import uuid
        id_queue = uuid.uuid4()
        id_results = uuid.uuid4()
        assert id_queue != id_results

        num_processes = 4
        num_jobs = 20
        timeout = 0.1

        with get_executor(id_queue, id_results) as executor:
            futures = [executor.submit(my_function, x = i) for i in range(num_jobs)]
            with Pool(processes = num_processes) as pool:
                result = pool.starmap(execution_enter_loop,
                    [(id_queue, id_results, timeout) for i in range(num_jobs)])
                self.assertEqual(result, [True] * num_jobs)
            for i, future in enumerate(futures):
                result = future.result(timeout = 1)
                self.assertEqual(my_function(x = i), result)

if __name__ == '__main__':
    unittest.main()
