import unittest

import warnings
warnings.simplefilter('default')
warnings.filterwarnings('error', category=DeprecationWarning, module='compdb')

from compdb.contrib.concurrency import DocumentLockError, DocumentLock, DocumentRLock, LOCK_ID_FIELD
import pymongo

def acquire_and_release(doc_id, wait):
    """Testing function, to test process concurrency this must be available on module level."""
    import time
    from pymongo import MongoClient
    from compdb.core.config import load_config
    config = load_config()
    client = MongoClient(config['database_host'])
    db = client['testing']
    mc = db['document_lock']
    lock = DocumentLock(mc, doc_id)
    lock.acquire()
    time.sleep(wait)
    lock.release()
    return True

@unittest.skipIf(pymongo.version_tuple[0] < 3, "Test requires pymongo version >= 3.x")
class TestDocumentLocks(unittest.TestCase):

    def setUp(self):
        from pymongo import MongoClient 
        from compdb.core.config import load_config
        config = load_config()
        client = MongoClient(config['database_host'])
        db = client['testing']
        self.mc = db['document_lock']

    def test_basic(self):
        doc_id = self.mc.insert_one({'a': 0}).inserted_id
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
            self.mc.delete_one({'_id': doc_id})

    # Check locks as context manager

    # Check nested locks
    def test_nested_locks(self):
        doc_id = self.mc.insert_one({'a': 0}).inserted_id
        try:
            with DocumentLock(self.mc, doc_id):
                with DocumentLock(self.mc, doc_id, blocking = False):
                    pass # should raise
        except DocumentLockError as error:
            pass # expected
        else:
            assert False
        finally:
            self.mc.delete_one({'_id': doc_id})

    # Check illegal modification during lock
    def test_document_corruption(self):
        doc_id = self.mc.insert_one({'a': 0}).inserted_id
        try:
            with DocumentLock(self.mc, doc_id):
                # modify lock attribute
                self.mc.update_one({'_id': doc_id}, {'$set': {LOCK_ID_FIELD: 0}})
        except DocumentLockError as error:
            pass # expected
        else:
            assert False
        finally:
            self.mc.delete_one({'_id': doc_id})

    def test_nested_locks_with_timeout(self):
        timeout = 1
        doc_id = self.mc.insert_one({'a': 0}).inserted_id
        try:
            with DocumentLock(self.mc, doc_id):
                with DocumentLock(self.mc, doc_id, timeout = timeout):
                    pass # should fail
        except DocumentLockError as error:
            pass # expected
        else:
            assert False
        finally:
            self.mc.delete_one({'_id': doc_id})

    def test_process_concurrency(self):
        doc_id = self.mc.insert_one({'a': 0}).inserted_id
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
            self.mc.delete_one({'_id': doc_id})

    def test_thread_concurrency(self):
        doc_id = self.mc.insert_one({'a': 0}).inserted_id
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
            self.mc.delete_one({'_id': doc_id})

    def test_process_concurrency(self):
        doc_id = self.mc.insert_one({'a': 0}).inserted_id
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
            self.mc.delete_one({'_id': doc_id})

def lock_and_release(doc_id):
    from pymongo import MongoClient
    from compdb.core.config import load_config
    config = load_config()
    client = MongoClient(config['database_host'])
    db = client['testing']
    collection = db['document_lock']
    import time
    with DocumentLock(collection, doc_id):
        time.sleep(0.01)
    return True

if __name__ == '__main__':
    unittest.main()
