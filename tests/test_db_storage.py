import unittest
from contextlib import contextmanager

# Make sure the storages created for this test are unique.
import uuid
test_token = {'test_token': str(uuid.uuid4())}

def test_doc():
    import uuid
    ret = dict(test_token)
    ret.update({
        'name' : str(uuid.uuid4())})
    return ret

def test_data():
    import uuid
    return str(uuid.uuid4())

@contextmanager
def remove_files(storage, spec):
    yield
    docs = storage.find(spec)
    for doc in docs:
        storage.delete(doc['_id'])

class StorageTest(unittest.TestCase):
    
    def setUp(self):
        import tempfile
        from pymongo import MongoClient
        from compdb.db import Storage
        from compdb.core.config import load_config
        config = load_config()
        client = MongoClient(config['database_host'])
        db = client['testing']
        self._mc = db['compdb_storage_test']
        self._tmp_fs = tempfile.TemporaryDirectory()
        self._storage = Storage(self._mc, self._tmp_fs.name)
    
    def tearDown(self):
        self._tmp_fs.cleanup()
        self._mc.drop()
    
    def test_new_file(self):
        from compdb.db import _get_global_fs
        storage = self._storage
        doc = test_doc()
        with remove_files(storage, doc):
            data = test_data()
            with storage.new_file(** doc) as file:
                file.write(data.encode())

    def test_delete_file(self):
        import os.path
        from compdb.db import _get_global_fs
        storage = self._storage
        doc = test_doc()
        with remove_files(storage, doc):
            data = test_data()
            with storage.new_file(** doc) as file:
                file.write(data.encode())
        self.assertEqual(len(list(storage.find(doc))), 0)

    def test_find(self):
        from compdb.db import _get_global_fs
        import uuid
        storage = self._storage
        doc = test_doc()
        data = test_data()
        with remove_files(storage, doc):
            with storage.new_file(** doc) as file:
                file.write(data.encode())
            files = storage.find(dict(name = 'my_file'))
            for file in files:
                read_back = file.read().decode()
                self.assertEqual(data, read_back)

if __name__ == '__main__':
    unittest.main()
