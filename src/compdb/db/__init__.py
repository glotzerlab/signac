import logging
logger = logging.getLogger('db')

from compdb.core.config import load_config
from compdb.core import _get_db

from .database import Database
from .conversion import make_db_method

def _get_db_global_fs():
    return _get_db(CONFIG['database_global_fs'])

def _get_global_fs():
    return Storage(
        collection = _get_db_global_fs()['compdb.fs'],
        fs_dir = CONFIG['global_fs_dir'])

class StorageFileCursor(object):

    def __init__(self, cursor, fn):
        self._cursor = cursor
        self._fn = fn

    def __getitem__(self, key):
        return self._cursor[key]

    def read(self):
        with open(self._fn, 'rb') as file:
            return file.read()

class Storage(object):
    
    def __init__(self, collection, fs_dir):
        self._collection = collection
        self._storage_path = fs_dir

    def _filename(self, file_id):
        from os.path import join
        return join(self._storage_path, str(file_id))
    
    def open(self, file_id, *args, ** kwargs):
        return open(self._filename(file_id), * args, ** kwargs)

    def new_file(self, ** kwargs):
        from datetime import datetime
        kwargs.update({
            '_fs_dir': self._storage_path,
            '_fs_timestamp': datetime.now(),
        })
        file_id = self._collection.insert(kwargs)
        return self.open(file_id, 'wb')

    def find(self, spec = {}, *args, ** kwargs):
        import os
        docs = self._collection.find(spec = spec, fields = ['_id'], * args, ** kwargs)
        for doc in docs:
            file_id = doc['_id']
            fn = self._filename(file_id)
            if not os.path.isfile(fn):
                fn2 = os.path.join(doc['_fs_dir'], str(file_id))
                if not os.path.isfile(fn2):
                    raise FileNotFoundError(file_id)
                else:
                    fn = fn2
            yield StorageFileCursor(doc, fn)

#    def find_recent(self, spec = {}, * args, ** kwargs):
#        spec.update({
#            {'$orderby': {'_fs_timestamp': -1}})
#        return find

    def delete(self, file_id):
        import os
        os.remove(self._filename(file_id))
        self._collection.remove({'_id': file_id})
