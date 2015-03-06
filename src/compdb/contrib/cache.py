import logging
logger = logging.getLogger('cache')

CACHE_KEY = 'compdb_cache'
CACHE_DIR = '.cache'

class Cache(object):
    
    def __init__(self, project):
        self._project = project
        #self._check()

    def _collection(self):
        return self._project.get_project_db()[CACHE_KEY]

    def _cache_dir(self):
        from os.path import join
        return join(self._project.filestorage_dir(), CACHE_DIR)

    def _fn(self, name):
        from os.path import join
        return join(self._cache_dir(), name)

    def _store_in_cache(self, doc, data):
        import pickle, os
        try:
            logger.debug("Trying to cache results.")
            blob = pickle.dumps(data)
            id_ = self._collection().save(doc)
            logger.debug('id_: {}'.format(id_))
            if not os.path.isdir(self._cache_dir()):
                os.mkdir(self._cache_dir())
            logger.debug("Storing in '{}'.".format(self._fn(str(id_))))
            with open(self._fn(str(id_)), 'wb') as cachefile:
                pickle.dump(data, cachefile)
        finally:
            return data

    def _hash_function(self, function):
        import hashlib, inspect
        code = inspect.getsource(function)
        m = hashlib.md5()
        m.update(code.encode())
        return m.hexdigest()

    def _code_is_identical(self, function, doc):
        assert 'code' in doc
        return self._hash_function(function) == doc['code']

    def _load_from_cache(self, name):
        import pickle
        logger.debug("Loading from '{}'.".format(self._fn(name)))
        with open(self._fn(name), 'rb') as cachefile:
            return pickle.load(cachefile)

    def _is_cached(self, spec):
        try:
            doc = self._collection().find_one(spec)
        except InvalidDocument as error:
            raise RuntimeError("Failed to encode function arguments.") from error
        else:
            return doc is not None

    def run(self, function, * args, ** kwargs):
        import inspect, pickle
        from bson.errors import InvalidDocument
        from . hashing import generate_hash_from_spec

        signature = str(inspect.signature(function))
        arguments = inspect.getcallargs(function, *args, ** kwargs)
        code = inspect.getsource(function)
        logger.debug("Cached function call for '{}{}'.".format(
            function.__name__, signature))
        spec = {
            'name': function.__name__,
            'module': function.__module__,
            'signature': signature,
            'arguments': generate_hash_from_spec(arguments),
        }
        doc = self._collection().find_one(spec)
        if doc is not None:
            if self._code_is_identical(function, doc):
                logger.debug("Results found. Trying to load.")
                try:
                    return self._load_from_cache(str(doc['_id']))
                except FileNotFoundError:
                    logger.debug("Error while loading.")
                    self._check()
                    spec.update({'code': self._hash_function(function)})
                    result = function(* args, ** kwargs)
                    return self._store_in_cache(spec, result)
        # No retrieval possible
        logger.debug("No results found. Executing...")
        result = function(* args, ** kwargs)
        spec.update({'code': self._hash_function(function)})
        return self._store_in_cache(spec, result)
            
    def _check(self):
        import os
        docs = self._collection().find()
        remove = []
        for doc in docs:
            fn = self._fn(str(doc['_id']))
            if not os.path.isfile(fn):
                remove.append(doc['_id'])
        self._collection().remove(
            {'_id': {'$in': remove}})
        if len(remove):
            msg = "Removed link to '{}'. File(s) not found."
            logger.warning(msg.format([str(i) for i in remove]))

    def clear(self):
        import os
        docs = self._collection().find()
        for doc in docs:
            try: 
                fn = self._fn(str(doc['_id']))
                os.remove(fn)
            except FileNotFoundError as error:
                pass
        self._collection().drop()

