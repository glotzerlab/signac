import logging
logger = logging.getLogger('compdb.matdb')

COLLECTION_DATA = 'data'
COLLECTION_CACHE = 'cache'
KEY_CALLABLE_NAME = 'name'
KEY_CALLABLE_MODULE = 'module'
KEY_CALLABLE_MODULE_HASH = 'module_hash'
KEY_FILE_ID = 'file_id'
KEY_FILE_TYPE = 'file_type'
KEY_CACHE_DOC_ID = 'doc_id'
KEY_CACHE_RESULT = 'result'
KEY_DOC_META = 'meta'
KEY_DOC_DATA = 'data'

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

def hash_module(c):
    import inspect, hashlib
    module = inspect.getmodule(c)
    src_file = inspect.getsourcefile(module)
    m = hashlib.md5()
    with open(src_file, 'rb') as file:
        m.update(file.read())
    return m.hexdigest()

def callable_name(c):
    try:
        return c.__name__
    except AttributeError:
        return c.name()

def callable_spec(c):
    assert callable(c)
    spec = {
        KEY_CALLABLE_NAME: callable_name(c),
        KEY_CALLABLE_MODULE: c.__module__,
        KEY_CALLABLE_MODULE_HASH: hash_module(c),
    }
    return spec

def encode(data):
    import jsonpickle
    binary = jsonpickle.encode(data).encode()
    #import json
    #binary = json.dumps(data).encode()
    return binary

def decode(data):
    import jsonpickle
    data = jsonpickle.decode(data.decode())
    #import json
    #data = json.loads(binary.decode())
    return data

class Database(object):

    def __init__(self, db, adapter_network = None):
        from gridfs import GridFS
        self._db = db
        self._data = self._db['data']
        self._cache = self._db['cache']
        self._gridfs = GridFS(self._db)
        self._adapter_network = adapter_network

    def _update_cache(self, doc_ids, method):
        from . import conversion
        docs = self._data.find({'_id': {'$in': list(doc_ids)}})
        failed_application = docs.count()
        for doc in docs:
            try:
                src = self._get(doc[KEY_FILE_ID])
                if isinstance(method, conversion.DBMethod):
                    if not isinstance(src, method.expects):
                        msg = "Trying to convert from '{}' to '{}'."
                        logger.debug(msg.format(type(src), method.expects))
                        try:
                            converter = conversion.get_converter(
                                self._adapter_network,
                                type(src), method.expects)
                            msg = "Found conversion path: {} nodes."
                            logger.debug(msg.format(len(converter)))
                            src_converted = converter.convert(src)
                        except conversion.ConversionError as error:
                            msg = "Failed. Trying implicit conversion."
                            logger.debug(msg)
                            try:
                                src_converted = method.expects(src)
                            except:
                                raise error
                        else:
                            src = src_converted
                        logger.debug('Success.')
                result = method(src)
            except conversion.ConversionError as error:
                msg = "Failed to convert form '{}' to '{}'."
                logger.debug(msg.format(* error.args))
            except Exception as error:
                msg = "Could not apply method '{}' to '{}': {}."
                logger.debug(msg.format(method, src, error))
                #raise
            else:
                failed_application -= 1
                cache_doc = callable_spec(method)
                cache_doc[KEY_CACHE_DOC_ID] = doc['_id']
                if PYMONGO_3:
                    self._cache.update_one(
                        filter = cache_doc,
                        update = {'$set': {KEY_CACHE_RESULT: result}},
                        upsert = True)
                else:
                    self._cache.update(
                        spec = cache_doc,
                        document = {'$set': {KEY_CACHE_RESULT: result}},
                        upsert = True)
        if failed_application > 0:
            msg = "Number of failed applications of '{}': {}."
            logger.warning(msg.format(method, failed_application))

    def _split_filter(self, filter):
        if filter is None:
            return None, None
        else:
            standard_filter = {}
            methods_filter = {}
            for key, value in filter.items():
                if callable(key):
                    methods_filter[key] = value
                else:
                    standard_filter[key] = value
            return standard_filter, methods_filter

    def _filter_by_method(self, doc_ids, method, expression):
        cached_docs = self._cache.find(
            {KEY_CACHE_DOC_ID: {'$in': list(doc_ids)}},
            projection = [KEY_CACHE_DOC_ID])
        cached_ids = [doc[KEY_CACHE_DOC_ID] for doc in cached_docs]
        non_cached_ids = doc_ids.difference(cached_ids)
        self._update_cache(non_cached_ids, method)
        pipe = [ 
            {'$match': {KEY_CACHE_DOC_ID: {'$in': list(doc_ids)}}},
            {'$project': {
                '_id': '$' + KEY_CACHE_DOC_ID,
                'result': '$' + KEY_CACHE_RESULT,
            }},
            {'$match': {KEY_CACHE_RESULT: expression}},
            {'$project': {'_id': '$_id'}},
            ]
        result = self._cache.aggregate(pipe)
        if PYMONGO_3:
            return set(doc['_id'] for doc in result)
        else:
            return set(doc['_id'] for doc in result['result'])

    def _filter_by_methods(self, docs, methods_filter):
        matching = set(doc['_id'] for doc in docs)
        for method, value in methods_filter.items():
            matching = self._filter_by_method(matching, method, value)
        return matching

    def _find_with_methods(self, filter = None, * args, ** kwargs):
        if filter is None:
            return self._data.find(filter, * args, ** kwargs)

        standard_filter, methods_filter = self._split_filter(filter)
        if methods_filter:
            if PYMONGO_3:
                docs = self._data.find(
                    filter = standard_filter, 
                    projection = ['_id'])
            else:
                docs = self._data.find(
                    spec = standard_filter,
                    fields = ['_id'])
            filtered = self._filter_by_methods(docs, methods_filter)
            if PYMONGO_3:
                return self._data.find(
                    filter = {'_id': {'$in': list(filtered)}},
                    * args, ** kwargs)
            else:
                return self._data.find(
                    spec = {'_id': {'$in': list(filtered)}},
                    * args, ** kwargs)
        else:
            if PYMONGO_3:
                return self._data.find(
                    filter = standard_filter, 
                    * args, ** kwargs)
            else:
                return self._data.find(
                    spec = standard_filter,
                    * args, ** kwargs)

    def _find_one_with_methods(self, filter = None, *args, **kwargs):
        if filter is None:
            return self._data.find_one(filter, * args, ** kwargs)
        standard_filter, methods_filter = self._split_filter(filter)
        doc = self._data.find_one(standard_filter, * args, ** kwargs)
        if len(methods_filter):
            filtered = self._filter_by_methods([doc], methods_filter)
            if doc['_id'] in filtered:
                return doc
            else:
                return None
        else:
            return doc

    def _insert_one(self, metadata, data):
        import copy
        meta = copy.copy(metadata)
        binary = encode(data)
        file_id = self._gridfs.put(encode(data))
        meta.update({
            KEY_FILE_ID: file_id,
            KEY_FILE_TYPE: str(type(data))
            })
        if PYMONGO_3:
            return self._data.insert_one(meta)
        else:
            return self._data.insert(meta)

    def _resolve_filter(self, filter):
        return filter

    def _find_one(self, filter = None):
        doc = self._data.find_one(self._resolve_filter(filter))
        file_id = doc[KEY_FILE_ID]
        return self._get(file_id)

    def _get(self, file_id):
        grid_file = self._gridfs.get(file_id)
        return decode(grid_file.read())

    def _result_from_doc(self, doc):
        result = dict(doc)
        result['data'] = self._get(result[KEY_FILE_ID])
        del result[KEY_FILE_ID]
        return result

    def insert_one(self, document, data, * args, ** kwargs):
        self._insert_one(document, data, * args, ** kwargs)

    def find(self, * args, ** kwargs):
        docs = self._find_with_methods(* args, ** kwargs)
        for doc in docs:
            yield self._result_from_doc(doc)

    def find_one(self, filter_or_id, * args, ** kwargs):
        if isinstance(filter_or_id, dict):
            doc = self._find_one_with_methods(
                filter_or_id, *args, ** kwargs)
            if doc is None:
                return None
            else:
                return self._result_from_doc(doc)
        else:
            doc = self._data.find_one(filter_or_id, *args, **kwargs)
            return self._result_from_doc(doc)

    def _delete_doc(self, doc):
        self._gridfs.delete(doc[KEY_FILE_ID])
        if PYMONGO_3:
            self._cache.delete_many({KEY_FILE_ID: doc['_id']})
        else:
            self._cache.remove({KEY_FILE_ID: doc['_id']})

    def add_adapter(self, adapter):
        from . import conversion
        conversion.add_adapter_to_network(
            self._adapter_network, adapter)

    def delete_one(self, filter, * args, ** kwargs):
        #doc = self._data.find_one_and_delete(filter, *args, **kwargs)
        doc = self._data.find_one(filter, *args, **kwargs)
        self._data.remove({'_id': doc['_id']})
        self._delete_doc(doc)

    def delete_many(self, filter, * args, ** kwargs):
        docs = self._data.find(filter, *args, ** kwargs)
        for doc in docs:
            self._delete_doc(doc)
        if PYMONGO_3:
            result = self._data.delete_many(filter)
        else:
            result = self._data.remove(filter)
        return result
