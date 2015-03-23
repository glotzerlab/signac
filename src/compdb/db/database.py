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
import bson

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
    if isinstance(data, dict):
        if 'py/object' in data:
            msg = "Missing format definition for: '{}'."
            logger.warning(msg.format(data['py/object']))
    #import json
    #data = json.loads(binary.decode())
    return data

def generate_auto_network():
    from . import conversion
    from . import formats
    import networkx as nx
    network = nx.DiGraph()
    network.add_nodes_from(formats.BASICS)
    network.add_nodes_from(conversion.BasicFormat.registry.values())
    for adapter in conversion.Adapter.registry.values():
        logger.debug("Adding adapter '{}' to network.".format(adapter))
        conversion.add_adapter_to_network(
            network, adapter)
    return network

class Database(object):

    def __init__(self, db, config = None):
        from gridfs import GridFS
        if config is None:
            from ..core.config import load_config
            config = load_config()
        self._config = config
        self._db = db
        self._data = self._db['data']
        self._cache = self._db['cache']
        self._gridfs = GridFS(self._db)
        self._formats_network = generate_auto_network()

    @property
    def formats_network(self):
        return self._formats_network

    @formats_network.setter
    def formats_network(self, value):
        self._formats_network = value

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
                                self._formats_network,
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
                msg = "Could not apply method '{}' to '{}': {}"
                logger.debug(msg.format(method, src, error))
                #raise
            else:
                failed_application -= 1
                cache_doc = callable_spec(method)
                cache_doc[KEY_CACHE_DOC_ID] = doc['_id']
                try:
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
                except bson.errors.InvalidDocument as error:
                    msg = "Caching error: {}"
                    logger.warning(msg.format(error))

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
        cache_spec = callable_spec(method)
        cache_spec[KEY_CACHE_DOC_ID] = {'$in': list(doc_ids)}
        if PYMONGO_3:
            cached_docs = self._cache.find(
                filter = cache_spec, projection = [KEY_CACHE_DOC_ID])
        else:
            cached_docs = self._cache.find(
                spec = cache_spec, fields = [KEY_CACHE_DOC_ID])
        cached_ids = [doc[KEY_CACHE_DOC_ID] for doc in cached_docs]
        non_cached_ids = doc_ids.difference(cached_ids)
        self._update_cache(non_cached_ids, method)
        pipe = [ 
            {'$match': cache_spec},
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

    def _add_metadata_from_context(self, metadata):
        if not 'author_name' in metadata:
            metadata['author_name'] = self._config['author_name']
        if not 'author_email' in metadata:
            metadata['author_email'] = self._config['author_email']

    def _make_meta_document(self, metadata, data):
        import copy
        meta = copy.copy(metadata)
        meta[KEY_FILE_TYPE] = str(type(data))
        self._add_metadata_from_context(meta)
        return meta

    def _insert_one(self, metadata, data):
        meta = self._make_meta_document(metadata, data)
        binary = encode(data)
        file_id = self._gridfs.put(encode(data))
        meta[KEY_FILE_ID] = file_id
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

    def replace_one(self, document, replacement_data, upsert = False, * args, ** kwargs):
        import copy
        meta = self._make_meta_document(document, replacement_data)
        to_be_replaced = self._data.find_one(meta)
        file_id = self._gridfs.put(encode(replacement_data))
        replacement = copy.copy(meta)
        replacement[KEY_FILE_ID] = file_id
        try:
            if PYMONGO_3:
                return self._data.replace_one(
                    filter = meta,
                    replacement = replacement,
                    * args, ** kwargs)
            else:
                if to_be_replaced is not None:
                    replacement['_id'] = to_be_replaced['_id']
                return self._data.save(to_save = replacement)
        except:
            self._gridfs.delete(file_id)
            raise
        else:
            if to_be_replaced is not None:
                self._gridfs.delete(to_be_replaced[KEY_FILE_ID])

    def update_one(self, document, data, * args, ** kwargs):
        meta = self._make_meta_document(document, data)
        file_id = self._gridfs.put(encode(data))
        update = {'$set': {KEY_FILE_ID: file_id}}
        to_be_updated = self._data.find_one(meta)
        try:
            if PYMONGO_3:
                self._data.update_one(meta, update, * args, ** kwargs)
            else:
                self._data.update(meta, update, * args, ** kwargs)
        except:
            self._gridfs.delete(file_id)
        else:
            if to_be_updated is not None:
                self._gridfs.delete(to_be_updated[KEY_FILE_ID])

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
            self._formats_network, adapter)

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
