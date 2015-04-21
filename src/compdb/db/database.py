import logging
logger = logging.getLogger('compdb.db.database')

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3
import bson
import uuid

COLLECTION_DATA = 'compdb_data'
COLLECTION_CACHE = 'compdb_cache'
KEY_CALLABLE_NAME = 'name'
KEY_CALLABLE_MODULE = 'module'
KEY_CALLABLE_SOURCE_HASH = 'source_hash'
KEY_CALLABLE_MODULE_HASH = 'module_hash'
KEY_FILE_ID = '_file_id'
KEY_FILE_TYPE = '_file_type'
#KEY_GROUP_FILES = '_file_ids'
KEY_CACHE_DOC_ID = 'doc_id'
KEY_CACHE_RESULT = 'result'
KEY_CACHE_COUNTER = 'counter'
KEY_DOC_META = 'meta'
KEY_DOC_DATA = 'data'

ILLEGAL_AGGREGATION_KEYS = ['$group', '$out']

def hash_module(c):
    import inspect, hashlib
    module = inspect.getmodule(c)
    src_file = inspect.getsourcefile(module)
    m = hashlib.md5()
    with open(src_file, 'rb') as file:
        m.update(file.read())
    return m.hexdigest()

def hash_source(c):
    import inspect, hashlib
    m = hashlib.md5()
    m.update(inspect.getsource(c).encode())
    return m.hexdigest()

def callable_name(c):
    try:
        return c.__name__
    except AttributeError:
        return c.name()

def callable_spec(c):
    import inspect
    assert callable(c)
    try:
        spec = {
            KEY_CALLABLE_NAME: callable_name(c),
            KEY_CALLABLE_SOURCE_HASH: hash_source(type(c)),
        }
    except TypeError:
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
            logger.debug(msg.format(data['py/object']))
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
        logger.debug("Adding '{}' to network.".format(adapter()))
        conversion.add_adapter_to_network(
            network, adapter)
    return network

class UnsupportedExpressionError(ValueError):
    pass

class FileCursor(object):

    def __init__(self, db, call_dict):
        self._db = db
        self._call_dict = call_dict

    def __call__(self, cursor):
        from . import conversion
        try:
            return self._db._resolve_doc(cursor, self._call_dict)
        except conversion.NoConversionPath:
            pass
        except conversion.ConversionError as error:
            msg = "Conversion error for doc with '{}': {}"
            logger.warning(msg.format(cursor['_id'], error))
            raise
        return {}

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
        self.debug_mode = False

    @property
    def formats_network(self):
        return self._formats_network

    @formats_network.setter
    def formats_network(self, value):
        self._formats_network = value

    def _convert_src(self, src, method):
        from . import conversion
        if isinstance(method, conversion.DBMethod):
            try:
                isinstance(src, method.expects)
            except TypeError:
                msg = "Illegal expect type: '{}'."
                raise TypeError(msg.format(method.expects))
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
                #except conversion.ConversionError as error:
                except conversion.NoConversionPath as error:
                    msg = "No path found. Trying implicit conversion."
                    logger.debug(msg)
                    try:
                        src_converted = method.expects(src)
                    except:
                        raise error
                else:
                    src = src_converted
                logger.debug('Success.')
        return src

    def _update_cache(self, doc_ids, method):
        from . import conversion
        docs = self._data.find({'_id': {'$in': list(doc_ids)}})
        records_skipped = docs.count()
        conversion_errors = 0
        no_conversion_path = 0
        for doc in docs:
            try:
                if not KEY_FILE_ID in doc:
                    continue
                src = self._get(doc[KEY_FILE_ID])
                src = self._convert_src(src, method)
                try:
                    result = method(src)
                except Exception as error:
                    raise RuntimeError(error)
            except conversion.NoConversionPath as error:
                no_conversion_path += 1
                msg = "No path to convert from '{}' to '{}'."
                logger.debug(msg.format(* error.args))
            except conversion.ConversionError as error:
                conversion_errors += 1
                msg = "Failed to convert form '{}' to '{}'."
                logger.debug(msg.format(* error.args))
            except RuntimeError as error:
                msg = "Could not apply method '{}' to '{}': {}"
                if len(str(src)) > 80:
                    src = str(src)[:80] + '...'
                logger.debug(msg.format(method, src, error))
                if self.debug_mode:
                    raise
            else:
                records_skipped -= 1
                cache_doc = callable_spec(method)
                cache_doc[KEY_CACHE_DOC_ID] = doc['_id']
                try:
                    update = {
                        '$set': {KEY_CACHE_RESULT: result},
                        '$setOnInsert': {KEY_CACHE_COUNTER: 0},
                    }
                    if PYMONGO_3:
                        self._cache.update_one(
                            filter = cache_doc,
                            update = update,
                            upsert = True)
                    else:
                        self._cache.update(
                            spec = cache_doc,
                            document = update,
                            upsert = True)
                except bson.errors.InvalidDocument as error:
                    msg = "Caching error: {}"
                    logger.warning(msg.format(error))
                    raise TypeError(error) from error
        if conversion_errors or records_skipped or no_conversion_path:
            msg = "{m}:"
            logger.debug(msg.format(m = method))
        if no_conversion_path > 0:
            msg = "# no conversion paths: {n}"
            logger.debug(msg.format(m = method, n = records_skipped))
        if conversion_errors > 0:
            msg = "# failed conversions: {n}"
            logger.debug(msg.format(m = method, n = records_skipped))
        if records_skipped > 0:
            msg = "# records skipped: {n}"
            logger.debug(msg.format(m = method, n = records_skipped))

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
        try:
            self._update_cache(non_cached_ids, method)
        except TypeError as error:
            msg = "Failed to process filter '{f}': {e}"
            f = {method: expression}
            raise TypeError(msg.format(f = f,e = error)) from error
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
        counter_update = {'$inc': {KEY_CACHE_COUNTER: 1}}
        if PYMONGO_3:
            self._cache.update_many(cache_spec, counter_update)
            return set(doc['_id'] for doc in result)
        else:
            self._cache.update(cache_spec, counter_update)
            return set(doc['_id'] for doc in result['result'])

    def _filter_by_methods(self, docs, methods_filter):
        matching = set(doc['_id'] for doc in docs)
        for method, value in methods_filter.items():
            matching = self._filter_by_method(matching, method, value)
        msg = "Record methods coverage: {:.2%} (records skipped: {})"
        skipped = docs.count() - len(matching)
        coverage = float(len(matching) / docs.count())
        logger.info(msg.format(coverage, skipped))
        return matching

    def _add_metadata_from_context(self, metadata):
        if not 'author_name' in metadata:
            metadata['author_name'] = self._config['author_name']
        if not 'author_email' in metadata:
            metadata['author_email'] = self._config['author_email']

    def _make_meta_document(self, metadata, data):
        import copy
        meta = copy.copy(metadata)
        if data is not None:
            meta[KEY_FILE_TYPE] = str(type(data))
        self._add_metadata_from_context(meta)
        return meta

    def _put_file(self, data):
        return self._gridfs.put(encode(data))

    def _insert_one(self, metadata, data):
        meta = self._make_meta_document(metadata, data)
        if data is not None:
            file_id = self._put_file(data)
            meta[KEY_FILE_ID] = file_id
        if PYMONGO_3:
            return self._data.insert_one(meta)
        else:
            return self._data.insert(meta)

    def _get(self, file_id):
        grid_file = self._gridfs.get(file_id)
        return decode(grid_file.read())

    def _resolve_files(self, doc):
        result = dict(doc)
        if KEY_FILE_ID in result:
            result[KEY_DOC_DATA] = self._get(result[KEY_FILE_ID])
            del result[KEY_FILE_ID]
        #if KEY_GROUP_FILES in result:
        #    result[KEY_GROUP_FILES] = [self._get(k) for k in result[KEY_GROUP_FILES]]
        #    del result[KEY_GROUP_FILES]
        return result

    def insert_one(self, document, data = None, * args, ** kwargs):
        self._insert_one(document, data, * args, ** kwargs)

    def replace_one(self, filter, replacement_data = None, upsert = False, * args, ** kwargs):
        import copy
        meta = self._make_meta_document(filter, replacement_data)
        to_be_replaced = self._data.find_one(meta)
        replacement = copy.copy(meta)
        if replacement_data is not None:
            file_id = self._put_file(replacement_data)
            replacement[KEY_FILE_ID] = file_id
        try:
            if PYMONGO_3:
                result = self._data.replace_one(
                    filter = meta,
                    replacement = replacement,
                    * args, ** kwargs)
            else:
                if to_be_replaced is not None:
                    replacement['_id'] = to_be_replaced['_id']
                result = self._data.save(to_save = replacement)
        except:
            if replacement_data is not None:
                self._gridfs.delete(file_id)
            raise
        else:
            if to_be_replaced is not None:
                if KEY_FILE_ID in to_be_replaced:
                    self._gridfs.delete(to_be_replaced[KEY_FILE_ID])
            return result

    def update_one(self, document, data = None, * args, ** kwargs):
        meta = self._make_meta_document(document, data)
        if data is not None:
            file_id = self._put_file(data)
            update = {'$set': {KEY_FILE_ID: file_id}}
        to_be_updated = self._data.find_one(meta)
        try:
            if PYMONGO_3:
                self._data.update_one(meta, update, * args, ** kwargs)
            else:
                self._data.update(meta, update, * args, ** kwargs)
        except:
            if data is not None:
                self._gridfs.delete(file_id)
        else:
            if to_be_updated is not None:
                if KEY_FILE_ID in to_be_updated:
                    self._gridfs.delete(to_be_updated[KEY_FILE_ID])

    def find(self, filter = None, projection = None, * args, ** kwargs):
        call_dict = dict()
        plain_filter = self._resolve(filter, call_dict)
        docs = self._data.find(
            plain_filter, projection, * args, ** kwargs)
        return map(FileCursor(self, call_dict), docs)

    def find_one(self, filter_or_id, * args, ** kwargs):
        call_dict = dict()
        plain_filter_or_id = self._resolve(filter_or_id, call_dict)
        doc = self._data.find_one(plain_filter_or_id, * args, ** kwargs)
        if doc is not None:
            doc = self._resolve_doc(doc, call_dict)
        return doc

    def _resolve_doc(self, doc, call_dict):
        return self._resolve_calls(self._resolve_files(doc), call_dict)

    def resolve(self, docs):
        for doc in docs:
            yield self._resolve_files(doc)

    def _delete_doc(self, doc):
        if KEY_FILE_ID in doc:
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

    def _resolve_dict(self, d, call_dict, * args, ** kwargs):
        standard = dict()
        methods_filter = dict()
        #methods_projection = dict()
        for key, value in d.items():
            if callable(key):
                assert not callable(value)
                methods_filter[key] = self._resolve(value, call_dict)
            elif key == '$project':
                value[KEY_FILE_ID] = '$'+KEY_FILE_ID
                standard[key] = value
            elif key.startswith('$') and key in ILLEGAL_AGGREGATION_KEYS:
                raise UnsupportedExpressionError(key)
            else:
                standard[key] = self._resolve(value, call_dict)
        if PYMONGO_3:
            docs = self._data.find(filter=standard,projection=['_id'])
        else:
            docs = self._data.find(spec = standard, fields = ['_id'])
        if methods_filter:
            filtered = self._filter_by_methods(docs, methods_filter)
            return {'_id': {'$in': list(filtered)}}
        else:
            return d

    def _resolve(self, expr, call_dict, *args, **kwargs):
        if isinstance(expr, dict):
            plain = {k: self._resolve(v, call_dict, * args, ** kwargs)
                    for k,v in expr.items()}
            return self._resolve_dict(plain, call_dict, * args, ** kwargs)
        elif isinstance(expr, list):
            return [self._resolve(v, call_dict, *args, **kwargs) for v in expr]
        elif callable(expr):
            call_id = str(uuid.uuid4())
            call_dict[call_id] = expr
            return {'$literal': "$CALL({})".format(call_id)}
        else:
            return expr

    def _resolve_stage(self, stage, call_dict):
        return self._resolve(stage, call_dict)

    def _resolve_pipeline(self, pipeline, call_dict):
        for stage in pipeline:
            yield self._resolve_stage(stage, call_dict)

    def _resolve_calls(self, result, call_dict, data = None):
        if isinstance(result, dict):
            if KEY_FILE_ID in result:
                data = self._get(result[KEY_FILE_ID])
            elif KEY_DOC_DATA in result:
                data = result[KEY_DOC_DATA]
            #elif KEY_GROUP_FILES in result:
            #    data = result[KEY_GROUP_FILES]
            return {self._resolve_calls(k, call_dict, data):
                        self._resolve_calls(v, call_dict, data)
                for k, v in result.items()}
        elif isinstance(result, list):
            return [self._resolve_calls(v, call_dict, data) for v in result]
        elif isinstance(result, str):
            if result.startswith('$CALL('):
                method = call_dict[result[6:-1]]
                if data is None:
                    msg = "Unable to resolve function call in expression."
                    return None
                    raise RuntimeError(msg)
                elif isinstance(data, list):
                    return [method(self._convert_src(d, method)) for d in data]
                else:
                    src = self._convert_src(data, method)
                    return method(src)
            else:
                return result
        else:
            return result

    def aggregate(self, pipeline, ** kwargs):
        call_dict = dict()
        plain_pipeline = list(self._resolve_pipeline(pipeline, call_dict))
        logger.debug("Pipeline expression: '{}'.".format(plain_pipeline))
        result = self._data.aggregate(plain_pipeline, ** kwargs)
        if PYMONGO_3:
            return filter(len, map(FileCursor(self, call_dict), result))
        else:
            return filter(len, map(FileCursor(self, call_dict), result['result']))
