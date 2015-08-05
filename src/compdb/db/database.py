"""Main module for the Database implementation.

This module contains the implementation of the CompMatDB API.

See also: https://bitbucket.org/glotzer/compdb/wiki/latest/compmatdb
"""

import logging
import warnings
import copy
import uuid
import hashlib
import inspect
import collections

import pymongo
import bson
import jsonpickle
import networkx as nx
from gridfs import GridFS

from ..core.config import load_config
from . import conversion, formats

logger = logging.getLogger(__name__)

PYMONGO_3 = pymongo.version_tuple[0] == 3

COLLECTION_DATA = 'compdb_data'
COLLECTION_CACHE = 'compdb_cache'
KEY_CALLABLE_NAME = 'name'
KEY_CALLABLE_MODULE = 'module'
KEY_CALLABLE_SOURCE_HASH = 'source_hash'
KEY_CALLABLE_MODULE_HASH = 'module_hash'
KEY_FILE_ID = '_file_id'
KEY_FILE_TYPE = '_file_type'
KEY_PROJECT_ID = 'project'
LEGACY_RECORD = 'legacy_file'
#KEY_GROUP_FILES = '_file_ids'
KEY_CACHE_DOC_ID = 'doc_id'
KEY_CACHE_RESULT = 'result'
KEY_CACHE_COUNTER = 'counter'
KEY_DOC_META = 'meta'
KEY_DOC_DATA = 'data'

ILLEGAL_AGGREGATION_KEYS = ['$group', '$out']
USER_KEYS = ('author_name', 'author_email')

def hash_module(c):
    "Calculate hash value for the module of c."
    module = inspect.getmodule(c)
    src_file = inspect.getsourcefile(module)
    m = hashlib.md5()
    with open(src_file, 'rb') as file:
        m.update(file.read())
    return m.hexdigest()

def hash_source(c):
    "Calculate hash value for the source code of c."
    m = hashlib.md5()
    m.update(inspect.getsource(c).encode())
    return m.hexdigest()

def callable_name(c):
    "Return a unique name for c."
    try:
        return c.__name__
    except AttributeError:
        return c.name()

def callable_spec(c):
    """Generate a mappable, which can be used as filter for c.

    This function is primarily used to query cached result values for c.
    """
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
    "Encode data in JSON."
    binary = jsonpickle.encode(data).encode()
    #binary = json.dumps(data).encode()
    return binary

def decode(data):
    "Decode data, which is expected to be in JSON format."
    data = jsonpickle.decode(data.decode())
    if isinstance(data, dict):
        if 'py/object' in data:
            msg = "Missing format definition for: '{}'."
            logger.debug(msg.format(data['py/object']))
    #data = json.loads(binary.decode())
    return data

def generate_auto_network():
    """Generate a formats network from all registered formats and adapters.
    Every adapter in the global namespace is automatically registered.
    """
    network = nx.DiGraph()
    network.add_nodes_from(formats.BASICS)
    network.add_nodes_from(conversion.BasicFormat.registry.values())
    for adapter in conversion.Adapter.registry.values():
        logger.debug("Adding '{}' to network.".format(adapter()))
        conversion.add_adapter_to_network(
            network, adapter)
    return network

def is_standard_expression(expr):
    """Return True if expr is a standard expression, otherwise False."""
    def search_callable(expr):
        if isinstance(expr, dict):
            return {search_callable(k): search_callable(v)
                for k, v in expr.items()}
        elif isinstance(expr, list):
            return [search_callable(v) for v in expr]
        elif callable(expr):
            raise ValueError()
        else:
            return expr
    try:
        search_callable(expr)
    except ValueError:
        return False
    else:
        return True

def split_standard_nonstandard(expr):
    """Split expression into standard and non-standard as far as possible."""
    if isinstance(expr, dict):
        s = dict()
        ns = dict()
        for k, v in expr.items():
            if is_standard_expression(k) and is_standard_expression(v):
                s[k] = v
            else:
                ns[k] = v
        return s, ns
    if isinstance(expr, list):
        s = list()
        ns = list()
        for v in expr:
            if is_standard_expression(v):
                s.append(v)
            else:
                ns.append(v)
        return s, ns
    else:
        if is_standard_expression(expr):
            return expr, None
        else:
            return None, expr

class UnsupportedExpressionError(ValueError):
    """This exception is raised when a legal MongoDB expression is not supported."""
    pass

class AuthorizationError(RuntimeError):
    """This exception is raised when a database entries are unaccessable with the authorized user's access rights."""
    pass

class _BaseCursor(object):

    def __init__(self, db, mongo_cursor, call_dict, projection = None):
        self._db = db
        self._mongo_cursor = mongo_cursor
        self._call_dict = call_dict
        self._projection = projection
        self._skipped_auth = collections.defaultdict(int)

    def next(self):
        try:
            doc = next(self._mongo_cursor)
        except pymongo.errors.OperationFailure as error:
            if 'not authorized' in str(error):
                raise AuthorizationError("Not authorized to query database.")
        else:
            try:
                return self._db._resolve_doc(doc, self._call_dict, self._projection)
            except conversion.NoConversionPath:
                pass
            except conversion.ConversionError as error:
                msg = "Conversion error for doc with id '{}': {}"
                logger.warning(msg.format(doc['_id'], error))
                raise
            except AuthorizationError as error:
                self._skipped_auth[doc['project']] += 1
                msg = "Skipping record from project '{project}': {cause}"
                logger.warn(msg.format(project=doc['project'], cause='not authorized'))
                return next(self)

    __next__ = next

    def __iter__(self):
        return self

class Cursor(_BaseCursor):
    """Iterator over database results.

    This class should not be instantiated by developers directly.
    See find() and aggregate() instead.
    """

    def count(self):
        return self._mongo_cursor.count()

    def rewind(self):
        "Reset the cursor to its unevaluated state."
        return self._mongo_cursor.rewind()

class CommandCursor(_BaseCursor):
    """Iterator over database command results.

    This class should not be instantiated by developers directrly.
    See aggregate() instead.
    """
    pass

class Database(object):
    """The CompMatDB base class.

    This object provides the CompMatDB API.
    """

    def __init__(self, db, get_gridfs, config = None):
        """Create a new Database object.

        :param db: The MongoDB backend database object.
        :param config: A compdb config object.

        This function should not be called directly.
        See compdb.db.access_compmatdb() instead.
        """
        if config is None:
            config = load_config()
        self._config = config
        self._client = None
        self._db = db
        self._data = self._db['data']
        self._cache = self._db['cache']
        self._gridfs_callback = get_gridfs
        #self._gridfs = GridFS(self._db)
        self._formats_network = generate_auto_network()
        self.debug_mode = False

    @property
    def config(self):
        "Returns the config instance associated with this database."
        return self._config

    @property
    def formats_network(self):
        "Returns the formats and adapter network."
        return self._formats_network

    @formats_network.setter
    def formats_network(self, value):
        "Set the formats and adapter network."
        self._formats_network = value

    def _get_gridfs(self, project_id):
        if project_id == LEGACY_RECORD:
            warnings.warn("Encountered legacy record!")
            return GridFS(self._db)
        else:
            return self._gridfs_callback(project_id)

    def _convert_src(self, src, method):
        """Convert the :param src: object to a type expected by :param method:.

        :param src: Arbitrary data type.
        :param method: A python callable.

        If :param method: is of type conversion.DBMethod, this function 
        will attempt to convert :param src: to the :param method: expects type.
        """
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
        """Attempt to update the cache for results of :param method: applied to data storen in documents with :param doc_ids:.

        :param doc_ids: Document ids holding data.
        :type doc_ids: A iterable of ObjectID.
        :param method: The method to apply.
        :type method: A python callable.
        """
        docs = self._data.find({'_id': {'$in': list(doc_ids)}})
        records_skipped = docs.count()
        conversion_errors = 0
        no_conversion_path = 0
        for doc in docs:
            try:
                if not KEY_FILE_ID in doc:
                    continue
                src = self._get_file(doc[KEY_FILE_ID], doc[KEY_PROJECT_ID])
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
        "Split filter into a standard MongoDB filter and a methods filter, which is locally resolved."
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
        """Apply method to data in document with doc_ds and filter by expression.

        :param doc_ids: Documents to apply the filter to.
        :param method: The method to apply.
        :param expression: The query expression used for filtering.
        """
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
        """Apply all methods in methods_filter to all docs.

        See also: _filter_by_method(..)
        """
        matching = set(doc['_id'] for doc in docs)
        for method, value in methods_filter.items():
            matching = self._filter_by_method(matching, method, value)
        msg = "Record methods coverage: {:.2%} (records skipped: {})"
        skipped = docs.count() - len(matching)
        if docs.count():
            coverage = float(len(matching) / docs.count())
        else:
            coverage = 0
        logger.info(msg.format(coverage, skipped))
        return matching

    def _metadata_from_context(self):
        return {
            'author_name': self._config['author_name'],
            'author_email': self._config['author_email'],
            }

    def _filter_by_user(self, filter):
        """Make the filter user-specific to protect against global data manipulation."""
        if self._config.get('compmatdb_admin', False):
            warnings.warn("Working with compmatdb admin role!")
        msg = "The value for '{key}' cannot be modified without admin rights!"
        for key in USER_KEYS:
            if key in filter:
                if filter[key] != self._config[key]:
                    raise KeyError(msg.format(key=key))
            filter[key] = self._config[key]

    def _add_metadata_from_context(self, metadata):
        "Add implicit meta data to the explicitely provided meta data."
        if not 'author_name' in metadata:
            metadata['author_name'] = self._config['author_name']
        if not 'author_email' in metadata:
            metadata['author_email'] = self._config['author_email']
        if not KEY_PROJECT_ID in metadata:
            metadata[KEY_PROJECT_ID] = self._config[KEY_PROJECT_ID]

    def _make_meta_document(self, metadata, data):
        "Generate the records document from metadata and data."
        meta = copy.copy(metadata)
        if data is not None:
            meta[KEY_FILE_TYPE] = str(type(data))
        self._add_metadata_from_context(meta)
        return meta

    def _put_file(self, data, project_id):
        "Store :param data: in a gridfs file."
        return self._get_gridfs(project_id).put(encode(data))
        #return self._gridfs.put(encode(data))

    def _insert_one(self, metadata, data):
        "Insert a record associating metadata and data."
        meta = self._make_meta_document(metadata, data)
        if data is not None:
            file_id = self._put_file(data, meta[KEY_PROJECT_ID])
            meta[KEY_FILE_ID] = file_id
        if PYMONGO_3:
            return self._data.insert_one(meta)
        else:
            return self._data.insert(meta)

    def _get_file(self, file_id, project_id):
        "Retrieve file with :param file_id: from the gridfs collection."
        grid_file = self._get_gridfs(project_id).get(file_id)
        #grid_file = self._gridfs.get(file_id)
        return decode(grid_file.read())

    def _resolve_files(self, doc):
        "Resolve the file data associated with doc."
        result = dict(doc)
        if KEY_FILE_ID in result:
            result[KEY_DOC_DATA] = self._get_file(result[KEY_FILE_ID], result.get(KEY_PROJECT_ID, LEGACY_RECORD))
            del result[KEY_FILE_ID]
        #if KEY_GROUP_FILES in result:
        #    result[KEY_GROUP_FILES] = [self._get(k) for k in result[KEY_GROUP_FILES]]
        #    del result[KEY_GROUP_FILES]
        return result

    def insert_one(self, document, data = None, * args, ** kwargs):
        """Insert a new document into the database.

        :param document: The metadata to be inserted into the database.
        :param data: The data to associate with this record.

        See also: 
            - https://bitbucket.org/glotzer/compdb/wiki/latest/compmatdb_part2
            - http://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_one
        """
        self._filter_by_user(document)
        self._insert_one(document, data, * args, ** kwargs)

    def replace_one(self, filter, replacement_data = None, upsert = False, * args, ** kwargs):
        """Replace a document in the database.
        
        :param filter: The first document to match the filter will be replaced. The filter itself is the replacement metadata.
        :type filter: A mapping type.
        :param replacement_data: The replacement data.
        :type replacement_data: Arbitrary binary data.
        :param upsert: If true and no document matches the filter, a new document will be inserted into the database.
        :type upsert: Boolean

        See also: 
            - https://bitbucket.org/glotzer/compdb/wiki/latest/compmatdb_part2
            - http://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.replace_one
        """
        self._filter_by_user(filter) # Modify the filter to only match user documents
        meta = self._make_meta_document(filter, replacement_data)
        to_be_replaced = self._data.find_one(filter)
        replacement = copy.copy(meta)
        if replacement_data is not None:
            file_id = self._put_file(replacement_data, meta[KEY_PROJECT_ID])
            replacement[KEY_FILE_ID] = file_id
        try:
            if PYMONGO_3:
                result = self._data.replace_one(
                    filter = filter,
                    replacement = replacement,
                    * args, ** kwargs)
            else:
                if to_be_replaced is not None:
                    replacement['_id'] = to_be_replaced['_id']
                    result = self._data.save(to_save = replacement)
                else:
                    result = None
        except:
            if replacement_data is not None:
                self._get_gridfs(meta[KEY_PROJECT_ID]).delete(file_id)
            raise
        else:
            if to_be_replaced is not None:
                if KEY_FILE_ID in to_be_replaced:
                    self._get_gridfs(meta[KEY_PROJECT_ID]).delete(to_be_replaced[KEY_FILE_ID])
            return result

    def update_one(self, document, data = None, * args, ** kwargs):
        """Update document with new data.

        :param document: The document to update.
        :type document: A mappable type.
        :param data: The data to replace the old data with.
        :type data: Arbitrary binary data.
        
        See also: 
            - https://bitbucket.org/glotzer/compdb/wiki/latest/compmatdb_part2
            - http://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_one
        """
        self._filter_by_user(document) # Modify the document to only match user documents
        meta = self._make_meta_document(document, data)
        if data is not None:
            file_id = self._put_file(data, meta[KEY_PROJECT_ID])
            update = {'$set': {KEY_FILE_ID: file_id}}
        to_be_updated = self._data.find_one(meta)
        try:
            if PYMONGO_3:
                result = self._data.update_one(meta, update, * args, ** kwargs)
            else:
                result = self._data.update(meta, update, * args, ** kwargs)
        except:
            if data is not None:
                self._get_gridfs(meta[KEY_PROJECT_ID]).delete(file_id)
                #self._gridfs.delete(file_id)
            raise
        else:
            if to_be_updated is not None:
                if KEY_FILE_ID in to_be_updated:
                    self._get_gridfs(meta[KEY_PROJECT_ID]).delete(to_be_updated[KEY_FILE_ID])
                    #self._gridfs.delete(to_be_updated[KEY_FILE_ID])
            return result

    def find(self, filter = None, projection = None, * args, ** kwargs):
        """Find all records that match filter.

        :param filter: The filter to match documents with.
        :type filter: A mappable, that may contain callables as keys or values.
        :param projection: An iterable of field names that the returned records contain.
        :type projection: Iterable of str.
        :raises UnsupportedExpressionError
        
        See also:
            - https://bitbucket.org/glotzer/compdb/wiki/latest/compmatdb_part2
            - http://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
        """
        # The call_dict keeps track of all calls required to resolve in this query
        call_dict = dict()
        # The plain_filter is a pymongo conform filter, where all callables have been
        # replaced with place_holders
        plain_filter = self._resolve(filter, call_dict)
        logger.debug("Resolved find query: '{}'.".format(plain_filter))
        # Return all documents that match the plain filter
        try:
            docs = self._data.find(
                plain_filter, projection, * args, ** kwargs)
            # The Cursor is an iterator over the resulting records
            # and resolves all callables
            cursor = Cursor(self, docs, call_dict, projection)
            return cursor
        except pymongo.errors.PyMongoError as error:
            logger.error("Error during find operation from expression: '{}'.".format(filter))
            raise

    def find_one(self, filter_or_id = None, projection = None, * args, ** kwargs):
        """Like find(), but returns the first matching document or None if no document matches.
        
        :param filter_or_id: A filter or a document id.
        :type filter_or_id: A mapping, which may contain callables as key or value or a str or ObjectID.
        :param projection: A iterable of field names that the returned records contain.
        :type projection: Iterable of str.
        :raises UnsupportedExpressionError

        See also: find()
        """
        call_dict = dict()
        plain_filter_or_id = self._resolve_expr(filter_or_id, call_dict)
        doc = self._data.find_one(plain_filter_or_id, projection, * args, ** kwargs)
        if doc is not None:
            try:
                return self._resolve_doc(doc, call_dict, projection)
            except AuthorizationError as error:
                logger.warning("Unauthorized for result: {}.".format(error))
                return None
        else:
            return None

    def _resolve_doc(self, doc, call_dict, projection = None):
        "Resolve a document containing callables."
        try:
            return self._resolve_projection(self._resolve_calls(self._resolve_files(doc), call_dict), projection)
        except pymongo.errors.OperationFailure as error:
            if 'not authorized' in str(error):
                raise AuthorizationError(error)
            raise error

    def _resolve_projection(self, doc, projection):
        "Resolve a projection document for callables."
        if projection is None:
            return doc
        else:
            return {k: v for k,v in doc.items() if k in projection}

    def resolve(self, docs):
        "Resolve the docs for file data."
        warnings.warn("This function is obsolete.", DeprecationWarning)
        raise Deprec
        for doc in docs:
            yield self._resolve_files(doc)

    def _delete_doc(self, doc):
        """Delete document :param doc:.

        This function removes both the metadata document as well as the filedata in the gridfs collection.
        """
        if KEY_FILE_ID in doc:
            self._get_gridfs(doc[KEY_PROJECT_ID]).delete(doc[KEY_FILE_ID])
            #self._gridfs.delete(doc[KEY_FILE_ID])
        if PYMONGO_3:
            self._cache.delete_many({KEY_FILE_ID: doc['_id']})
        else:
            self._cache.remove({KEY_FILE_ID: doc['_id']})

    def add_adapter(self, adapter):
        """Add adapter :param adapter: to the formats and adapter network."""
        conversion.add_adapter_to_network(
            self._formats_network, adapter)

    def delete_one(self, filter, * args, ** kwargs):
        """Delete the first document matching filter.

        :param filter: The filter that the document to be deleted matches.
        :type filter: A mapping type.
        :raises UnsupportedExpressionError
        
        See also: delete_many()
        """
        self._filter_by_user(filter) # Modify the filter to only match user documents
        doc = self._data.find_one(filter, *args, **kwargs)
        if doc is not None:
            if PYMONGO_3:
                self._data.delete_one({'_id': doc['_id']})
            else:
                self._data.remove({'_id': doc['_id']})
            self._delete_doc(doc)

    def delete_many(self, filter, * args, ** kwargs):
        """Delete all documents matching filter.

        :param filter: The filter that all documents to be deleted match.
        :type filter: A mapping type.
        :raises UnsupportedExpressionError

        See also: delete_one()
        """
        self._filter_by_user(filter) # Modify the filter to only match user documents
        docs = self._data.find(filter, *args, ** kwargs)
        for doc in docs:
            self._delete_doc(doc)
        if PYMONGO_3:
            result = self._data.delete_many(filter)
        else:
            result = self._data.remove(filter)
        return result

    def _resolve(self, expr, call_dict, *args, **kwargs):
        """First resolve stage, trying to resolve standard filters first."""
        standard, nonstandard = split_standard_nonstandard(expr)
        doc_ids = list()
        if standard:
            try:
                doc_ids = [doc['_id'] for doc in self._data.find(standard, *args, **kwargs)]
            except pymongo.errors.OperationFailure as error:
                if 'not authorized' in str(error):
                    raise AuthorizationError("Not authorized to access database.")
                raise
            if '_id' in standard:
                nonstandard['_id'] = {'$and': [standard['_id'], {'$in': doc_ids}]}
            else:
                nonstandard['_id'] = {'$in': doc_ids}
        return self._resolve_expr(nonstandard, call_dict, doc_ids, *args, **kwargs)

    def _resolve_dict(self, d, call_dict, doc_ids, * args, ** kwargs):
        "Resolve the dictionary for callables and other operators."
        standard = dict()
        methods_filter = dict()
        for key, value in d.items():
            if callable(key):
                assert not callable(value)
                methods_filter[key] = self._resolve_expr(value, call_dict, doc_ids)
            elif key == '$project':
                value[KEY_FILE_ID] = '$'+KEY_FILE_ID
                value[KEY_PROJECT_ID] = '$'+KEY_PROJECT_ID
                standard[key] = value
            elif key.startswith('$') and key in ILLEGAL_AGGREGATION_KEYS:
                raise UnsupportedExpressionError(key)
            else:
                standard[key] = self._resolve_expr(value, call_dict, doc_ids)
        if methods_filter:
            if doc_ids and not '_id' in standard:
                standard['_id'] = {'$in': doc_ids}
            if PYMONGO_3:
                docs = self._data.find(filter=standard,projection=['_id'])
            else:
                docs = self._data.find(spec = standard, fields = ['_id'])
            logger.debug("# docs to filter by method: {}".format(docs.count()))
            filtered = self._filter_by_methods(docs, methods_filter)
            return {'_id': {'$in': list(filtered)}}
        else:
            return d

    def _resolve_expr(self, expr, call_dict, doc_ids = None, *args, **kwargs):
        """"Resolve expr into a pymongo readable form.

        All callables are replaced with placeholders, that link to the call_dict.
        """
        if isinstance(expr, dict):
            plain = {k: self._resolve_expr(v, call_dict, doc_ids, * args, ** kwargs)
                        for k,v in expr.items()}
            return self._resolve_dict(plain, call_dict, doc_ids,* args, ** kwargs)
        elif isinstance(expr, list):
            return [self._resolve_expr(v, call_dict, doc_ids, *args, **kwargs) for v in expr]
        elif callable(expr):
            call_id = str(uuid.uuid4())
            call_dict[call_id] = expr
            return {'$literal': "$CALL({})".format(call_id)}
        else:
            return expr

    def _resolve_stage(self, stage, call_dict):
        "Resolve a stage, which is part of a pipeline."
        return self._resolve_expr(stage, call_dict)

    def _resolve_pipeline(self, pipeline, call_dict):
        "Resolve a pipeline."
        for stage in pipeline:
            yield self._resolve_stage(stage, call_dict)

    def _resolve_calls(self, result, call_dict, data = None):
        "Resolve all calls in result."
        if isinstance(result, dict):
            if KEY_FILE_ID in result:
                data = self._get_file(result[KEY_FILE_ID], result[KEY_PROJECT_ID])
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
        """Evaluate the aggregation pipeline.
        
        :param pipeline: The aggregation pipeline.
        :type pipeline: An iterable of expressions.
        :raises UnsupportedExpressionError

        See also:
            - https://bitbucket.org/glotzer/compdb/wiki/latest/compmatdb_part3
            - http://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
        """
        call_dict = dict()
        plain_pipeline = list(self._resolve_pipeline(pipeline, call_dict))
        logger.debug("Pipeline expression: '{}'.".format(plain_pipeline))
        try:
            result = self._data.aggregate(plain_pipeline, ** kwargs)
            cursor = CommandCursor(self, result, call_dict)
            return cursor
        except pymongo.errors.PyMongoError as error:
            logger.error("Error during aggregation of pipeline expression: '{}'.".format(pipeline))
            raise
