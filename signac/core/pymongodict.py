# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Dict implementation with pymongo backend."""

from .attrdict import SyncedAttrDict
from collections.abc import Mapping
from copy import copy
import datetime
import hashlib
from contextlib import contextmanager
import sys
import json

import logging

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from .jsondict import BufferException


logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE = 32 * 2**20    # 32 MB

# a dictionary of (collection_name, PyMongoBufer) pairs
_PYMONGO_BUFFERS = dict()

def _hash(blob):
    """Calculate and return the md5 hash value for the file data."""
    if blob is not None:
        # create an oredered dict through json
        json_str = json.dumps(blob, sort_keys=True)
        return hashlib.md5(json_str.encode()).hexdigest()

class BufferedDocumentError(BufferException):
    """Raised when an error occured while flushing one or more buffered documents

    .. attribute:: jobids

        A dictionary of jobids that caused issues during the flush operation,
        mapped to a possible reason for the issue or None in case that it
        cannot be determined.
    """

    def __init__(self, jobids):
        self.jobids = jobids

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.jobids)

class PyMongoBuffer:
    """A cache for aggregating update queries to a collection
       into a single bulk_write operation.
    """

    def __init__(self, collection):
        self.collection = collection
        self.buffered_mode = True
        self.buffered_mode_force_write = None
        self.buffer_size = None
        self.update_buffer_size = None
        self.buf = dict()
        self.update_buf = dict()
        self.hashes = dict()
        self.meta = dict()

    def _get_document_metadata(self, jobid):
        doc = self.collection.find_one(jobid)

        if 'doc_last_modified' in doc:
            return doc['doc_last_modified']
        else:
            return None

    def _store_in_buffer(self, jobid, blob, store_hash=False):
        assert self.buffered_mode
        blob_size = sys.getsizeof(blob)
        buffer_load = self.get_buffer_load()
        if self.buffer_size > 0:
            if blob_size > self.buffer_size:
                return False
            elif blob_size + buffer_load > self.buffer_size:
                logger.debug("Buffer overflow, flushing...")
                self.flush_all()

        self.buf[jobid] = blob
        if store_hash:
            if not self.buffered_mode_force_write:
                self.meta[jobid] = self._get_document_metadata(jobid)
            self.hashes[jobid] = _hash(blob)
        return True

    def _store_in_update_buffer(self,jobid, blob):
        assert self.buffered_mode
        blob_size = sys.getsizeof(blob)
        buffer_load = self.get_update_buffer_load()
        if self.update_buffer_size > 0:
            if blob_size > self.update_buffer_size:
                return False
            elif blob_size + buffer_load > self.update_buffer_size:
                logger.debug("Write-through buffer overflow, flushing...")
                self.flush_all()

        # store this update operation
        if jobid in self.update_buf:
            self.update_buf[jobid].update(blob)
        else:
            self.update_buf[jobid] = blob
        return True

    def flush_all(self):
        """Execute all deferred PyMongoDict write operations."""
        logger.debug("Flushing buffer...")
        issues = dict()
        jobids = []
        time_current = datetime.datetime.utcnow()

        update_blobs = dict()
        while self.buf:
            jobid, blob = self.buf.popitem()
            if not self.buffered_mode_force_write:
                meta = self.meta.pop(jobid)

            # if there's data in the write cache, update current blob
            if jobid in self.update_buf:
                blob.update(self.update_buf.pop(jobid))
            updated = _hash(blob) != self.hashes.pop(jobid)

            if updated:
                if not self.buffered_mode_force_write:
                    new_meta = self._get_document_metadata(jobid)
                    if new_meta is not None and new_meta != meta:
                        print(meta,new_meta)
                        issues[jobid] = 'job document in '+ str(self.collection) + ' appears to have been externally modified.'
                        continue
                update_blobs[jobid] = blob

        if issues:
            raise BufferedDocumentError(issues)

        # now go through the partial updates without local info
        while self.update_buf:
            jobid, blob = self.update_buf.popitem()
            update_blobs[jobid] = blob

        # construct the query
        ops = []
        for jobid,blob in update_blobs.items():
            update_query = {}
            for key,val in blob.items():
                update_query['doc'+'.'+key] = val
            update_query['doc_last_modified'] = time_current
            ops.append(UpdateOne({'_id': jobid}, {'$set': update_query}))

        if len(ops):
            try:
                # submit the query as a parallel bulk write operation
                self.collection.bulk_write(ops,ordered=False)
            except BulkWriteError:
                logger.error(str(error))
                raise

    def get_buffer_size(self):
        """Return the current maximum size of the read/write buffer."""
        return self.buffer_size

    def get_buffer_load(self):
        """Return the current actual size of the read/write buffer."""
        return sum((sys.getsizeof(x) for x in self.buf.values()))

    def get_update_buffer_size(self):
        """Return the current maximum size of the read/write buffer."""
        return self.update_buffer_size

    def get_update_buffer_load(self):
        """Return the current actual size of the read/write buffer."""
        return sum((sys.getsizeof(x) for x in self.update_buf.values()))

    def in_buffered_mode(self):
        """Return true if in buffered read/write mode."""
        return self.buffered_mode


@contextmanager
def buffer_pymongo_reads_writes(project, buffer_size=DEFAULT_BUFFER_SIZE,
                                update_buffer_size=DEFAULT_BUFFER_SIZE,
                                force_write=False):
    """Enter a global buffer mode for all PyMongoDict instances.

    All future write operations are written to the buffer, read
    operations are performed from the buffer whenever possible.

    All write operations are deferred until the flush_all() function
    is called, the buffer overflows, or upon exiting the buffer mode.

    This context may be entered multiple times, however the buffer size
    can only be set *once*. Any subsequent specifications of the buffer
    size are ignored.

    :param buffer_size:
        Specify the maximum size of the read/write buffer. Defaults
        to DEFAULT_BUFFER_SIZE. A negative number indicates to not
        restrict the buffer size.
    :type buffer_size:
        int
    :type force_write:
        bool, if True, do not check timestamps upon editing the documents.
        This can further speed up large numbers of edits, but doesn't
        detect if the document has been update simultaneously by other procesees

    """

    global _PYMONGO_BUFFERS

    if project.db is None:
        raise BufferException('PyMongo buffered mode requires that the project is configured '
                              'with an index database.')

    assert project.index_collection is not None

    collection_name = project.index_collection.full_name

    if collection_name in _PYMONGO_BUFFERS:
        raise BufferException('There alreads exists a buffer context for collection '.format(collection_name))

    # Basic type check (to prevent common user error)
    if not isinstance(buffer_size, int) or \
            buffer_size is True or buffer_size is False:    # explicit check against boolean
        raise TypeError("The buffer size must be an integer!")

    # instantiate the cache pointing to this collection
    buf = _PYMONGO_BUFFERS[collection_name] = PyMongoBuffer(project.index_collection)

    buf.buffer_size = buffer_size
    buf.update_buffer_size = update_buffer_size
    buf.buffered_mode_force_write = force_write

    try:
        yield
    finally:
        buf.flush_all()
        del _PYMONGO_BUFFERS[collection_name]

class PyMongoDict(SyncedAttrDict):
    """A dict-like mapping interface to pymongo document.

    .. code-block:: python

        db = get_database('test', 'myhost')
        doc = PyMongoDict(db.my_collection, jobid)
        doc['foo'] = "bar"
        assert doc.foo == doc['foo'] == "bar"
        assert 'foo' in doc
        del doc['foo']

    This class allows access to values through key indexing or attributes
    named by keys, including nested keys:

    .. code-block:: python

        >>> doc['foo'] = dict(bar=True)
        >>> doc
        {'foo': {'bar': True}}
        >>> doc.foo.bar = False
        {'foo': {'bar': False}}

    :param collection:
        A handle to a :class:py:`pymongo.collection.Collection` object.
    :param jobid:
        A unique identifier for the pymongo document containing the job doc
    :param parent:
        A parent instance of PyMongoDic or None.
    """

    def __init__(self, collection=None, jobid=None, parent=None):
        if (collection is None or jobid is None) == (parent is None):
            raise ValueError(
                "Illegal argument combination, one of "
                "parent or collection/jobid must be None, but not both.")
        self._collection = collection
        self._jobid = jobid
        self._deleted_keys = []
        super(PyMongoDict, self).__init__(parent=parent, protected_keys=['_jobid','_deleted_keys','_collection'])

    def reset(self, data):
        """Replace the document contents with data."""
        if isinstance(data, Mapping):
            with self._suspend_sync():
                backup = copy(self._data)
                try:
                    self._data = {
                        self._validate_key(k): self._dfs_convert(v)
                        for k, v in data.items()
                    }
                    self._save()
                except BaseException:  # rollback
                    self._data = backup
                    raise
        else:
            raise ValueError("The document must be a mapping.")

    def _load_from_db(self):
        logger.debug("Loading document for job {} from {}".format(self._jobid, repr(self._collection)))
        pymongo_doc = self._collection.find_one({'_id': self._jobid})
        if 'doc' in pymongo_doc:
            return pymongo_doc['doc']
        else:
            return dict()

    def _load(self):
        assert self._collection is not None
        assert self._jobid is not None

        collection_name = self._collection.full_name

        if collection_name in _PYMONGO_BUFFERS:
            buf = _PYMONGO_BUFFERS[collection_name]
            if self._jobid in buf.buf:
                # Load from buffer:
                blob = buf.buf[self._jobid]
            else:
                # Load from db and store in buffer
                blob = self._load_from_db()
                buf._store_in_buffer(self._jobid, blob, store_hash=True)

            # if any updates are pending in the cache, update the blob just loaded
            if self._jobid in buf.update_buf:
                blob.update(buf.update_buf.pop(self._jobid))
                buf._store_in_buffer(self._jobid, blob, store_hash=False)

            return blob
        else:
            return self._load_from_db()

    def _save(self, data=None):
        assert self._collection is not None
        assert self._jobid is not None

        if data is None:
            data = self._as_dict()

        collection_name = self._collection.full_name

        have_cache = collection_name in _PYMONGO_BUFFERS
        do_sync = not have_cache or len(self._deleted_keys) > 0

        if do_sync:
            # in synchronous mode it doesn't matter if we have partial information,
            # as pymongo will only update select fields anyway
            update_query = {}
            for key in data.keys():
                update_query['doc'+'.'+key] = data[key]
            update_query['doc_last_modified'] = datetime.datetime.utcnow()

            delete_query = {}
            for key in self._deleted_keys:
                delete_query['doc'+'.'+key] = None
            self._deleted_keys = []

            query = {}
            if update_query:
                query['$set'] = update_query
            if delete_query:
                query['$unset'] = delete_query
            if len(query):
                logger.debug("Updating job {} in {}".format(self._jobid, repr(self._collection)))
                self._collection.update_one({'_id': self._jobid}, query)

        if collection_name in _PYMONGO_BUFFERS:
            buf = _PYMONGO_BUFFERS[collection_name]

            # update the buffered object
            buf._store_in_buffer(self._jobid, data, store_hash=do_sync)


    def __delitem__(self, key):
        # store the key in a list for later removal from the collection
        self._deleted_keys += [key]

        # this will trigger a load()/save()
        super(PyMongoDict, self).__delitem__(key)

    def __setitem__(self, key, value):
        # a special codepath for __setitem__ ensures we can directly write through to the cache
        have_cache = self._collection is not None and self._collection.full_name in _PYMONGO_BUFFERS
        if  have_cache:
            buf = _PYMONGO_BUFFERS[self._collection.full_name]
            buf._store_in_update_buffer(self._jobid, {key: value})
            return value
        else:
            super(PyMongoDict, self).__setitem__(key, value)

