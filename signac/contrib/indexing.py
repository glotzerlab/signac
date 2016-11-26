# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
import json
import math
import hashlib
import logging
import warnings
import errno
from time import sleep

from ..common import six
from ..common import errors
from .utility import walkdepth, is_string
from .hashing import calc_id

if six.PY2:
    import imp
else:
    import importlib.machinery


logger = logging.getLogger(__name__)

KEY_PROJECT = 'project'
KEY_FILENAME = 'filename'
KEY_PATH = 'root'
KEY_PAYLOAD = 'format'
KEY_LINK = 'signac_link'
KEY_CRAWLER_PATH = 'access_crawler_root'
KEY_CRAWLER_MODULE = 'access_module'
KEY_CRAWLER_ID = 'access_crawler_id'


def md5(file):
    "Calculate and return the md5 hash value for the file data."
    m = hashlib.md5()
    for chunk in iter(lambda: file.read(4096), b''):
        m.update(chunk)
    return m.hexdigest()


class BaseCrawler(object):
    """Crawl through `root` and index all files.

    The crawler creates an index on data, which can be exported
    to a database for easier access."""
    tags = None

    def __init__(self, root):
        """Initialize a BaseCrawler instance.

        :param root: The path to the root directory to crawl through.
        :type root: str"""
        self.root = root
        self.tags = set() if self.tags is None else set(self.tags)

    def docs_from_file(self, dirpath, fn):
        """Implement this method to generate documents from files.

        :param dirpath: The path of the file, relative to `root`.
        :type dirpath: str
        :param fn: The filename.
        :type fn: str
        :returns: A document, that means an instance of mapping.
        :rtype: mapping"""
        raise NotImplementedError()

    def fetch(self, doc, mode='r'):
        """Implement this generator method to associate data with a document.

        :returns: object associated with doc
        """
        raise errors.FetchError("Unable to fetch object for '{}'.".format(doc))

    @classmethod
    def _calculate_hash(cls, doc, dirpath, fn):
        import hashlib
        blob = json.dumps(doc, sort_keys=True)
        m = hashlib.md5()
        m.update(dirpath.encode('utf-8'))
        m.update(fn.encode('utf-8'))
        m.update(blob.encode('utf-8'))
        return m.hexdigest()

    def crawl(self, depth=0):
        """Crawl through the `root` directory.

        The crawler will inspect every file and directory up
        until the specified `depth` and call the
        :meth:`docs_from_file` method.

        :param depth: Crawl through the directory for the specified depth.
                      A value of 0 specifies no limit.
        :type dept: int
        :yields: (id, doc)-tuples"""
        logger.info("Crawling '{}' (depth={})...".format(self.root, depth))
        for dirpath, dirnames, filenames in walkdepth(self.root, depth):
            for fn in filenames:
                for doc in self.docs_from_file(dirpath, fn):
                    logger.debug("doc from file: '{}'.".format(
                        os.path.join(dirpath, fn)))
                    doc.setdefault(KEY_PAYLOAD, None)
                    doc.setdefault(
                        '_id', self._calculate_hash(doc, dirpath, fn))
                    yield doc
        logger.info("Crawl of '{}' done.".format(self.root))

    def process(self, doc, dirpath, fn):
        """Implement this method for additional processing of generated docs.

        The default implementation will return the unmodified `doc`.

        :param dirpath: The path of the file, relative to `root`.
        :type dirpath: str
        :param fn: The filename.
        :type fn: str
        :returns: A document, that means an instance of mapping.
        :rtype: mapping"""
        return doc


class RegexFileCrawler(BaseCrawler):
    """Generate documents from filenames and associate each file with a data type.

    The `RegexFileCrawler` uses regular expressions to generate
    data from files. This is a particular easy method to retrieve meta data
    associated with files. Inherit from this class to configure a crawler
    for your data structre.

    Let's assume we want to index text files, with a naming pattern, that
    specifies a parameter `a` through the filename, e.g.:

    .. code-block:: python

        ~/my_project/a_0.txt
        ~/my_project/a_1.txt
        ...

    A valid regular expression to match
    this pattern would be: ``a_(?P<a>\d+)\.txt``.

    A regular expression crawler for this structure could be implemented
    like this:

    .. code-block:: python

        import re

        class TextFile(object):
            def __init__(self, file):
                # file is a file-like object
                return file.read()

        MyCrawler(RegexFileCrawler):
            pass

        MyCrawler.define('a_(?P<a>\d+)\.txt, TextFile)

    In this case we could also use :class:`.formats.TextFile`
    as data type which is an implementation of the example shown above.
    However we could use any other type, as long as its constructor
    expects a `file-like object`_ as its first argument.

    .. _`file-like object`: https://docs.python.org/3/glossary.html#term-file-object
    """
    "Mapping of compiled regex objects and associated formats."
    definitions = dict()

    @classmethod
    def define(cls, regex, format_=None):
        """Define a format for a particular regular expression.

        :param regex: All files of the specified format
            must match this regular expression.
        :type regex: :class:`str` or `compiled regular expression`_
        :param format_: The format associated with all matching files.
        :type format_: :class:`object`

        .. _`compiled regular expression`: https://docs.python.org/3.4/library/re.html#re-objects"""
        if six.PY2:
            if isinstance(regex, basestring):  # noqa
                regex = re.compile(regex)
        else:
            if isinstance(regex, str):
                regex = re.compile(regex)
        definitions = dict(cls.definitions)
        definitions[regex] = format_
        cls.definitions = definitions

    @classmethod
    def compute_file_id(cls, doc, file):
        """Compute the file id for a given doc and the associated file.

        :param doc: The index document
        :param file: The associated file
        :returns: The file id.
        """
        file_id = doc['md5'] = md5(file)
        return file_id

    def docs_from_file(self, dirpath, fn):
        """Generate documents from filenames.

        This method is an implementation of the abstract method
        of :class:`~.BaseCrawler`.
        It is not recommended to reimplement this method to modify
        documents generated from filenames.
        See :meth:`~RegexFileCrawler.process` instead."""
        for regex, format_ in self.definitions.items():
            m = regex.match(os.path.join(dirpath, fn))
            if m:
                doc = self.process(m.groupdict(), dirpath, fn)
                doc[KEY_FILENAME] = os.path.relpath(
                    os.path.join(dirpath, fn), self.root)
                doc[KEY_PATH] = os.path.abspath(self.root)
                doc[KEY_PAYLOAD] = str(format_)
                with open(os.path.join(dirpath, fn), 'rb') as file:
                    doc['file_id'] = self.compute_file_id(doc, file)
                yield doc

    def fetch(self, doc, mode='r'):
        """Fetch the data associated with `doc`.

        :param doc: A index document.
        :type doc: :class:`dict`
        :returns: The file associated with the index document.
        :rtype: A file-like object
        """
        fn = doc.get(KEY_FILENAME)
        if fn:
            for regex, format_ in self.definitions.items():
                ffn = os.path.join(self.root, fn)
                m = regex.match(ffn)
                if m:
                    if is_string(format_):
                        return open(ffn, mode=mode)
                    else:
                        for meth in ('read', 'close'):
                            if not callable(getattr(format_, meth, None)):
                                msg = "Format {} has no {}() method.".format(format_, meth)
                                warnings.warn(msg)
                        return format_(open(ffn, mode=mode))
            else:
                raise errors.FetchError("Unable to match file path of doc '{}' "
                                        "to format definition.".format(doc))
        else:
            raise errors.FetchError("Insufficient meta data in doc '{}'.".format(doc))

    def process(self, doc, dirpath, fn):
        """Post-process documents generated from filenames.

        Example:

        .. code-block:: python

            MyCrawler(signac.indexing.RegexFileCrawler):
                def process(self, doc, dirpath, fn):
                    doc['long_name_for_a'] = doc['a']
                    return super(MyCrawler, self).process(doc, dirpath, fn)

        :param dirpath: The path of the file, relative to `root`.
        :type dirpath: str
        :param fn: The filename.
        :type fn: str
        :returns: A document, that means an instance of mapping.
        :rtype: mapping"""
        result = dict()
        for key, value in doc.items():
            if value is None or isinstance(value, bool):
                result[key] = value
                continue
            try:
                value = float(value)
            except Exception:
                result[key] = value
            else:
                if not math.isnan(value) or math.isinf(value):
                    if float(value) == int(value):
                        result[key] = int(value)
                    else:
                        result[key] = float(value)
        return super(RegexFileCrawler, self).process(result, dirpath, fn)

    def crawl(self, depth=0):
        if self.definitions:
            for doc in super(RegexFileCrawler, self).crawl(depth=depth):
                yield doc
        else:
            return


class JSONCrawler(BaseCrawler):
    encoding = 'utf-8'
    fn_regex = '.*\.json'

    def docs_from_json(self, doc):
        yield doc

    def docs_from_file(self, dirpath, fn):
        if re.match(self.fn_regex, os.path.join(dirpath, fn)):
            with open(os.path.join(dirpath, fn), 'rb') as file:
                doc = json.loads(file.read().decode(self.encoding))
                for d in self.docs_from_json(doc):
                    yield d


def _index_signac_project_workspace(root,
                                    include_job_document=True,
                                    fn_statepoint='signac_statepoint.json',
                                    fn_job_document='signac_job_document.json',
                                    statepoint_index='statepoint',
                                    signac_id_alias='_id',
                                    encoding='utf-8',
                                    statepoint_dict=None):
    "Yields standard index documents for a signac project workspace."
    m = re.compile(r'[a-f0-9]{32}')
    try:
        job_ids = [jid for jid in os.listdir(root) if m.match(jid)]
    except OSError as error:
        if error.errno == errno.ENOENT:
            return
        else:
            raise
    for job_id in job_ids:
        if not m.match(job_id):
            continue
        doc = dict(signac_id=job_id)
        if signac_id_alias:
            doc[signac_id_alias] = job_id
        fn_sp = os.path.join(root, job_id, fn_statepoint)
        with open(fn_sp, 'rb') as file:
            sp = json.loads(file.read().decode(encoding))
            if statepoint_dict is not None:
                statepoint_dict[job_id] = sp
            if statepoint_index:
                doc[statepoint_index] = sp
            else:
                doc.update(sp)
        if include_job_document:
            fn_doc = os.path.join(root, job_id, fn_job_document)
            try:
                with open(fn_doc, 'rb') as file:
                    doc.update(json.loads(file.read().decode(encoding)))
            except IOError as error:
                if error.errno != errno.ENOENT:
                    raise
        yield doc


class SignacProjectCrawler(RegexFileCrawler):
    """Index a signac project workspace.

    Without any file format definitions, this crawler
    yields index documents for each job, including
    the statepoint and the job document.

    See also: :py:class:`~.RegexFileCrawler`

    :param root: The path to the project workspace.
    :type root: str"""
    encoding = 'utf-8'
    statepoint_index = 'statepoint'
    fn_statepoint = 'signac_statepoint.json'
    fn_job_document = 'signac_job_document.json'
    signac_id_alias = '_id'

    def __init__(self, root):
        self.root = root
        self._statepoints = dict()

    def _get_job_id(self, dirpath):
        return os.path.relpath(dirpath, self.root).split('/')[0]

    def _read_statepoint(self, job_id):
        fn_sp = os.path.join(self.root, job_id, self.fn_statepoint)
        with open(fn_sp, 'rb') as file:
            return json.loads(file.read().decode(self.encoding))

    def _get_statepoint(self, job_id):
        sp = self._statepoints.setdefault(job_id, self._read_statepoint(job_id))
        assert calc_id(sp) == job_id
        return sp

    def get_statepoint(self, dirpath):
        job_id = self._get_job_id(dirpath)
        return job_id, self._get_statepoint(self, job_id)

    def process(self, doc, dirpath, fn):
        if dirpath is not None:
            job_id = self._get_job_id(dirpath)
            statepoint = self._get_statepoint(job_id)
            doc['signac_id'] = job_id
            if self.statepoint_index:
                doc[self.statepoint_index] = statepoint
            else:
                doc.update(statepoint)
        return super(SignacProjectCrawler, self).process(doc, dirpath, fn)

    def crawl(self, depth=0):
        for doc in _index_signac_project_workspace(
                root=self.root,
                fn_statepoint=self.fn_statepoint,
                fn_job_document=self.fn_job_document,
                statepoint_index=self.statepoint_index,
                signac_id_alias=self.signac_id_alias,
                encoding=self.encoding,
                statepoint_dict=self._statepoints):
                yield self.process(doc, None, None)
        for doc in super(SignacProjectCrawler, self).crawl(depth=depth):
            yield doc


class MasterCrawler(BaseCrawler):
    """Crawl the data space and search for signac crawlers.

    The MasterCrawler executes signac slave crawlers
    defined in signac_access.py modules.

    If the master crawlers has defined tags, it will only
    execute slave crawlers with at least one matching tag.

    :param root: The path to the root directory to crawl through.
    :type root: str
    :param mirrors: An optional set of mirrors, to export data to."""

    FN_ACCESS_MODULE = 'signac_access.py'
    "The filename of modules containing crawler definitions."

    def __init__(self, root):
        self._crawlers = dict()
        super(MasterCrawler, self).__init__(root=root)

    def _docs_from_module(self, dirpath, fn):
        name = os.path.join(dirpath, fn)
        module = _load_crawler(name)
        for crawler_id, crawler in module.get_crawlers(dirpath).items():
            logger.info("Executing slave crawler:\n {}: {}".format(crawler_id, crawler))
            tags = getattr(crawler, 'tags', set())
            if tags is not None and len(set(tags)):
                if self.tags is None or not len(set(self.tags)):
                    logger.info("Skipping, crawler has defined tags.")
                    continue
                elif not set(self.tags).intersection(set(crawler.tags)):
                    logger.info("Skipping, tag mismatch.")
                    continue
            elif self.tags is not None and len(set(self.tags)):
                logger.info("Skipping, crawler has no defined tags.")
                continue
            for doc in crawler.crawl():
                doc.setdefault(
                    KEY_PROJECT, os.path.relpath(dirpath, self.root))
                yield doc

    def docs_from_file(self, dirpath, fn):
        if fn == self.FN_ACCESS_MODULE:
            try:
                for doc in self._docs_from_module(dirpath, fn):
                    yield doc
            except AttributeError as error:
                if str(error) == 'get_crawlers':
                    logger.warning(
                        "Module has no '{}' function.".format(error))
                else:
                    raise
            except Exception:
                logger.error("Error while indexing from module '{}'.".format(
                    os.path.join(dirpath, fn)))
            else:
                logger.debug("Executed slave crawlers.")


def _load_crawler(name):
    if six.PY2:
        return imp.load_source(os.path.splitext(name)[0], name)
    else:
        return importlib.machinery.SourceFileLoader(name, name).load_module()


def fetch(doc_or_id, mode='r', mirrors=None, num_tries=3, timeout=60, ignore_local=False):
    """Fetch the file associated with this document or file id.

    This function retrieves a file associated with the provided
    index document or file id and behaves like the built-in
    :py:func:`open` function, e.g.:

    .. code-block:: python

        for doc in index:
            with signac.fetch(doc) as file:
                do_something_with(file)

    :param doc_or_id: A file_id or a document with a file_id value.
    :param mode: Mode to use for opening files.
    :param mirrors: An optional set of mirrors to fetch the file from.
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :returns: The file associated with the document or file id.
    :rtype: A file-like object
    """
    if doc_or_id is None:
        raise ValueError("Argument 'doc_or_id' must not be None!")
    file_id = doc_or_id if isinstance(doc_or_id, str) else doc_or_id.get('file_id')
    if not ignore_local:
        try:
            fn = os.path.join(doc_or_id['root'], doc_or_id['filename'])
            return open(fn, mode=mode)
        except KeyError:
            raise errors.FetchError("Insufficient file meta data for fetch.", doc_or_id)
        except OSError as error:
            if error.errno == errno.ENOENT:
                if file_id is None:
                    raise errors.FetchError("Failed to fetch '{}'.".format(doc_or_id))
    if mirrors is None:
        raise errors.FetchError("No mirrors provided!")
    else:
        for i in range(num_tries):
            for mirror in mirrors:
                try:
                    return mirror.get(file_id, mode=mode)
                except mirror.AutoRetry as error:
                    logger.warning(error)
                    sleep(timeout)
                except mirror.FileNotFoundError as error:
                    logger.debug(error)
            else:
                raise errors.FetchError("Unable to fetch object for '{}'.".format(file_id))


def fetch_one(doc, *args, **kwargs):
    "Legacy function, use :py:func:`~.fetch` instead."
    warnings.warn(
        "This function is deprecated, please use fetch() instead.",
        DeprecationWarning)
    return fetch(doc_or_id=doc, *args, **kwargs)


def fetched(docs):
    """Iterate over documents and yield associated files."""
    for doc in docs:
        if 'file_id' in doc:
            yield doc, fetch(doc)


def _export_to_mirror(file, file_id, mirror):
    "Export a file-like object with file_id to mirror."
    with mirror.new_file(_id=file_id) as dst:
        dst.write(file.read())


def export_to_mirror(doc, mirror, num_tries=3, timeout=60):
    """Export a file associated with doc to mirror.

    :param doc: A document with a file_id entry.
    :param mirror: A file-system object to export the file to.
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :returns: The file id after successful export.
    """
    if 'file_id' not in doc:
        raise errors.ExportError("Doc '{}' does not have a file_id entry.".format(doc))
    for i in range(num_tries):
        try:
            with fetch(doc, mode='rb') as file:
                _export_to_mirror(file, doc['file_id'], mirror)
        except mirror.FileExistsError:
            logger.debug("File with id '{}' already exported, skipping.".format(doc['file_id']))
            break
        except mirror.AutoRetry as error:
            logger.warning("Error during export: '{}', retrying...".format(error))
            sleep(timeout)
        else:
            logger.debug("Stored file with id '{}' in mirror '{}'.".format(doc['file_id'], mirror))
            return doc['file_id']
    else:
        raise errors.ExportError(doc)


def export_one(doc, index, mirrors=None, num_tries=3, timeout=60):
    """Export one document to index and an optionally associated file to mirrors.

    :param doc: A document with a file_id entry.
    :param docs: The index collection to export to.
    :param mirrors: An optional set of mirrors to export files to.
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :returns: The id and file id after successful export.
    """
    index.replace_one({'_id': doc['_id']}, doc, upsert=True)
    if mirrors and 'file_id' in doc:
        for mirror in mirrors:
            export_to_mirror(doc, mirror, num_tries, timeout)
        return doc['_id'], doc['file_id']
    else:
        return doc['_id'], None


def export(docs, index, mirrors=None, num_tries=3, timeout=60, **kwargs):
    """Export docs to index and optionally associated files to mirrors.

    The behavior of this function is equivalent to:

    .. code-block:: python

        for doc in docs:
            export_one(doc, index, mirrors, num_tries)

    .. note::

        This function will automatically delegate to specialized
        implementations for special index types. For example, if
        the index argument is a MongoDB document collection, the
        index documents will be exported via :py:func:`~.export_pymongo`.

    :param docs: The index documents to export.
    :param index: The collection to export the index to.
    :param mirrors: An optional set of mirrors to export files to.
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :param kwargs: Optional keyword arguments to pass to
        delegate implementations.
    """
    try:
        import pymongo
    except ImportError:
        pass
    else:
        if isinstance(index, pymongo.collection.Collection):
            logger.info("Using optimized export function export_pymongo().")
            return export_pymongo(docs, index, mirrors, num_tries, timeout, **kwargs)
    for doc in docs:
        export_one(doc, index, mirrors, num_tries, timeout, **kwargs)


def _export_pymongo(docs, operations, index, mirrors, num_tries, timeout):
    """Export docs via operations to index and files to mirrors."""
    import pymongo
    if mirrors is not None:
        for mirror in mirrors:
            for doc in docs:
                if 'file_id' in doc:
                    export_to_mirror(doc, mirror, num_tries, timeout)
    for i in range(num_tries):
        try:
            index.bulk_write(operations)
            break
        except pymongo.errors.AutoReconnect as error:
            logger.warning(error)
            sleep(timeout)
    else:
        raise errors.ExportError()


def export_pymongo(docs, index, mirrors=None, num_tries=3, timeout=60, chunksize=100):
    """Optimized :py:func:`~.export` function for pymongo index collections.

    The behavior of this function is rougly equivalent to:

    .. code-block:: python

        for doc in docs:
            export_one(doc, index, mirrors, num_tries)

    .. note::

        All index documents must be JSON-serializable to
        be able to be exported to a MongoDB collection.

    :param docs: The index documents to export.
    :param index: The database collection to export the index to.
    :type index: :class:`pymongo.collection.Collection`
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :param chunksize: The buffer size for export operations.
    :type chunksize: int"""
    import pymongo
    logger.info("Exporting to pymongo database collection index '{}'.".format(index))
    chunk = []
    operations = []
    for doc in docs:
        f = {'_id': doc['_id']}
        chunk.append(doc)
        operations.append(pymongo.ReplaceOne(f, doc, upsert=True))
        if len(chunk) >= chunksize:
            logger.debug("Pushing chunk.")
            _export_pymongo(chunk, operations, index, mirrors, num_tries, timeout)
            chunk.clear()
            operations.clear()
    if len(operations):
        logger.debug("Pushing final chunk.")
        _export_pymongo(chunk, operations, index, mirrors, num_tries, timeout)
