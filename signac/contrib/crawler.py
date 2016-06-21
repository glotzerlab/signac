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

from ..common import six
from .utility import walkdepth
from .hashing import calc_id
from .filesystems import filesystems_from_configs

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

        The return value of this generator function is not directly defined,
        however it is recommended to use `file-like objects`_.

        .. _`file-like objects`:
            https://docs.python.org/3/glossary.html#term-file-object

        :yields: arbitrary objects."""
        return
        yield

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

    .. code::

        ~/my_project/a_0.txt
        ~/my_project/a_1.txt
        ...

    A valid regular expression to match
    this patter would be: ``a_(?P<a>\d+)\.txt``.

    A regular expression crawler for this structure could be implemented
    like this:

    .. code::

        import re

        class TextFile(object):
            def __init__(self, file):
                # file is a file-like object
                return file.read()

        MyCrawler(RegexFileCrawler):
            pass

        MyCrawler.define('a_(?P<a>\d+)\.txt, TextFile)

    In this case we could also use :class:`.contrib.formats.TextFile`
    as data type which is an implementation of the example shown above.
    However we could use any other type, as long as its constructor
    expects a `file-like object`_ as its first argument.

    .. _`file-like object`: https://docs.python.org/3/glossary.html#term-file-object
    """
    "Mapping of compiled regex objects and associated formats."
    definitions = dict()

    @classmethod
    def define(cls, regex, format_):
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
        for meth in ('read', 'close'):
            if not callable(getattr(format_, meth, None)):
                msg = "Format {} has no {}() method.".format(format_, meth)
                warnings.warn(msg)
        definitions = dict(cls.definitions)
        definitions[regex] = format_
        cls.definitions = definitions

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
                yield doc

    def fetch(self, doc, mode='r'):
        """Fetch the data associated with `doc`.

        :param doc: A document.
        :type doc: :class:`dict`
        :yields: All files associated with doc in the defined format.

        .. note::

            For generality the :meth:`~.BaseCrawler.fetch` method is
            a generator function, which may yield an arbitrary number
            of objects of arbitrary type. In the case of the
            :class:`~.RegexFileCrawler` it will always yield
            exactly **one** object."""
        fn = doc.get(KEY_FILENAME)
        if fn:
            for regex, format_ in self.definitions.items():
                ffn = os.path.join(self.root, fn)
                m = regex.match(ffn)
                if m:
                    yield format_(open(ffn, mode=mode))

    def process(self, doc, dirpath, fn):
        """Post-process documents generated from filenames.

        Example:

        .. code::

            MyCrawler(signac.contrib.crawler.RegexFileCrawler):
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


class SignacProjectBaseCrawler(BaseCrawler):
    encoding = 'utf-8'
    fn_statepoint = 'signac_statepoint.json'
    statepoint_index = 'statepoint'

    def get_statepoint(self, dirpath):
        job_path = os.path.join(
            self.root,
            os.path.relpath(dirpath, self.root).split('/')[0])
        with open(os.path.join(job_path, self.fn_statepoint), 'rb') as file:
            doc = json.loads(file.read().decode(self.encoding))
        signac_id = calc_id(doc)
        assert job_path.endswith(signac_id)
        return signac_id, doc

    def process(self, doc, dirpath, fn):
        signac_id, statepoint = self.get_statepoint(dirpath)
        doc['signac_id'] = signac_id
        if self.statepoint_index:
            doc[self.statepoint_index] = statepoint
        else:
            doc.update(statepoint)
        return super(SignacProjectBaseCrawler, self).process(doc, dirpath, fn)


class SignacProjectRegexFileCrawler(
        SignacProjectBaseCrawler,
        RegexFileCrawler):
    pass


class SignacProjectJobDocumentCrawler(SignacProjectBaseCrawler):
    re_job_document = '.*\/signac_job_document\.json'
    statepoint_index = 'statepoint'
    signac_id_alias = '_id'

    def docs_from_file(self, dirpath, fn):
        if re.match(self.re_job_document, os.path.join(dirpath, fn)):
            with open(os.path.join(dirpath, fn), 'rb') as file:
                try:
                    job_doc = json.loads(file.read().decode(self.encoding))
                except ValueError:
                    logger.error(
                        "Failed to load job document for job '{}'.".format(
                            self.get_statepoint(dirpath)[0]))
                    raise
            signac_id, statepoint = self.get_statepoint(dirpath)
            job_doc['signac_id'] = signac_id
            if self.statepoint_index:
                job_doc[self.statepoint_index] = statepoint
            else:
                job_doc.update(statepoint)
            if self.signac_id_alias:
                job_doc[self.signac_id_alias] = signac_id
            yield job_doc
        for doc in super(SignacProjectJobDocumentCrawler, self).docs_from_file(
                dirpath, fn):
            yield doc


class SignacProjectCrawler(
        SignacProjectRegexFileCrawler,
        SignacProjectJobDocumentCrawler):
    pass


def _store_files_to_mirror(mirror, crawler, doc, mode='rb'):
    link = doc.setdefault(KEY_LINK, dict())
    fs_config = link.setdefault('mirrors', list())
    fs_config.append({mirror.name: mirror.config()})
    file_ids = link.setdefault('file_ids', list())
    for file in crawler.fetch(doc, mode=mode):
        file_id = hashlib.md5(file.read()).hexdigest()
        file.seek(0)
        try:
            with mirror.new_file(_id=file_id) as mirrorfile:
                mirrorfile.write(file.read())
        except mirror.FileExistsError:
            pass
        if file_id not in file_ids:
            file_ids.append(file_id)
        file.close()


class MasterCrawler(BaseCrawler):
    """Crawl the data space and search for signac crawlers.

    The MasterCrawler executes signac slave crawlers
    defined in signac_access.py modules.

    If the master crawlers has defined tags, it will only
    execute slave crawlers with at least one matching tag.

    :param root: The path to the root directory to crawl through.
    :type root: str
    :param link_local: Store a link to the local access module.
    :param mirrors: An optional set of mirrors, to export data to."""

    FN_ACCESS_MODULE = 'signac_access.py'
    "The filename of modules containing crawler definitions."

    def __init__(self, root, link_local=True, mirrors=None):
        self.link_local = link_local
        if mirrors is None:
            self.mirrors = list()
        else:
            self.mirrors = list(filesystems_from_configs(mirrors))
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
                if hasattr(crawler, 'fetch'):
                    if self.link_local:
                        link = doc.setdefault(KEY_LINK, dict())
                        link['link_type'] = 'module_fetch'  # deprecated
                        link[KEY_CRAWLER_PATH] = os.path.abspath(dirpath)
                        link[KEY_CRAWLER_MODULE] = fn
                        link[KEY_CRAWLER_ID] = crawler_id
                    for mirror in self.mirrors:
                        _store_files_to_mirror(mirror, crawler, doc)
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
                raise
            else:
                logger.debug("Executed slave crawlers.")


def _load_crawler(name):
    if six.PY2:
        return imp.load_source(os.path.splitext(name)[0], name)
    else:
        return importlib.machinery.SourceFileLoader(name, name).load_module()


def fetch(doc, mode='r', sources=None, ignore_linked_mirrors=False):
    """Fetch all data associated with this document.

    The sources argument is either a list of filesystem-like objects
    or a list of file system configurations or a mix of both.

    See :func:`.contrib.filesystems.filesystems_from_config`
    for details.

    :param doc: A document which is part of an index.
    :type doc: mapping
    :param mode: Mode to use for file opening.
    :param sources: An optional set of sources to fetch files from.
    :param ignore_linked_mirrors: Ignore all mirror information in the
        document's link attribute.
    :yields: Data associated with this document in the specified format."""
    if doc is None:
        raise ValueError(doc)
    link = doc.get(KEY_LINK)
    if link is None:
        return
    else:
        link = dict(link)
    if KEY_CRAWLER_PATH in link:
        logger.debug("Fetching files from the local file system.")
        try:
            for file in _fetch_fs(doc, mode=mode):
                yield file
            return
        except OSError as error:
            logger.warning(
                "Unable to fetch file from local file system: {}".format(error))
    to_fetch = set(link.pop('file_ids', []))
    n = len(to_fetch)
    if n == 0:
        return
    if sources is None:
        sources = list()
    else:
        sources = list(filesystems_from_configs(sources))
    if not ignore_linked_mirrors:
        sources.extend(
            list(filesystems_from_configs(link.get('mirrors', list()))))
    logger.debug("Using sources to fetch files: {}".format(sources))
    for source in sources:
        fetched = set()
        for file_id in to_fetch:
            logger.debug("Fetching file with id '{}'.".format(file_id))
            try:
                yield source.get(file_id, mode=mode)
                fetched.add(file_id)
            except source.FileNotFoundError:
                continue
        for file_id in fetched:
            to_fetch.remove(file_id)
    if to_fetch:
        msg = "Unable to fetch {}/{} file(s) associated with this document ."
        raise IOError(msg.format(len(to_fetch), n))


def fetch_one(doc, *args, **kwargs):
    """Fetch data associated with this document.

    Unlike :func:`~signac.fetch`, this function returns only the first
    file associated with doc and ignores all others.

    :param doc: A document which is part of an index.
    :type doc: mapping
    :yields: Data associated with this document in the specified format."""
    return next(fetch(doc, *args, **kwargs))


def fetched(docs):
    for doc in docs:
        for data in fetch(doc):
            yield doc, data


def _fetch_fs(doc, mode):
    "Fetch files for doc from the local file system."
    link = doc[KEY_LINK]
    fn_module = os.path.join(
        link[KEY_CRAWLER_PATH], link[KEY_CRAWLER_MODULE])
    crawler_module = _load_crawler(fn_module)
    crawlers = crawler_module.get_crawlers(link[KEY_CRAWLER_PATH])
    for d in crawlers[link[KEY_CRAWLER_ID]].fetch(doc, mode=mode):
        yield d


def export_pymongo(docs, index, chunksize=1000, *args, **kwargs):
    """Optimized export function for pymongo collections.

    The behavior of this function is equivalent to:

    .. code-block:: python

        for doc in docs:
            index.replace_one({'_id': doc['_id']}, doc)

    .. note::

        All index documents must be JSON-serializable to
        be able to be exported to a MongoDB collection.

    :param docs: The index documents to export.
    :param index: The database collection to export the index to.
    :type index: :class:`pymongo.collection.Collection`
    :param chunksize: The buffer size for export operations.
    :type chunksize: int"""
    import pymongo
    logger.info("Exporting index for pymongo.")
    operations = []

    # backwards compatibility hacks
    if hasattr(docs, 'crawl'):
        docs = docs.crawl(* args, **kwargs)
        warnings.warn(
            "You are using a deprecated API for export_pymongo()!",
            DeprecationWarning)

    for doc in docs:
        f = {'_id': doc['_id']}
        operations.append(pymongo.ReplaceOne(f, doc, upsert=True))
        if len(operations) >= chunksize:
            logger.debug("Pushing chunk.")
            index.bulk_write(operations)
            operations.clear()
    if len(operations):
        logger.debug("Pushing final chunk.")
        index.bulk_write(operations)


def export(docs, index, *args, **kwargs):
    """Export function for collections.

    The behavior of this function is equivalent to:

    .. code-block:: python

        for doc in docs:
            index.replace_one({'_id': doc['_id']}, doc)

    :param docs: The index docs to export.
    :param index: The collection to export the index to."""
    logger.info("Exporting index.")
    if hasattr(docs, 'crawl'):
        docs = docs.crawl(* args, **kwargs)
        warnings.warn(
            "You are using a deprecated API of export()!")
    for doc in docs:
        f = {'_id': doc['_id']}
        index.replace_one(f, doc)
