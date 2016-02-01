import os
import re
import json
import six
import math
import io
import hashlib
import logging
import warnings

from .utility import walkdepth
from .hashing import calc_id
from ..db import get_database

if six.PY2:
    import imp
else:
    import importlib.machinery


logger = logging.getLogger(__name__)

FN_CRAWLER = 'signac_access.py'
KEY_PROJECT = 'project'
KEY_FILENAME = 'filename'
KEY_PATH = 'root'
KEY_PAYLOAD = 'format'
KEY_LINK = 'signac_link'
KEY_LINK_TYPE = 'link_type'
KEY_CRAWLER_PATH = 'access_crawler_root'
KEY_CRAWLER_MODULE = 'access_module'
KEY_CRAWLER_ID = 'access_crawler_id'
LINK_MODULE_FETCH = 'module_fetch'
KEY_GRIDFS_CONFIG = 'gridfs_config'

GRIDS = dict()
GRIDFS_LARGE_FILE_WARNING_THRSHLD = 1e9  # 1GB


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

    def fetch(self, doc):
        """Implement this generator method to associate data with a document.

        :yields: An iterable of arbitray objects."""
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
        :yields: An iterable of dict objects."""
        logger.info("Crawling '{}' (depth={})...".format(self.root, depth))
        for dirpath, dirnames, filenames in walkdepth(self.root, depth):
            for fn in filenames:
                for doc in self.docs_from_file(dirpath, fn):
                    logger.debug("doc from file: '{}'.".format(
                        os.path.join(dirpath, fn)))
                    doc.setdefault(KEY_PAYLOAD, None)
                    _id = doc.setdefault(
                        '_id', self._calculate_hash(doc, dirpath, fn))
                    yield _id, doc
        logger.info("Crawl of '{}' done.".format(self.root))

    def process(self, doc, dirpath, fn):
        """Implement this method for additional processing of generated docs.

        This method is particular useful to specialize non-abstract crawlers.
        The default implemenation will return the unmodified `doc`.

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
            if isinstance(regex, basestring):
                regex = re.compile(regex)
        else:
            if isinstance(regex, str):
                regex = re.compile(regex)
        cls.definitions[regex] = format_

    def docs_from_file(self, dirpath, fn):
        """Generate documents from filenames.

        This method is an implementation of the abstract method
        of :class:`~.BaseCrawler`.
        It is not recommended to reimplement this method to modify
        documents generated from filenames. See :meth:`~.process` instead."""
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
        :yields: An instance of the format associated with the
                  file used to generate `doc`.

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
                    yield format_(open(ffn, mode))

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
            except ValueError:
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


class MasterCrawler(BaseCrawler):

    """Crawl the data space and search for signac crawlers.

    The MasterCrawler executes signac slave crawlers
    defined in signac_access.py modules.

    If the master crawlers has defined tags, it will only
    execute slave crawlers with at least one matching tag.

    :param root: The path to the root directory to crawl through.
    :type root: str
    :param linkfs: Store the module path to enable file system fetch.
    :type linkfs: bool
    :param gridfs: Provide a GridFS configuration to export all data
        files into the specified GridFS.
    """
    def __init__(self, root, linkfs=True, gridfs_config=None):
        self.linkfs = linkfs
        self.gridfs = None
        self.gridfs_config = gridfs_config
        if self.gridfs_config is not None:
            self.gridfs = _get_gridfs(gridfs_config)
        self._crawlers = dict()
        super(MasterCrawler, self).__init__(root=root)

    def _store_files_to_gridfs(self, crawler, doc):
        import gridfs
        link = doc.setdefault(KEY_LINK, dict())
        link[KEY_GRIDFS_CONFIG] = self.gridfs_config
        for file in crawler.fetch(doc, mode='rb'):
            file_id = hashlib.md5(file.read()).hexdigest()
            file.seek(0)
            try:
                with self.gridfs.new_file(_id=file_id) as gridfile:
                    gridfile.write(file.read())
            except gridfs.errors.FileExists:
                pass
        link['file_id'] = file_id

    def _docs_from_module(self, dirpath, fn):
        name = os.path.join(dirpath, fn)
        module = _load_crawler(name)
        for crawler_id, crawler in module.get_crawlers(dirpath).items():
            if crawler.tags is not None and len(set(crawler.tags)):
                if self.tags is None or not len(set(self.tags)):
                    logger.info("Skipping, project has defined tags.")
                    continue
                elif not set(self.tags).intersection(set(crawler.tags)):
                    logger.info("Skipping, tag mismatch.")
                    continue
            for _id, doc in crawler.crawl():
                doc.setdefault(
                    KEY_PROJECT, os.path.relpath(dirpath, self.root))
                if self.linkfs:
                    link = doc.setdefault(KEY_LINK, dict())
                    link[KEY_LINK_TYPE] = LINK_MODULE_FETCH  # deprecated
                    link[KEY_CRAWLER_PATH] = os.path.abspath(dirpath)
                    link[KEY_CRAWLER_MODULE] = fn
                    link[KEY_CRAWLER_ID] = crawler_id
                if self.gridfs is not None:
                    self._store_files_to_gridfs(crawler, doc)
                yield doc

    def docs_from_file(self, dirpath, fn):
        if fn == FN_CRAWLER:
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


def fetch(doc, mode='r'):
    """Fetch all data associated with this document.

    :param doc: A document which is part of an index.
    :type doc: mapping
    :param mode: Mode to use for file opening ('r' or 'rb').
    :yields: Data associated with this document in the specified format."""
    if mode not in ('r', 'rb'):
        raise ValueError(mode)
    if doc is None:
        raise ValueError(doc)
    try:
        link = doc[KEY_LINK]
    except KeyError:
        logger.error(
            "This document is missing the '{}' key. "
            "Are you sure it is part of a signac index?".format(KEY_LINK))
        raise
    if KEY_CRAWLER_PATH in doc:
        logger.debug("Fetching files from the file system.")
        try:
            for file in _fetch_fs(doc, mode=mode):
                yield file
            return
        except OSError as error:
            logger.warning(
                "Unable to fetch file from file system: {}".format(error))
            if KEY_GRIDFS_CONFIG in link:
                logger.warning("Fallback to GridFS fetch.")
    if KEY_GRIDFS_CONFIG in link:
        logger.debug("Fetching files from GridFS.")
        try:
            for file in _fetch_gridfs(link, mode):
                yield file
            return
        except Exception:
            raise


def fetch_one(doc, mode='r'):
    """Fetch data associated with this document.

    Unlike fetch(), this function returns only the first
    file associated with doc and ignores all others.

    :param doc: A document which is part of an index.
    :type doc: mapping
    :param mode: Mode to use for file opening ('r' or 'rb').
    :yields: Data associated with this document in the specified format."""
    try:
        return next(fetch(doc, mode=mode))
    except Exception:
        raise
        return None


def fetched(docs):
    for doc in docs:
        for data in fetch(doc):
            yield doc, data


def _fetch_fs(doc, mode):
    "Fetch files for doc from the file system."
    link = doc[KEY_LINK]
    fn_module = os.path.join(
        link[KEY_CRAWLER_PATH], link[KEY_CRAWLER_MODULE])
    crawler_module = _load_crawler(fn_module)
    crawlers = crawler_module.get_crawlers(link[KEY_CRAWLER_PATH])
    for d in crawlers[link[KEY_CRAWLER_ID]].fetch(doc, mode=mode):
        yield d


def _fetch_gridfs(link, mode):
    "Fetch file for given link from gridfs."
    gridfs_config = link[KEY_GRIDFS_CONFIG]
    grid_id = hashlib.md5(
        json.dumps(gridfs_config, sort_keys=True).encode()).hexdigest()
    grid = GRIDS.setdefault(grid_id, _get_gridfs(gridfs_config))
    if mode == 'r':
        file = io.StringIO(grid.get(link['file_id']).read().decode())
        if len(file.getvalue()) > GRIDFS_LARGE_FILE_WARNING_THRSHLD:
            warnings.warn(
                "Open large GridFS files more efficiently in 'rb' mode.")
        yield file
    elif mode == 'rb':
        yield grid.get(link['file_id'])
    else:
        raise ValueError(mode)


def _get_gridfs(gridfs_config):
    "Return the gridfs for given configuration."
    import gridfs
    grid_db = get_database(
        gridfs_config['db'],
        hostname=gridfs_config.get('hostname'))
    return gridfs.GridFS(grid_db)


def export_pymongo(crawler, index, chunksize=1000, *args, **kwargs):
    """Optimized export function for pymongo collections.

    The behaviour of this function is equivalent to:

    .. code-block:: python

        for _id, doc in crawler.crawl(*args, **kwargs):
            index.replace_one({'_id': _id}, doc)

    :param crawler: The crawler to execute.
    :param index: A index collection to export to.
    :param chunksize: The buffer size for export operations.
    :type chunksize: int
    :param args: Extra arguments and keyword arguments are
                 forwarded to the crawler's crawl() method."""
    import pymongo
    logger.info("Exporting index for pymongo.")
    operations = []
    for _id, doc in crawler.crawl(*args, **kwargs):
        f = {'_id': _id}
        assert doc['_id'] == _id
        operations.append(pymongo.ReplaceOne(f, doc, upsert=True))
        if len(operations) >= chunksize:
            logger.debug("Pushing chunk.")
            index.bulk_write(operations)
            operations.clear()
    if len(operations):
        logger.debug("Pushing final chunk.")
        index.bulk_write(operations)


def export(crawler, index, *args, **kwargs):
    """Optimized export function for collections.

    The behaviour of this function is equivalent to:

    .. code-block:: python

        for _id, doc in crawler.crawl(*args, **kwargs):
            index.replace_one({'_id': _id}, doc)

    :param crawler: The crawler to execute.
    :param index: A index collection to export to.
    :param args: Extra arguments and keyword arguments are
                 forwarded to the crawler's crawl() method."""
    logger.info("Exporting index.")
    for _id, doc in crawler.crawl(*args, **kwargs):
        f = {'_id': _id}
        index.replace_one(f, doc)
