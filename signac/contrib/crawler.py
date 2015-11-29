import os
import re
import logging
import json
import six

from .utility import walkdepth
from .hashing import calc_id

if six.PY3:
    import importlib.machinery
else:
    import imp


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


class BaseCrawler(object):
    """Crawl through `root` and index all files.

    The crawler creates an index on data, which can be exported
    to a database for easier access."""

    def __init__(self, root):
        """Initialize a BaseCrawler instance.

        :param root: The path to the root directory to crawl through.
        :type root: str"""
        self.root = root

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

    A regular expression crawler for this structure could be implemented
    like this:

    .. code::

        import re

        class TextFile(object):
            def __init__(self, file):
                # file is a file-like object
                return file.read()

        # This expressions yields mappings of the type: {'a': value_of_a}.
        RE_TXT = re.compile('a_(?P<a>\d+).txt')

        MyCrawler(RegexFileCrawler): pass
        MyCrawler.define(RE_TXT, TextFile)

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
            must match this expression.
        :type regex: `compiled regular expression`_
        :param format_: The format associated with all matching files.
        :type format_: :class:`object`

        .. _`compiled regular expression`: https://docs.python.org/3.4/library/re.html#re-objects"""
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

    def fetch(self, doc):
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
                    yield format_(open(ffn))

    def process(self, doc, dirpath, fn):
        """Post-process documents generated from filenames.

        Example:

        .. code::

            MyCrawler(signac.contrib.crawler.RegexFileCrawler):
                def process(self, doc, dirpath, fn):
                    doc['long_name_for_a'] = doc['a']
                    return super().process(doc, dirpath, fn)

        :param dirpath: The path of the file, relative to `root`.
        :type dirpath: str
        :param fn: The filename.
        :type fn: str
        :returns: A document, that means an instance of mapping.
        :rtype: mapping"""
        result = dict()
        for key, value in doc.items():
            if isinstance(value, bool):
                result[key] = value
                continue
            try:
                float(value)
            except ValueError:
                result[key] = value
            else:
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
                    return d


class SignacProjectBaseCrawler(BaseCrawler):
    encoding = 'utf-8'
    fn_statepoint = 'signac_statepoint.json'

    def get_statepoint(self, dirpath):
        with open(os.path.join(dirpath, self.fn_statepoint), 'rb') as file:
            doc = json.loads(file.read().decode(self.encoding))
        signac_id = calc_id(doc)
        assert dirpath.endswith(signac_id)
        return signac_id, doc

    def process(self, doc, dirpath, fn):
        signac_id, statepoint = self.get_statepoint(dirpath)
        doc.update(statepoint)
        doc['signac_id'] = signac_id
        return super().process(doc, dirpath, fn)


class SignacProjectRegexFileCrawler(
        SignacProjectBaseCrawler,
        RegexFileCrawler):
    pass


class SignacProjectJobDocumentCrawler(SignacProjectBaseCrawler):
    re_job_document = '.*\/signac_job_document\.json'

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
            job_doc['_id'] = signac_id
            job_doc['statepoint'] = statepoint
            yield job_doc
        for doc in super(SignacProjectJobDocumentCrawler, self).docs_from_file(
                dirpath, fn):
            yield doc


class SignacProjectCrawler(
        SignacProjectRegexFileCrawler,
        SignacProjectJobDocumentCrawler):
    pass


class MasterCrawler(BaseCrawler):

    def __init__(self, root):
        super(MasterCrawler, self).__init__(root=root)
        self._crawlers = dict()

    def _docs_from_module(self, dirpath, fn):
        name = os.path.join(dirpath, fn)
        module = _load_crawler(name)
        for crawler_id, crawler in module.get_crawlers(dirpath).items():
            for _id, doc in crawler.crawl():
                doc.setdefault(
                    KEY_PROJECT, os.path.relpath(dirpath, self.root))
                link = doc.setdefault(KEY_LINK, dict())
                link[KEY_LINK_TYPE] = LINK_MODULE_FETCH
                link[KEY_CRAWLER_PATH] = os.path.abspath(dirpath)
                link[KEY_CRAWLER_MODULE] = fn
                link[KEY_CRAWLER_ID] = crawler_id
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
    if six.PY3:
        return importlib.machinery.SourceFileLoader(name, name).load_module()
    else:
        return imp.load_source(os.path.splitext(name)[0], name)


def fetch(doc):
    """Fetch all data associated with this document.

    :param doc: A document which is part of an index.
    :type doc: mapping
    :yields: Data associated with this document in the specified format."""
    try:
        link = doc[KEY_LINK]
    except KeyError:
        logger.error(
            "This document is missing the '{}' key. "
            "Are you sure it is part of a signac index?".format(KEY_LINK))
        raise
    if link[KEY_LINK_TYPE] == LINK_MODULE_FETCH:
        fn_module = os.path.join(
            link[KEY_CRAWLER_PATH], link[KEY_CRAWLER_MODULE])
        crawler_module = _load_crawler(fn_module)
        crawlers = crawler_module.get_crawlers(link[KEY_CRAWLER_PATH])
        for d in crawlers[link[KEY_CRAWLER_ID]].fetch(doc):
            yield d


def fetched(docs):
    for doc in docs:
        for data in fetch(doc):
            yield doc, data


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
