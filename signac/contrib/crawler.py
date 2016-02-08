import os
import re
import json
import six
import math
import io
import errno
import hashlib
import logging
import warnings

try:
    import gridfs
except ImportError:
    GRIDFS = False
else:
    GRIDFS = True

from .utility import walkdepth
from .hashing import calc_id
from ..db import get_database

if six.PY2:
    import imp
    from collections import Mapping
else:
    import importlib.machinery
    from collections.abc import Mapping


logger = logging.getLogger(__name__)

FN_CRAWLER = 'signac_access.py'
KEY_PROJECT = 'project'
KEY_FILENAME = 'filename'
KEY_PATH = 'root'
KEY_PAYLOAD = 'format'
KEY_LINK = 'signac_link'
KEY_CRAWLER_PATH = 'access_crawler_root'
KEY_CRAWLER_MODULE = 'access_module'
KEY_CRAWLER_ID = 'access_crawler_id'

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

        The return value of this generator function is not directly defined,
        however it is recommneded to us `file-like object`_s.

        .. _`file-like object`: https://docs.python.org/3/glossary.html#term-file-object

        :yields: An iterable of arbitrary objects."""
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
            if isinstance(regex, basestring):
                regex = re.compile(regex)
        else:
            if isinstance(regex, str):
                regex = re.compile(regex)
        for meth in ('read', 'close'):
            if not callable(getattr(format_, meth, None)):
                msg = "Format {} has no {}() method.".format(format_, meth)
                warnings.warn(msg)
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


class LocalFS(object):
    """A file system handler for the local file system."""
    name = 'localfs'
    FileExistsError = IOError
    FileNotFoundError = IOError

    def __init__(self, root):
        self.root = root

    def config(self):
        return {'root': self.root}

    def __repr__(self):
        return '{}({})'.format(
            type(self),
            ', '.join('{}={}'.format(k, v) for k, v in self.config().items()))

    @classmethod
    def from_config(cls, config):
        return LocalFS(root=config['root'])

    def _fn(self, file_id, n=2, suffix='.dat'):
        fn = os.path.join(
            self.root,
            * [file_id[i:i + n] for i in range(0, len(file_id), n)]) + suffix
        return fn

    def new_file(self, **kwargs):
        assert 'x' in kwargs.get('mode', 'x')
        file_id = kwargs.get('_id', calc_id(kwargs))
        fn = self._fn(file_id)
        try:
            path = os.path.dirname(fn)
            os.makedirs(path)
        except OSError as error:
            if not (error.errno == errno.EEXIST and os.path.isdir(path)):
                raise
        return open(fn, mode='wxb' if six.PY2 else 'xb')

    def get(self, file_id, mode='r'):
        assert 'r' in mode
        return open(self._fn(file_id), mode=mode)


class GridFS(object):
    """A file system handler for the MongoDB GridFS file system."""
    name = 'gridfs'

    FileExistsError = gridfs.errors.FileExists
    FileNotFoundError = gridfs.errors.NoFile

    def __init__(self, db, collection='fs'):
        if isinstance(db, str):
            self.db = None
            self.db_name = db
        else:
            self.db = db
            self.db_name = db.name
        self.collection = collection
        self._gridfs = None

    def config(self):
        return {'db': self.db_name, 'collection': self.collection}

    def __repr__(self):
        return '{}({})'.format(
            type(self),
            ', '.join('{}={}'.format(k, v) for k, v in self.config().items()))

    @classmethod
    def from_config(cls, config):
        return GridFS(db=config['db'], collection=config.get('collection'))

    @property
    def gridfs(self):
        if self._gridfs is None:
            if self.db is None:
                self.db = get_database(self.db_name)
            self._gridfs = gridfs.GridFS(self.db, collection=self.collection)
        return self._gridfs

    def new_file(self, **kwargs):
        return self.gridfs.new_file(** kwargs)

    def get(self, file_id, mode='r'):
        if mode == 'r':
            file = io.StringIO(self.gridfs.get(file_id).read().decode())
            if len(file.getvalue()) > GRIDFS_LARGE_FILE_WARNING_THRSHLD:
                warnings.warn(
                    "Open large GridFS files more efficiently in 'rb' mode.")
            return file
        elif mode == 'rb':
            return self.gridfs.get(file_id=file_id)
        else:
            raise ValueError(mode)


def _store_files_to_filesystem(filesystem, crawler, doc):
    link = doc.setdefault(KEY_LINK, dict())
    fs_config = link.setdefault('filesystems', list())
    fs_config.append({filesystem.name: filesystem.config()})
    file_ids = link.setdefault('file_ids', list())
    for file in crawler.fetch(doc, mode='rb'):
        file_id = hashlib.md5(file.read()).hexdigest()
        file.seek(0)
        try:
            with filesystem.new_file(_id=file_id) as filesystemfile:
                filesystemfile.write(file.read())
        except filesystem.FileExistsError:
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
    :param filesystems: An optional set of file systems, to export
        data to.
    :type filesystems: A sequence of filesystem-like objects or
        filesystem configurations. See :funct:`~fetch` for details.
    """

    def __init__(self, root, link_local=True, filesystems=None):
        self.link_local = link_local
        if filesystems is None:
            self.filesystems = list()
        else:
            self.filesystems = list(_filesystems_from_config(filesystems))
        self._crawlers = dict()
        super(MasterCrawler, self).__init__(root=root)

    def _docs_from_module(self, dirpath, fn):
        name = os.path.join(dirpath, fn)
        module = _load_crawler(name)
        for crawler_id, crawler in module.get_crawlers(dirpath).items():
            try:
                tags = crawler.tags
            except AttributeError:
                pass
            else:
                if tags is not None and len(set(tags)):
                    if self.tags is None or not len(set(self.tags)):
                        logger.info("Skipping, project has defined tags.")
                        continue
                    elif not set(self.tags).intersection(set(crawler.tags)):
                        logger.info("Skipping, tag mismatch.")
                        continue
            for _id, doc in crawler.crawl():
                doc.setdefault(
                    KEY_PROJECT, os.path.relpath(dirpath, self.root))
                if hasattr(crawler, 'fetch'):
                    if self.link_local:
                        link = doc.setdefault(KEY_LINK, dict())
                        link['link_type'] = 'module_fetch'  # deprecated
                        link[KEY_CRAWLER_PATH] = os.path.abspath(dirpath)
                        link[KEY_CRAWLER_MODULE] = fn
                        link[KEY_CRAWLER_ID] = crawler_id
                    for filesystem in self.filesystems:
                        _store_files_to_filesystem(filesystem, crawler, doc)
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


def _filesystems_from_config(fs_config):
    for item in fs_config:
        if isinstance(item, Mapping):
            for key in item:
                if key == 'localfs':
                    if isinstance(item[key], Mapping):
                        yield LocalFS.from_config(item[key])
                    else:
                        yield LocalFS(item[key])
                elif key == 'gridfs':
                    if GRIDFS:
                        if isinstance(item[key], Mapping):
                            yield GridFS.from_config(item[key])
                        else:
                            yield GridFS(item[key])
                    else:
                        warnings.warn("gridfs not available!")
                else:
                    warnings.warn("Unknown filesystem type '{}'.".format(key))
        else:
            yield item


def fetch(doc, mode='r', filesystems=None, ignore_linked_fs=False):
    """Fetch all data associated with this document.

    The filesystems argument is either a list of filesystem-like objects,
    see :class:`~LocalFS` for an example, or a list of filesystem
    configurations.
    The latter is a slightly shorter notation, e.g.:

    .. code-block:
        fetch(doc, filesystems=[{'localfs': '/path/to/storage'}])

    :param doc: A document which is part of an index.
    :type doc: mapping
    :param mode: Mode to use for file opening.
    :param filesystems: An optional set of filesystems to fetch files from.
    :type filesystems: A sequence of filesystem-like objects or
        filesystem configurations.
    :param ignore_linked_fs: Ignore all file system information in the
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
        logger.debug("Fetching files from the file system.")
        try:
            for file in _fetch_fs(doc, mode=mode):
                yield file
            return
        except OSError as error:
            logger.warning(
                "Unable to fetch file from file system: {}".format(error))
    to_fetch = set(link.pop('file_ids', []))
    n = len(to_fetch)
    if n == 0:
        return
    if filesystems is None:
        filesystems = list()
    else:
        filesystems = list(_filesystems_from_config(filesystems))
    if not ignore_linked_fs:
        filesystems.extend(
            list(_filesystems_from_config(link.get('filesystems', list()))))
    logger.debug("Using filesystems to fetch files: {}".format(filesystems))
    for filesystem in filesystems:
        fetched = set()
        for file_id in to_fetch:
            logger.debug("Fetching file with id '{}'.".format(file_id))
            try:
                yield filesystem.get(file_id, mode=mode)
                fetched.add(file_id)
            except filesystem.FileNotFoundError:
                continue
        for file_id in fetched:
            to_fetch.remove(file_id)
    if to_fetch:
        msg = "Unable to fetch {}/{} file(s) associated with this document ."
        raise IOError(msg.format(len(to_fetch), n))


def fetch_one(doc, *args, **kwargs):
    """Fetch data associated with this document.

    Unlike fetch(), this function returns only the first
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
    "Fetch files for doc from the file system."
    link = doc[KEY_LINK]
    fn_module = os.path.join(
        link[KEY_CRAWLER_PATH], link[KEY_CRAWLER_MODULE])
    crawler_module = _load_crawler(fn_module)
    crawlers = crawler_module.get_crawlers(link[KEY_CRAWLER_PATH])
    for d in crawlers[link[KEY_CRAWLER_ID]].fetch(doc, mode=mode):
        yield d


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
