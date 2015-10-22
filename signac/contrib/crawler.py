import os
import re
import logging
import json
import importlib

from .utility import walkdepth
from .hashing import calc_id


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

    def __init__(self, root):
        self.root = root

    def docs_from_file(self, dirpath, fn):
        raise NotImplementedError()

    def fetch(self, doc):
        return
        yield

    @classmethod
    def calculate_hash(cls, doc, dirpath, fn):
        import hashlib
        blob = json.dumps(doc, sort_keys=True)
        m = hashlib.md5()
        m.update(dirpath.encode('utf-8'))
        m.update(fn.encode('utf-8'))
        m.update(blob.encode('utf-8'))
        return m.hexdigest()

    def crawl(self, depth=0):
        logger.info("Crawling '{}' (depth={})...".format(self.root, depth))
        for dirpath, dirnames, filenames in walkdepth(self.root, depth):
            for fn in filenames:
                for doc in self.docs_from_file(dirpath, fn):
                    logger.debug("doc from file: '{}'.".format(
                        os.path.join(dirpath, fn)))
                    doc.setdefault(KEY_PAYLOAD, None)
                    _id = doc.setdefault(
                        '_id', self.calculate_hash(doc, dirpath, fn))
                    yield _id, doc
        logger.info("Crawl of '{}' done.".format(self.root))

    def process(self, doc, dirpath, fn):
        return doc


class RegexFileCrawler(BaseCrawler):
    definitions = dict()

    @classmethod
    def define(cls, regex, format_):
        cls.definitions[regex] = format_

    def docs_from_file(self, dirpath, fn):
        for regex, format_ in self.definitions.items():
            m = regex.search(os.path.join(dirpath, fn))
            if m:
                doc = self.process(m.groupdict(), dirpath, fn)
                doc[KEY_FILENAME] = os.path.relpath(
                    os.path.join(dirpath, fn), self.root)
                doc[KEY_PATH] = os.path.abspath(self.root)
                doc[KEY_PAYLOAD] = str(format_)
                yield doc

    def fetch(self, doc):
        fn = doc.get(KEY_FILENAME)
        if fn:
            for regex, format_ in self.definitions.items():
                ffn = os.path.join(self.root, fn)
                m = regex.search(ffn)
                if m:
                    yield format_(open(ffn))

    def process(self, doc, dirpath, fn):
        result = dict()
        types = (int, float)
        for key, value in doc.items():
            if isinstance(value, bool):
                result[key] = value
                continue
            for t in types:
                try:
                    result[key] = t(value)
                    break
                except ValueError:
                    continue
            else:
                result[key] = value
        return super().process(result, dirpath, fn)


class JSONCrawler(BaseCrawler):
    encoding = 'utf-8'
    fn_regex = '.*\.json'

    def docs_from_json(self, doc):
        yield doc

    def docs_from_file(self, dirpath, fn):
        if re.match(self.fn_regex, os.path.join(dirpath, fn)):
            with open(os.path.join(dirpath, fn), 'rb') as file:
                doc = json.loads(file.read().decode(self.encoding))
                yield from self.docs_from_json(doc)


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
    re_job_document = '.*signac_job_document\.json'

    def docs_from_file(self, dirpath, fn):
        if re.match(self.re_job_document, fn):
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
        yield from super().docs_from_file(dirpath, fn)


class SignacProjectCrawler(
        SignacProjectRegexFileCrawler,
        SignacProjectJobDocumentCrawler):
    pass


class MasterCrawler(BaseCrawler):

    def __init__(self, root):
        super(MasterCrawler, self).__init__(root=root)
        self._crawlers = dict()

    def _load_crawler(self, name):
        return importlib.machinery.SourceFileLoader(name, name).load_module()

    def _docs_from_module(self, dirpath, fn):
        name = os.path.join(dirpath, fn)
        module = self._load_crawler(name)
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
                yield from self._docs_from_module(dirpath, fn)
            except AttributeError as error:
                if str(error) == 'get_crawlers':
                    logger.warning(
                        "Module has no '{}' function.".format(error))
            except Exception:
                logger.error("Error while indexing from module '{}'.".format(
                    os.path.join(dirpath, fn)))
                raise

    def fetch(self, doc):
        yield from fetch(doc)


def _load_crawler(name):
    return importlib.machinery.SourceFileLoader(name, name).load_module()


def fetch(doc):
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
        yield from crawlers[link[KEY_CRAWLER_ID]].fetch(doc)


def fetched(docs):
    for doc in docs:
        for data in fetch(doc):
            yield doc, data


def export_pymongo(crawler, index, chunksize=1000, *args, **kwargs):
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
    logger.info("Exporting index.")
    for _id, doc in crawler.crawl(*args, **kwargs):
        f = {'_id': _id}
        index.replace_one(f, doc)
