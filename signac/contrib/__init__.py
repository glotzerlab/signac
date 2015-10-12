import os
import logging
import json
import uuid
import importlib
import itertools

from .project import Project

logger = logging.getLogger(__name__)

KEY_CRAWLER_PATH = 'signac_crawler_path'
KEY_CRAWLER_MODULE = 'signac_crawler_module'
KEY_CRAWLER_ID = 'signac_crawler_id'
KEY_PROJECT = 'project'
FN_CRAWLER = 'signac_crawler.py'

def get_project(project_path=None):
    if project_path is not None:
        cwd = os.getcwd()
        os.chdir(project_path)
        project = Project()
        os.chdir(cwd)
    else:
        project = Project()
    project.get_id()  # sanity check
    return project

def calculate_hash(doc, dirpath, fn):
    import hashlib
    blob = json.dumps(doc, sort_keys=True)
    m = hashlib.md5()
    m.update(dirpath.encode('utf-8'))
    m.update(fn.encode('utf-8'))
    m.update(blob.encode('utf-8'))
    return m.hexdigest()


class SimpleCollection(object):

    def __init__(self):
        self._index = dict()

    def insert_one(self, doc):
        _id = doc.setdefault('_id', uuid.uuid4())
        self._index[_id] = doc

    def insert_many(self, docs):
        for doc in docs:
            self.insert_one(doc)

    def replace_one(self, filter, doc):
        if not list(filter.keys()) == ['_id']:
            raise NotImplementedError("Simple collection can only be queried for _id.")
        self._index[filter['_id']] = doc

    def find(self):
        yield from self._index.values()


class BaseCrawler(object):

    def __init__(self, root):
        self.root = root

    def file_matches(self, dirpath, fn):
        raise NotImplementedError()

    def docs_from_file(self, dirpath, fn):
        raise NotImplementedError()

    def fetch(self, doc):
        return 

    def crawl(self):
        logger.debug("Starting crawl...")
        for dirpath, dirnames, filenames in os.walk(self.root):
            for fn in filenames:
                if self.file_matches(dirpath, fn):
                    logger.debug("Matched file: '{}'.".format(os.path.join(dirpath, fn)))
                    for doc in self.docs_from_file(dirpath, fn):
                        _id = doc.setdefault('_id', calculate_hash(doc, dirpath, fn))
                        yield _id, doc
        logger.debug("Done.")

    def build_index(self):
        logger.debug("Building index for '{}'...".format(self.root))
        self._index = dict()
        for _id, doc in self.crawl():
            self._index[_id] = doc

    def fetched(self, docs):
        for doc in docs:
            for data in self.fetch(doc):
                yield doc, data


class JSONCrawler(BaseCrawler):
    encoding='utf-8'
    
    def file_matches(self, dirpath, fn):
        return os.path.splitext(fn)[1] == '.json'

    def docs_from_json(self, doc):
        yield doc

    def docs_from_file(self, dirpath, fn):
        with open(os.path.join(dirpath, fn), 'rb') as file:
            doc = json.loads(file.read().decode(self.encoding))
            yield from self.docs_from_json(doc)


class ProjectCrawler(BaseCrawler):

    def __init__(self, root):
        super(ProjectCrawler, self).__init__(root=root)
        self._crawlers = dict()
    
    def file_matches(self, dirpath, fn):
        return fn == FN_CRAWLER

    def _load_crawler(self, name):
        return importlib.machinery.SourceFileLoader(name, name).load_module()

    def docs_from_file(self, dirpath, fn):
        name = os.path.join(dirpath, fn)
        module = self._load_crawler(name)
        for crawler_id, crawler in module.get_crawlers(dirpath).items():
            for _id, doc in crawler.crawl():
                doc.setdefault(KEY_PROJECT, dirpath)
                doc[KEY_CRAWLER_PATH] = dirpath
                doc[KEY_CRAWLER_MODULE] = name
                doc[KEY_CRAWLER_ID] = crawler_id
                yield doc

    def fetch(self, doc):
        crawler_module = self._load_crawler(doc[KEY_CRAWLER_MODULE])
        crawlers = crawler_module.get_crawlers(doc[KEY_CRAWLER_PATH])
        try:
            yield from crawlers[doc[KEY_CRAWLER_ID]].fetch(doc)
        except KeyError:
            raise KeyError("Unable to load associated crawler.")


def export_pymongo(crawler, collection, chunksize=1000):
    import pymongo
    logger.debug("Exporting index for pymongo.")
    operations = []
    for _id, doc in crawler.crawl():
        f = {'_id': _id}
        assert doc['_id'] == _id
        operations.append(pymongo.ReplaceOne(f, doc, upsert=True))
        if len(operations) >= chunksize:
            logger.debug("Pushing chunk.")
            collection.bulk_write(operations)
            operations.clear()
    if len(operations):
        logger.debug("Pushing final chunk.")
        collection.bulk_write(operations)

def export(crawler, collection):
    logger.debug("Exporting index.")
    for _id, doc in crawler.crawl():
        f = {'_id': _id}
        collection.replace_one(f, doc)
