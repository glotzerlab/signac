# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import io
import re
import json
import logging

import signac
import signac.db
from signac.contrib import indexing
from signac.common import six
from signac.common import errors

try:
    signac.db.get_database('testing', hostname='testing')
except signac.common.errors.ConfigError:
    SKIP_REASON = "No 'testing' host configured."
except ImportError:
    SKIP_REASON = "pymongo not available"
else:
    SKIP_REASON = None

if six.PY2:
    logging.basicConfig(level=logging.WARNING)
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


SIGNAC_ACCESS_MODULE = """import os
import re

from signac.contrib import RegexFileCrawler

RE_TXT = r".*a_(?P<a>\d)\.txt"

class Crawler(RegexFileCrawler):
    tags = {'test1', 'test2'}

Crawler.define(RE_TXT, 'TextFile')

def get_crawlers(root):
    return {'main':  Crawler(os.path.join(root, '.'))}
"""


class TestFormat(object):

    def read(self):
        assert 0

    def close(self):
        assert 0


class TestCollection(object):

    def __init__(self):
        self.docs = dict()
        self.called = False

    def replace_one(self, filter, doc, upsert=False):
        self.called = True
        assert len(filter) == 1
        _id = filter['_id']
        assert doc['_id'] == _id
        if not upsert and _id not in self.docs:
            return
        else:
            self.docs[_id] = doc

    def find_one(self, filter):
        assert len(filter) == 1
        return self.docs.get(filter['_id'])

    def __contains__(self, _id):
        return _id in self.docs


class TestFS(object):
    name = 'inmemorytestgrid'
    files = dict()

    class FileExistsError(IOError):
        pass
    FileNotFoundError = KeyError

    class _Writer(io.BytesIO):

        def __init__(self, cache, file_id):
            self.cache = cache
            self.file_id = file_id

        def close(self):
            self.cache[self.file_id] = self.getvalue()
            super(TestFS._Writer, self).close()

    def __init__(self, _id='testfs'):
        self._id = _id

    def config(self):
        return {'id': self._id}

    @classmethod
    def from_config(cls, config):
        return TestFS(_id=config['id'])

    def new_file(self, mode='xb', **kwargs):
        _id = kwargs['_id']
        cache = self.files.setdefault(self._id, dict())
        if _id in cache:
            raise self.FileExistsError(_id)
        if mode == 'xb':
            return self._Writer(cache, _id)
        else:
            raise ValueError(mode)

    def get(self, file_id, mode='r'):
        cache = self.files[self._id]
        buf = cache[file_id]
        if mode == 'r':
            return io.StringIO(buf.decode())
        elif mode == 'rb':
            return io.BytesIO(buf)
        else:
            raise ValueError(mode)


class IndexingBaseTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)

    def setup_project(self):
        def fn(name):
            return os.path.join(self._tmp_dir.name, name)
        with open(fn('a_0.txt'), 'w') as file:
            file.write('{"a": 0}')
        with open(fn('a_1.txt'), 'w') as file:
            file.write('{"a": 1}')
        with open(fn('a_0.json'), 'w') as file:
            json.dump(dict(a=0), file)
        with open(fn('a_1.json'), 'w') as file:
            json.dump(dict(a=1), file)
        with open(fn('signac_access.py'), 'w') as module:
            module.write(SIGNAC_ACCESS_MODULE)

    def get_index_collection(self):
        return TestCollection()

    def test_base_crawler(self):
        crawler = indexing.BaseCrawler(root=self._tmp_dir.name)
        self.assertEqual(len(list(crawler.crawl())), 0)
        doc = dict(a=0)
        with self.assertRaises(errors.FetchError):
            self.assertIsNone(crawler.fetch(doc))
        self.assertEqual(doc, crawler.process(doc, None, None))
        with self.assertRaises(NotImplementedError):
            crawler.docs_from_file(None, None)

    def test_regex_file_crawler_pre_compiled(self):
        self.setup_project()

        class Crawler(indexing.RegexFileCrawler):
            pass

        regex = re.compile(".*a_(?P<a>\d)\.txt")
        Crawler.define(regex, TestFormat)
        crawler = Crawler(root=self._tmp_dir.name)
        no_find = True
        for doc in crawler.crawl():
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            m = regex.match(ffn)
            self.assertIsNotNone(m)
            self.assertTrue(os.path.isfile(ffn))
            with open(ffn) as file:
                doc2 = json.load(file)
                self.assertEqual(doc2['a'], doc['a'])
        self.assertFalse(no_find)

    def test_regex_file_crawler(self):
        self.setup_project()

        class Crawler(indexing.RegexFileCrawler):
            pass

        # First test without pattern
        crawler = Crawler(root=self._tmp_dir.name)
        self.assertEqual(len(list(crawler.crawl())), 0)

        # Now with pattern(s)
        pattern = ".*a_(?P<a>\d)\.txt"
        pattern_false = "nomatch"
        regex = re.compile(pattern)
        Crawler.define(pattern, TestFormat)
        Crawler.define("negativematch", "negativeformat")
        crawler = Crawler(root=self._tmp_dir.name)
        no_find = True
        for doc in crawler.crawl():
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            m = regex.match(ffn)
            self.assertIsNotNone(m)
            self.assertTrue(os.path.isfile(ffn))
            with open(ffn) as file:
                doc2 = json.load(file)
                self.assertEqual(doc2['a'], doc['a'])
        self.assertFalse(no_find)
        with self.assertRaises(errors.FetchError):
            crawler.fetch(dict())
        with self.assertRaises(errors.FetchError):
            crawler.fetch({'filename': 'shouldnotmatch'})

    def test_regex_file_crawler_inheritance(self):
        self.setup_project()

        class CrawlerA(indexing.RegexFileCrawler):
            pass

        class CrawlerB(indexing.RegexFileCrawler):
            pass

        CrawlerA.define('a', TestFormat)
        CrawlerB.define('b', TestFormat)
        self.assertEqual(len(CrawlerA.definitions), 1)
        self.assertEqual(len(CrawlerB.definitions), 1)

        class CrawlerC(CrawlerA):
            pass

        self.assertEqual(len(CrawlerA.definitions), 1)
        self.assertEqual(len(CrawlerC.definitions), 1)
        self.assertEqual(len(CrawlerB.definitions), 1)
        CrawlerC.define('c', TestFormat)
        self.assertEqual(len(CrawlerA.definitions), 1)
        self.assertEqual(len(CrawlerB.definitions), 1)
        self.assertEqual(len(CrawlerC.definitions), 2)

    def test_json_crawler(self):
        self.setup_project()
        crawler = indexing.JSONCrawler(root=self._tmp_dir.name)
        docs = list(sorted(crawler.crawl(), key=lambda d: d['a']))
        self.assertEqual(len(docs), 2)
        for i, doc in enumerate(docs):
            self.assertEqual(doc['a'], i)
            self.assertIsNone(doc['format'])
        ids = set(doc['_id'] for doc in docs)
        self.assertEqual(len(ids), len(docs))

    def test_master_crawler(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        no_find = True
        for doc in crawler.crawl():
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            self.assertTrue(os.path.isfile(ffn))
            with open(ffn) as file:
                doc2 = json.load(file)
                self.assertEqual(doc2['a'], doc['a'])
            with signac.fetch(doc) as file:
                pass
        self.assertFalse(no_find)

    def test_fetch(self):
        with self.assertRaises(ValueError):
            signac.fetch(None)
        with self.assertRaises(errors.FetchError):
            signac.fetch(dict())
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        docs = list(crawler.crawl())
        self.assertEqual(len(docs), 2)
        for doc in docs:
            with signac.fetch(doc) as file:
                pass
        for doc, file in indexing.fetched(docs):
            doc2 = json.load(file)
            self.assertEqual(doc['a'], doc2['a'])
            file.close()

    def test_export_one(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = self.get_index_collection()
        for doc in crawler.crawl():
            signac.export_one(doc, index)
        self.assertTrue(index.called)
        for doc in crawler.crawl():
            self.assertIsNotNone(index.find_one({'_id': doc['_id']}))

    def test_export(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = self.get_index_collection()
        signac.export(crawler.crawl(), index)
        self.assertTrue(index.called)
        for doc in crawler.crawl():
            self.assertIsNotNone(index.find_one({'_id': doc['_id']}))

    def test_export_to_mirror(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = self.get_index_collection()
        mirror = TestFS()
        for doc in crawler.crawl():
            self.assertIn('file_id', doc)
            doc.pop('file_id')
            with self.assertRaises(errors.ExportError):
                signac.export_to_mirror(doc, mirror)
            break
        for doc in crawler.crawl():
            self.assertIn('file_id', doc)
            signac.export_one(doc, index)
            signac.export_to_mirror(doc, mirror)
        self.assertTrue(index.called)
        for doc in crawler.crawl():
            self.assertIsNotNone(index.find_one({'_id': doc['_id']}))
            with mirror.get(doc['file_id']):
                pass

    def test_master_crawler_tags(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        self.assertEqual(0, len(list(crawler.crawl())))
        crawler.tags = None
        self.assertEqual(0, len(list(crawler.crawl())))
        crawler.tags = {}
        self.assertEqual(0, len(list(crawler.crawl())))
        crawler.tags = {'nomatch'}
        self.assertEqual(0, len(list(crawler.crawl())))
        crawler.tags = {'test1'}
        self.assertEqual(2, len(list(crawler.crawl())))
        crawler.tags = {'test2'}
        self.assertEqual(2, len(list(crawler.crawl())))
        crawler.tags = {'test1', 'test2'}
        self.assertEqual(2, len(list(crawler.crawl())))
        crawler.tags = {'test1', 'bs'}
        self.assertEqual(2, len(list(crawler.crawl())))
        crawler.tags = {'test2', 'bs'}
        self.assertEqual(2, len(list(crawler.crawl())))
        crawler.tags = {'test1', 'test2', 'bs'}
        self.assertEqual(2, len(list(crawler.crawl())))


@unittest.skipIf(SKIP_REASON is not None, SKIP_REASON)
class IndexingPyMongoTest(IndexingBaseTest):

    def get_index_collection(self):
        db = signac.db.get_database('testing', hostname='testing')
        return db.test_index

if __name__ == '__main__':
    unittest.main()
