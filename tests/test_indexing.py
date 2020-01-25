# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
import os
import io
import re
import json

import signac
import signac.db
from signac import Collection
from signac.contrib import indexing
from signac.common import errors

try:
    signac.db.get_database('testing', hostname='testing')
except signac.common.errors.ConfigError:
    SKIP_REASON = "No 'testing' host configured."
except ImportError:
    SKIP_REASON = "pymongo not available"
else:
    SKIP_REASON = None

from tempfile import TemporaryDirectory
from unittest.mock import Mock
import pytest


SIGNAC_ACCESS_MODULE_LEGACY = r"""import os
import re

from signac.contrib import RegexFileCrawler

RE_TXT = r".*a_(?P<a>\d)\.txt"

class Crawler(RegexFileCrawler):
    tags = {'test1', 'test2'}

Crawler.define(RE_TXT, 'TextFile')

def get_crawlers(root):
    yield Crawler(root)
"""

SIGNAC_ACCESS_MODULE = r"""import signac

def get_indexes(root):
    yield signac.index_files(root, r'.*a_(?P<a>\d)\.txt')

get_indexes.tags = {'test1', 'test2'}
"""

SIGNAC_ACCESS_MODULE_GET_CRAWLERS = r"""import signac

class Crawler(signac.RegexFileCrawler):
    tags = {'test1', 'test2'}
Crawler.define(r'.*_(?P<a>\d)\.txt')

def get_crawlers(root):
    yield Crawler(root)
"""


class TestFormat(object):

    def read(self):
        assert 0

    def close(self):
        assert 0


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


class TestIndexingBase():

    access_module = SIGNAC_ACCESS_MODULE

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        request.addfinalizer(self._tmp_dir.cleanup)

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
            module.write(self.access_module)

    def get_index_collection(self):
        c = Collection()
        return Mock(spec=c, wraps=c)

    def test_base_crawler(self):
        crawler = indexing.BaseCrawler(root=self._tmp_dir.name)
        assert len(list(crawler.crawl())) == 0
        doc = dict(a=0)
        with pytest.raises(errors.FetchError):
            assert crawler.fetch(doc) is None
        assert doc == crawler.process(doc, None, None)
        with pytest.raises(NotImplementedError):
            for doc in crawler.docs_from_file(None, None):
                pass

    def test_regex_file_crawler_pre_compiled(self):
        self.setup_project()

        class Crawler(indexing.RegexFileCrawler):
            pass

        regex = re.compile(r".*a_(?P<a>\d)\.txt")
        Crawler.define(regex, TestFormat)
        crawler = Crawler(root=self._tmp_dir.name)
        no_find = True
        for doc in crawler.crawl():
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            m = regex.match(ffn)
            assert m is not None
            assert os.path.isfile(ffn)
            with open(ffn) as file:
                doc2 = json.load(file)
                assert doc2['a'] == doc['a']
        assert not no_find

    def test_regex_file_crawler(self):
        self.setup_project()

        class Crawler(indexing.RegexFileCrawler):
            pass

        # First test without pattern
        crawler = Crawler(root=self._tmp_dir.name)
        assert len(list(crawler.crawl())) == 0

        # Now with pattern(s)
        pattern = r".*a_(?P<a>\d)\.txt"
        regex = re.compile(pattern)
        Crawler.define(pattern, TestFormat)
        Crawler.define("negativematch", "negativeformat")
        crawler = Crawler(root=self._tmp_dir.name)
        no_find = True
        for doc in crawler.crawl():
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            m = regex.match(ffn)
            assert m is not None
            assert os.path.isfile(ffn)
            with open(ffn) as file:
                doc2 = json.load(file)
                assert doc2['a'] == doc['a']
        assert not no_find
        with pytest.raises(errors.FetchError):
            crawler.fetch(dict())
        with pytest.raises(errors.FetchError):
            crawler.fetch({'filename': 'shouldnotmatch'})

    def test_regex_file_crawler_inheritance(self):
        self.setup_project()

        class CrawlerA(indexing.RegexFileCrawler):
            pass

        class CrawlerB(indexing.RegexFileCrawler):
            pass

        CrawlerA.define('a', TestFormat)
        CrawlerB.define('b', TestFormat)
        assert len(CrawlerA.definitions) == 1
        assert len(CrawlerB.definitions) == 1

        class CrawlerC(CrawlerA):
            pass

        assert len(CrawlerA.definitions) == 1
        assert len(CrawlerC.definitions) == 1
        assert len(CrawlerB.definitions) == 1
        CrawlerC.define('c', TestFormat)
        assert len(CrawlerA.definitions) == 1
        assert len(CrawlerB.definitions) == 1
        assert len(CrawlerC.definitions) == 2

    def test_index_files(self):
        self.setup_project()

        # First test without pattern
        root = self._tmp_dir.name
        assert len(list(signac.index_files(root))) == 5

        # Now with pattern(s)
        pattern_positive = r".*a_(?P<a>\d)\.txt"
        pattern_negative = "nomatch"

        assert len(list(signac.index_files(root, pattern_positive))) == 2
        assert len(list(signac.index_files(root, pattern_negative))) == 0

        no_find = True
        for doc in signac.index_files(root, pattern_positive):
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            assert re.match(r".*a_(?P<a>\d)\.txt", ffn) is not None
            assert os.path.isfile(ffn)
            with open(ffn) as file:
                doc2 = json.load(file)
                assert doc2['a'] == doc['a']
        assert not no_find

    def test_json_crawler(self):
        self.setup_project()
        crawler = indexing.JSONCrawler(root=self._tmp_dir.name)
        docs = list(sorted(crawler.crawl(), key=lambda d: d['a']))
        assert len(docs) == 2
        for i, doc in enumerate(docs):
            assert doc['a'] == i
            assert doc['format'] is None
        ids = set(doc['_id'] for doc in docs)
        assert len(ids) == len(docs)

    def test_master_crawler(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        no_find = True
        for doc in crawler.crawl():
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            assert os.path.isfile(ffn)
            with open(ffn) as file:
                doc2 = json.load(file)
                assert doc2['a'] == doc['a']
            with signac.fetch(doc) as file:
                pass
        assert not no_find

    def test_index(self):
        self.setup_project()
        root = self._tmp_dir.name
        assert len(list(signac.index(root=root))) == 0
        index = signac.index(root=self._tmp_dir.name, tags={'test1'})
        no_find = True
        for doc in index:
            no_find = False
            ffn = os.path.join(doc['root'], doc['filename'])
            assert os.path.isfile(ffn)
            with open(ffn) as file:
                doc2 = json.load(file)
                assert doc2['a'] == doc['a']
            with signac.fetch(doc) as file:
                pass
        assert not no_find

    def test_fetch(self):
        with pytest.raises(ValueError):
            signac.fetch(None)
        with pytest.raises(errors.FetchError):
            signac.fetch(dict())
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        docs = list(crawler.crawl())
        assert len(docs) == 2
        for doc in docs:
            with signac.fetch(doc) as file:
                pass
        for doc, file in indexing.fetched(docs):
            doc2 = json.load(file)
            assert doc['a'] == doc2['a']
            file.close()

    def test_export_one(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = self.get_index_collection()
        for doc in crawler.crawl():
            signac.export_one(doc, index)
        assert index.replace_one.called
        for doc in crawler.crawl():
            assert index.find_one({'_id': doc['_id']}) is not None

    def test_export(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = self.get_index_collection()
        signac.export(crawler.crawl(), index)
        assert index.replace_one.called or index.bulk_write.called
        for doc in crawler.crawl():
            assert index.find_one({'_id': doc['_id']}) is not None

    def test_export_with_update(self):
        self.setup_project()
        index = list(signac.index(root=self._tmp_dir.name, tags={'test1'}))
        collection = self.get_index_collection()
        signac.export(index, collection, update=True)
        assert collection.replace_one.called or collection.bulk_write.called
        for doc in index:
            assert collection.find_one({'_id': doc['_id']}) is not None
        collection.reset_mock()
        assert len(index) == collection.find().count()
        assert collection.find.called
        signac.export(index, collection, update=True)
        assert collection.replace_one.called or collection.bulk_write.called
        for doc in index:
            assert collection.find_one({'_id': doc['_id']}) is not None
        assert len(index) == collection.find().count()
        collection.reset_mock()
        for fn in ('a_0.txt', 'a_1.txt'):
            os.remove(os.path.join(self._tmp_dir.name, fn))
            N = len(index)
            index = list(signac.index(root=self._tmp_dir.name, tags={'test1'}))
            assert len(index) == N - 1
            collection.reset_mock()
            if index:
                signac.export(index, collection, update=True)
                assert collection.replace_one.called or collection.bulk_write.called
                assert len(index) == collection.find().count()
            else:
                with pytest.raises(errors.ExportError):
                    signac.export(index, collection, update=True)

    def test_export_to_mirror(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = self.get_index_collection()
        mirror = TestFS()
        for doc in crawler.crawl():
            assert 'file_id' in doc
            doc.pop('file_id')
            with pytest.raises(errors.ExportError):
                signac.export_to_mirror(doc, mirror)
            break
        for doc in crawler.crawl():
            assert 'file_id' in doc
            signac.export_one(doc, index)
            signac.export_to_mirror(doc, mirror)
        assert index.replace_one.called
        for doc in crawler.crawl():
            assert index.find_one({'_id': doc['_id']}) is not None
            with mirror.get(doc['file_id']):
                pass

    def test_master_crawler_tags(self):
        self.setup_project()
        crawler = indexing.MasterCrawler(root=self._tmp_dir.name)
        assert 0 == len(list(crawler.crawl()))
        crawler.tags = None
        assert 0 == len(list(crawler.crawl()))
        crawler.tags = {}
        assert 0 == len(list(crawler.crawl()))
        crawler.tags = {'nomatch'}
        assert 0 == len(list(crawler.crawl()))
        crawler.tags = {'test1'}
        assert 2 == len(list(crawler.crawl()))
        crawler.tags = {'test2'}
        assert 2 == len(list(crawler.crawl()))
        crawler.tags = {'test1', 'test2'}
        assert 2 == len(list(crawler.crawl()))
        crawler.tags = {'test1', 'non-existent-key'}
        assert 2 == len(list(crawler.crawl()))
        crawler.tags = {'test2', 'non-existent-key'}
        assert 2 == len(list(crawler.crawl()))
        crawler.tags = {'test1', 'test2', 'non-existent-key'}
        assert 2 == len(list(crawler.crawl()))


@pytest.mark.skipif(SKIP_REASON is not None,reason= SKIP_REASON)
class TestIndexingPyMongo(TestIndexingBase):

    def get_index_collection(self):
        db = signac.db.get_database('testing', hostname='testing')
        db.test_index.drop()
        return Mock(spec=db.test_index, wraps=db.test_index)


class TestIndexingBaseGetCrawlers(TestIndexingBase):
    access_module = SIGNAC_ACCESS_MODULE_GET_CRAWLERS


class TestIndexingBaseLegacy(TestIndexingBase):
    access_module = SIGNAC_ACCESS_MODULE_LEGACY

