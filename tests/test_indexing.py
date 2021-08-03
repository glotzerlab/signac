# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import json
import os
import re
from tempfile import TemporaryDirectory
from unittest.mock import Mock

import pytest

from signac import Collection
from signac.contrib import indexing
from signac.errors import FetchError


class TestFormat:
    def read(self):
        assert 0

    def close(self):
        assert 0


class TestIndexingBase:
    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self._tmp_dir = TemporaryDirectory(prefix="signac_")
        request.addfinalizer(self._tmp_dir.cleanup)

    def setup_project(self):
        def fn(name):
            return os.path.join(self._tmp_dir.name, name)

        with open(fn("a_0.txt"), "w") as file:
            file.write('{"a": 0}')
        with open(fn("a_1.txt"), "w") as file:
            file.write('{"a": 1}')
        with open(fn("a_0.json"), "w") as file:
            json.dump(dict(a=0), file)
        with open(fn("a_1.json"), "w") as file:
            json.dump(dict(a=1), file)

    def get_index_collection(self):
        c = Collection()
        return Mock(spec=c, wraps=c)

    def test_base_crawler(self):
        crawler = indexing._BaseCrawler(root=self._tmp_dir.name)
        assert len(list(crawler.crawl())) == 0
        doc = dict(a=0)
        with pytest.raises(FetchError):
            assert crawler.fetch(doc) is None
        assert doc == crawler.process(doc, None, None)
        with pytest.raises(NotImplementedError):
            for doc in crawler.docs_from_file(None, None):
                pass

    def test_regex_file_crawler_pre_compiled(self):
        self.setup_project()

        class Crawler(indexing._RegexFileCrawler):
            pass

        regex = re.compile(r".*a_(?P<a>\d)\.txt")
        Crawler.define(regex, TestFormat)
        crawler = Crawler(root=self._tmp_dir.name)
        no_find = True
        for doc in crawler.crawl():
            no_find = False
            ffn = os.path.join(doc["root"], doc["filename"])
            m = regex.match(ffn)
            assert m is not None
            assert os.path.isfile(ffn)
            with open(ffn) as file:
                doc2 = json.load(file)
                assert doc2["a"] == doc["a"]
        assert not no_find

    def test_regex_file_crawler(self):
        self.setup_project()

        class Crawler(indexing._RegexFileCrawler):
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
            ffn = os.path.join(doc["root"], doc["filename"])
            m = regex.match(ffn)
            assert m is not None
            assert os.path.isfile(ffn)
            with open(ffn) as file:
                doc2 = json.load(file)
                assert doc2["a"] == doc["a"]
        assert not no_find
        with pytest.raises(FetchError):
            crawler.fetch(dict())
        with pytest.raises(FetchError):
            crawler.fetch({"filename": "shouldnotmatch"})

    def test_regex_file_crawler_inheritance(self):
        self.setup_project()

        class CrawlerA(indexing._RegexFileCrawler):
            pass

        class CrawlerB(indexing._RegexFileCrawler):
            pass

        CrawlerA.define("a", TestFormat)
        CrawlerB.define("b", TestFormat)
        assert len(CrawlerA.definitions) == 1
        assert len(CrawlerB.definitions) == 1

        class CrawlerC(CrawlerA):
            pass

        assert len(CrawlerA.definitions) == 1
        assert len(CrawlerC.definitions) == 1
        assert len(CrawlerB.definitions) == 1
        CrawlerC.define("c", TestFormat)
        assert len(CrawlerA.definitions) == 1
        assert len(CrawlerB.definitions) == 1
        assert len(CrawlerC.definitions) == 2
