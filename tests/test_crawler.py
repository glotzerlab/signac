import unittest
import os
import io
import re
import json
import logging

import signac.contrib
from signac.common import six

if six.PY2:
    logging.basicConfig(level=logging.WARNING)
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


SIGNAC_ACCESS_MODULE = """import os
import re

import signac.contrib

RE_TXT = ".*a_(?P<a>\d)\.txt"

class Crawler(signac.contrib.RegexFileCrawler):
    tags = {'test1', 'test2'}

Crawler.define(RE_TXT, signac.contrib.formats.TextFile)

def get_crawlers(root):
    return {'main':  Crawler(os.path.join(root, '.'))}
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

    def __init__(self, _id):
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


class CrawlerBaseTest(unittest.TestCase):

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
        with open(fn('signac_access.py'), 'w') as module:
            module.write(SIGNAC_ACCESS_MODULE)

    def test_regex_file_crawler_pre_compiled(self):
        self.setup_project()

        crawler = signac.contrib.RegexFileCrawler(root=self._tmp_dir.name)
        regex = re.compile(".*a_(?P<a>\d)\.txt")
        crawler.define(regex, TestFormat)
        no_find = True
        for doc_id, doc in crawler.crawl():
            no_find = False
            self.assertEqual(doc_id, doc['_id'])
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

        crawler = signac.contrib.RegexFileCrawler(root=self._tmp_dir.name)
        pattern = ".*a_(?P<a>\d)\.txt"
        regex = re.compile(pattern)
        crawler.define(pattern, TestFormat)
        no_find = True
        for doc_id, doc in crawler.crawl():
            no_find = False
            self.assertEqual(doc_id, doc['_id'])
            ffn = os.path.join(doc['root'], doc['filename'])
            m = regex.match(ffn)
            self.assertIsNotNone(m)
            self.assertTrue(os.path.isfile(ffn))
            with open(ffn) as file:
                doc2 = json.load(file)
                self.assertEqual(doc2['a'], doc['a'])
        self.assertFalse(no_find)

    def test_master_crawler(self):
        self.setup_project()
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        no_find = True
        for doc_id, doc in crawler.crawl():
            no_find = False
            self.assertEqual(doc_id, doc['_id'])
            ffn = os.path.join(doc['root'], doc['filename'])
            self.assertTrue(os.path.isfile(ffn))
            with open(ffn) as file:
                doc2 = json.load(file)
                self.assertEqual(doc2['a'], doc['a'])
            no_files = True
            for file in signac.contrib.fetch(doc):
                no_files = False
                self.assertEqual(signac.contrib.formats.TextFile, type(file))
                file.close()
            self.assertFalse(no_files)
        self.assertFalse(no_find)

    def test_master_crawler_tags(self):
        self.setup_project()
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
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

    def test_custom_mirror(self):
        self.setup_project()
        fs_write = TestFS('custom_filesystem')
        fs_read = TestFS('custom_filesystem')
        fs_bad = TestFS('bad')
        crawler = signac.contrib.MasterCrawler(
            root=self._tmp_dir.name,
            link_local=False,
            mirrors=(fs_write,))
        crawler.tags = {'test1'}
        index = {_id: doc for _id, doc in crawler.crawl()}
        self.assertEqual(len(index), 2)
        check = list()
        for _id, doc in index.items():
            for file in signac.contrib.fetch(
                    doc, sources=(fs_read, ), ignore_linked_mirrors=True):
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
        self.assertEqual(len(check), 2)
        check = list()
        for _id, doc in index.items():
            for file in signac.contrib.fetch(
                    doc, sources=(fs_bad, fs_read), ignore_linked_mirrors=True):
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
        self.assertEqual(len(check), 2)
        for _id, doc in index.items():
            with self.assertRaises(IOError):
                signac.contrib.fetch_one(
                    doc, sources=[], ignore_linked_mirrors=True)
        for _id, doc in index.items():
            with self.assertRaises(IOError):
                signac.contrib.fetch_one(
                    doc, sources=(fs_bad,), ignore_linked_mirrors=True)

    def test_local_filesystem(self):
        self.setup_project()
        fs_root = os.path.join(self._tmp_dir.name, 'local')
        fs_test = signac.contrib.filesystems.LocalFS(fs_root)
        with fs_test.new_file(_id='test123') as file:
            if six.PY2:
                file.write('testfilewrite')
            else:
                file.write('testfilewrite'.encode())
        with self.assertRaises(fs_test.FileExistsError):
            fs_test.new_file(_id='test123')
        with fs_test.get('test123') as file:
            self.assertEqual(file.read(), 'testfilewrite')
        with self.assertRaises(fs_test.FileNotFoundError):
            fs_test.get('badid')
        fs_bad = signac.contrib.filesystems.LocalFS('/bad/path')
        crawler = signac.contrib.MasterCrawler(
            root=self._tmp_dir.name,
            link_local=False,
            mirrors=({'localfs': {'root': fs_root}},))
        crawler.tags = {'test1'}
        index = {_id: doc for _id, doc in crawler.crawl()}
        check = list()
        for _id, doc in index.items():
            for file in signac.contrib.fetch(doc, sources=(fs_test,)):
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for _id, doc in index.items():
            for file in signac.contrib.fetch(
                    doc, sources=({'localfs': {'root': fs_root}},)):
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for _id, doc in index.items():
            for file in signac.contrib.fetch(
                    doc, sources=[dict(localfs=fs_root)]):
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for _id, doc in index.items():
            for file in signac.contrib.fetch(doc, sources=[]):
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for _id, doc in index.items():
            with self.assertRaises(IOError):
                signac.contrib.fetch_one(doc, ignore_linked_mirrors=True)
        for _id, doc in index.items():
            with self.assertRaises(IOError):
                signac.contrib.fetch_one(
                    doc, sources=(fs_bad,), ignore_linked_mirrors=True)


if __name__ == '__main__':
    unittest.main()
