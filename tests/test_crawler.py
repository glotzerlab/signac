import unittest
import os
import io
import re
import json
import logging

import signac.contrib
from signac.common import six
from signac.common import errors

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
        with open(fn('a_0.json'), 'w') as file:
            json.dump(dict(a=0), file)
        with open(fn('a_1.json'), 'w') as file:
            json.dump(dict(a=1), file)
        with open(fn('signac_access.py'), 'w') as module:
            module.write(SIGNAC_ACCESS_MODULE)

    def test_base_crawler(self):
        crawler = signac.contrib.BaseCrawler(root=self._tmp_dir.name)
        self.assertEqual(len(list(crawler.crawl())), 0)
        doc = dict(a=0)
        for doc in crawler.fetch(doc):
            pass
        self.assertEqual(doc, crawler.process(doc, None, None))
        with self.assertRaises(NotImplementedError):
            crawler.docs_from_file(None, None)

    def test_regex_file_crawler_pre_compiled(self):
        self.setup_project()

        class Crawler(signac.contrib.RegexFileCrawler):
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

        class Crawler(signac.contrib.RegexFileCrawler):
            pass

        # First test without pattern
        crawler = Crawler(root=self._tmp_dir.name)
        self.assertEqual(len(list(crawler.crawl())), 0)

        # Now with pattern
        pattern = ".*a_(?P<a>\d)\.txt"
        regex = re.compile(pattern)
        Crawler.define(pattern, TestFormat)
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

    def test_regex_file_crawler_inheritance(self):
        self.setup_project()

        class CrawlerA(signac.contrib.RegexFileCrawler):
            pass

        class CrawlerB(signac.contrib.RegexFileCrawler):
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
        crawler = signac.contrib.JSONCrawler(root=self._tmp_dir.name)
        docs = list(sorted(crawler.crawl(), key=lambda d: d['a']))
        self.assertEqual(len(docs), 2)
        for i, doc in enumerate(docs):
            self.assertEqual(doc['a'], i)
            self.assertIsNone(doc['format'])
        ids = set(doc['_id'] for doc in docs)
        self.assertEqual(len(ids), len(docs))

    def test_master_crawler(self):
        self.setup_project()
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
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
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        docs = list(crawler.crawl())
        self.assertEqual(len(docs), 2)
        for doc in docs:
            with signac.fetch(doc) as file:
                pass
        for doc, file in signac.contrib.crawler.fetched(docs):
            doc2 = json.load(file)
            self.assertEqual(doc['a'], doc2['a'])
            file.close()

    def test_export_one(self):
        self.setup_project()
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = TestCollection()
        for doc in crawler.crawl():
            signac.export_one(doc, index)
        self.assertTrue(index.called)
        for doc in crawler.crawl():
            self.assertIn(doc['_id'], index)

    def test_export(self):
        self.setup_project()
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = TestCollection()
        signac.export(crawler.crawl(), index)
        self.assertTrue(index.called)
        for doc in crawler.crawl():
            self.assertIn(doc['_id'], index)

    def test_export_to_mirror(self):
        self.setup_project()
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
        crawler.tags = {'test1'}
        index = TestCollection()
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
            self.assertIn(doc['_id'], index)
            with mirror.get(doc['file_id']):
                pass

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
        index = list(crawler.crawl())
        self.assertEqual(len(index), 2)
        check = list()
        for doc in index:
            with signac.fetch(doc, mirrors=(fs_read, )) as file:
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
        self.assertEqual(len(check), 2)
        check = list()
        for doc in index:
            with signac.fetch(doc, mirrors=(fs_read, )) as file:
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
        self.assertEqual(len(check), 2)
        for doc in index:
            with self.assertRaises(IOError):
                signac.fetch(doc, mirrors=(fs_bad,))

    def test_local_filesystem(self):
        self.setup_project()
        fs_root = os.path.join(self._tmp_dir.name, 'local')
        fs_test = signac.fs.LocalFS(fs_root)
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
        fs_bad = signac.fs.LocalFS('/bad/path')
        crawler = signac.contrib.MasterCrawler(
            root=self._tmp_dir.name,
            link_local=False,
            mirrors=({'localfs': {'root': fs_root}},))
        crawler.tags = {'test1'}
        index = list(crawler.crawl())
        check = list()
        for doc in index:
            with signac.fetch(doc, mirrors=(fs_test,)) as file:
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for doc in index:
            with signac.fetch(doc, mirrors=(fs_test,)) as file:
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for doc in index:
            with signac.fetch(doc, mirrors=(fs_test,)) as file:
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for doc in index:
            with signac.fetch(doc, mirrors=(fs_test,)) as file:
                m = json.load(file)
                self.assertTrue('a' in m)
                check.append(m)
                file.close()
        self.assertEqual(len(check), 2)
        check = list()
        for doc in index:
            with self.assertRaises(IOError):
                signac.fetch(doc, mirrors=(fs_bad,))


if __name__ == '__main__':
    unittest.main()
