import unittest
import os
import re
import six
import json

import signac.contrib

if six.PY3:
    from tempfile import TemporaryDirectory
else:
    from tempdir import TemporaryDirectory

SIGNAC_ACCESS_MODULE = """import os
import re

import signac.contrib

RE_TXT = re.compile(".*a_(?P<a>\d)\.json")
class Crawler(signac.contrib.RegexFileCrawler):
    pass
Crawler.definitions.update({RE_TXT: signac.contrib.formats.TextFile})

def get_crawlers(root):
    return {'main':  Crawler(os.path.join(root, '.'))}
"""


class CrawlerBaseTest(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory(prefix='signac_')
        self.addCleanup(self._tmp_dir.cleanup)

    def test_regex_file_crawler(self):
        def fn(name):
            return os.path.join(self._tmp_dir.name, name)
        with open(fn('a_0.txt'), 'w') as file:
            file.write('{"a": 0}')
        with open(fn('a_1.txt'), 'w') as file:
            file.write('{"a": 1}')

        class MyType(object):
            pass
        crawler = signac.contrib.RegexFileCrawler(root=self._tmp_dir.name)
        regex = re.compile(".*a_(?P<a>\d)\.txt")
        crawler.definitions.update({regex: MyType})
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
        def fn(name):
            return os.path.join(self._tmp_dir.name, name)
        with open(fn('a_0.json'), 'w') as file:
            file.write('{"a": 0}')
        with open(fn('a_1.json'), 'w') as file:
            file.write('{"a": 1}')
        with open(fn('signac_access.py'), 'w') as module:
            module.write(SIGNAC_ACCESS_MODULE)
        crawler = signac.contrib.MasterCrawler(root=self._tmp_dir.name)
        no_find = True
        for doc_id, doc in crawler.crawl():
            no_find = False
            self.assertEqual(doc_id, doc['_id'])
            ffn = os.path.join(doc['root'], doc['filename'])
            self.assertTrue(os.path.isfile(ffn))
            with open(ffn) as file:
                doc2 = json.load(file)
                self.assertEqual(doc2['a'], doc['a'])
            for data in signac.contrib.fetch(doc):
                self.assertEqual(signac.contrib.formats.TextFile, type(data))
                # prevents resource warning, usually not required
                data._file_object.close()
        self.assertFalse(no_find)


if __name__ == '__main__':
    unittest.main()
