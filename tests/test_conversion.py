import unittest
import tempfile
import uuid
import os

import signac.contrib.formats_network

class ConversionTest(unittest.TestCase):

    def test_get_formats_network(self):
        n = signac.contrib.formats_network.get_formats_network()
        self.assertIsNotNone(n)
        self.assertTrue(len(n) > 0)

    def test_get_conversion_network(self):
        cn = signac.contrib.formats_network.get_conversion_network()
        self.assertIsNotNone(cn)

    def test_convert_with_network(self):
        cn = signac.contrib.formats_network.get_conversion_network()
        self.assertIsNotNone(cn)
        a = '42'
        cn.convert(a, str)
        cn.convert(a, float)
        cn.convert(a, int)
        cn.convert(a, bool)
        b = '42.0'
        cn.convert(b, str)
        cn.convert(b, float)
        cn.convert(b, bool)
        with self.assertRaises(signac.contrib.formats_network.ConversionError):
            cn.convert(b, int)
        class CustomInt(int): pass
        c = CustomInt(42)
        cn.convert(c, float)
        d = 42.0
        cn.convert(d, float)
        class CustomType(object): pass
        e = CustomType()
        with self.assertRaises(signac.contrib.formats_network.NoConversionPathError):
            cn.convert(e, float)
        l = [a, b, c, d]
        l2 = list(cn.converted(l, float))

    def test_fileformat(self):
        testtoken=str(uuid.uuid4())
        class MyFileFormat(signac.contrib.formats.FileFormat):
            pass
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(testtoken.encode())
            tmp.flush()
            tmp.seek(0)
            myfile = MyFileFormat(tmp)
            self.assertEqual(myfile.read().decode(), testtoken)
            tmp.seek(0)
            self.assertEqual(myfile.data.decode(), testtoken)

    def test_link(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        testtoken=str(uuid.uuid4())
        def fn(name):
            return os.path.join(self.tmp_dir.name, name)
        class MyLink(signac.contrib.formats.FileLink): pass
        # MyLink.linked_format must be specified in class definitions
        # for automatic Adapter generation.
        with self.assertRaises(TypeError):
            MyLink('testfile')
        MyLink.linked_format = signac.contrib.formats.TextFile
        MyLink.set_root(self.tmp_dir.name)
        with self.assertRaises(signac.contrib.formats.LinkError):
            MyLink('bs').data
        with open(fn('testfile'), 'w') as file:
            file.write(testtoken)
        link = MyLink('testfile')
        data = link.data
        self.assertEqual(type(data), signac.contrib.formats.TextFile)
        self.assertEqual(data.read().decode(), testtoken)
        # usually not required
        data._file_object.close()

    def test_link_conversion(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        testtoken=str(uuid.uuid4())
        def fn(name):
            return os.path.join(self.tmp_dir.name, name)
        class MyLink(signac.contrib.formats.FileLink):
            linked_format = signac.contrib.formats.TextFile
        MyLink.set_root(self.tmp_dir.name)
        with open(fn('testfile'), 'w') as file:
            file.write(testtoken)
        cn = signac.contrib.formats_network.get_conversion_network()
        link = MyLink('testfile')
        tf = cn.convert(link, signac.contrib.formats.TextFile)
        self.assertEqual(tf.data.decode(), testtoken)
        tf._file_object.close()


if __name__ == '__main__':
    unittest.main()
