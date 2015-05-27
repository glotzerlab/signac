from . import conversion

BASICS = [int, float, str, bool]

conversion.make_adapter(int, float)
conversion.make_adapter(float, int)
conversion.make_adapter(int, str)
conversion.make_adapter(str, int)
conversion.make_adapter(float, str)
conversion.make_adapter(str, float)
conversion.make_adapter(int, bool)
conversion.make_adapter(float, bool)

class FileFormat(conversion.BasicFormat):
    def __init__(self, data):
        self._data = data
    @property
    def data(self):
        return self._data

class TextFile(FileFormat):
    pass

class FileToBytesAdapter(conversion.Adapter):
    expects = FileFormat
    returns = bytes
    def convert(self, x):
        return x._data

class BytesToStrAdapter(conversion.Adapter):
    expects = bytes
    returns = str
    def convert(self, x):
        return x.decode()

class StrToBytesAdapter(conversion.Adapter):
    expects = str
    returns = bytes
    def convert(self, x):
        return x.encode()
