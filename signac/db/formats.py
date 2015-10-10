import logging

from . import conversion

logger = logging.getLogger(__name__)

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

    def read(self):
        return self._data


class TextFile(FileFormat):
    pass


class FileLink(conversion.BaseLink):
    """Link format for file system files.

    Derive from this class to specify link formats, that are accessible
    through the local file system.

    Attributes:
        root: Specify the root path for this specific link type.
              This allows to create file link formats for specific resources,
              which might be linked into local file systems at different
              locations.

    .. example::

        class HomeFileLink(FileLink):
            from os.path import expanduser
            root=expanduser('~')
    """
    root = ''

    def fetch(self):
        "Load data from file at joined root and url path."
        import os
        fn = os.path.join(self.root, self.url)
        try:
            return open(fn, 'rb').read()
        except FileNotFoundError as error:
            msg = "Unable to open file '{}': {}. root='{}'"
            logger.warning(msg.format(fn, error, self.root))
            raise conversion.LinkError(error)

    @classmethod
    def set_root(cls, path):
        "Change the root path of this file link type."
        cls.root = path


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
