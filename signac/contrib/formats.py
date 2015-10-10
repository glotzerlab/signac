import logging

from . import conversion

logger = logging.getLogger(__name__)

BASICS = [int, float, str, bool]

class FormatMetaType(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = dict()
        else:
            cls.registry[name] = cls

        super().__init__(name, bases, dct)


class BasicFormat(metaclass=FormatMetaType):
    pass


class LinkMetaType(FormatMetaType):
    """This is the meta class for all link types.

    Do not derive from this class directly, but derive
    from BaseLink.

    This meta class defines the required adapter to convert
    from the link type to the linked type.
    """
    def __init__(cls, name, bases, dct):
        if cls.linked_format is not None:
            # create adapter
            class LinkAdapter(Adapter):
                expects = cls
                returns = cls.linked_format

                def convert(self, x):
                    return x.data


class LinkError(EnvironmentError):
    "Unable to fetch linked resource."
    pass


class BaseLink(metaclass=LinkMetaType):
    """BaseLink allows to create a generic link to an object.

    Derive from this class and implement the fetch method
    to retrieve the data that this link is associated with.
    The linked format will automatically add an adapter to
    allow for automatic conversion. To make this possible
    you need to specify the class attribute `linked_format`.

    The constructor of `linked_format` needs to accept a
    single argument, the return value of `fetch()`.

    .. example::

        class SimpleFileLink(BaseLink):
            def fetch(self):
                return open(self.url, 'rb').read()

        class SimpleTextFileLink(SimpleFileLink):
            linked_format=TextFile
    """
    linked_format = None

    def __init__(self, url):
        if self.linked_format is None:
            raise TypeError(
                "The class attribute linked_format cannot be None!")
        self._url = url

    @property
    def url(self):
        "The url of the linked data object."
        return self._url

    def fetch(self):
        """"Fetch the linked resource.

        Returns: A value which is passed to the linked type's
                 constructor.
        """
        raise NotImplementedError("This is an abstract base class.")

    @property
    def data(self):
        "Return the data of the linked object in the linked format."
        return self.linked_format(self.fetch())

class FileFormat(BasicFormat):

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
