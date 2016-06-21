# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging
import errno

from ..common.six import with_metaclass
from .conversion import Adapter

logger = logging.getLogger(__name__)

BASICS = [int, float, str, bool]


class _FormatMetaType(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = dict()
        else:
            cls.registry[name] = cls

        super(_FormatMetaType, cls).__init__(name, bases, dct)


class BasicFormat(with_metaclass(_FormatMetaType)):
    pass


class _LinkMetaType(_FormatMetaType):
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


class BaseLink(with_metaclass(_LinkMetaType)):
    """BaseLink allows to create a generic link to an object.

    Derive from this class and implement the fetch method
    to retrieve the data that this link is associated with.
    The linked format will automatically add an adapter to
    allow for automatic conversion. To make this possible
    you need to specify the class attribute `linked_format`.

    The constructor of `linked_format` needs to accept a
    single argument, the return value of `fetch()`.

    .. code::

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

    def fetch(self):  # pragma no coverage
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

    def __init__(self, file_object):
        self._file_object = file_object

    @property
    def data(self):
        return self.read()

    def read(self, size=-1):
        return self._file_object.read(size)

    def seek(self, offset):
        return self._file_object.seek(offset)

    def tell(self):
        return self._file_object.tell()

    def __iter__(self):
        return iter(self._file_object)

    def close(self):
        return self._file_object.close()


class TextFile(FileFormat):
    pass


class FileLink(BaseLink):
    """Link format for file system files.

    Derive from this class to specify link formats, that are accessible
    through the local file system.

    Attributes:
        root: Specify the root path for this specific link type.
              This allows to create file link formats for specific resources,
              which might be linked into local file systems at different
              locations.

    .. code::

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
            return open(fn, 'rb')
        except IOError as error:
            if not error.errno == errno.ENOENT:
                raise
            msg = "Unable to open file '{}': {}. root='{}'"
            logger.warning(msg.format(fn, error, self.root))
            raise LinkError(error)

    @classmethod
    def set_root(cls, path):
        "Change the root path of this file link type."
        cls.root = path
