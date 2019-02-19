# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"Basic wrapper to access multiple different data stores."
import os
import re
import errno
import uuid

from ..common import six


class DictManager(object):

    cls = None
    suffix = None

    __slots__ = ['_prefix', '_dict_registry']

    def __init__(self, prefix):
        assert self.cls is not None
        assert self.suffix is not None
        self._prefix = os.path.abspath(prefix)
        self._dict_registry = dict()

    @property
    def prefix(self):
        return self._prefix

    def __eq__(self, other):
        return os.path.realpath(self.prefix) == os.path.realpath(other.prefix) and \
            self.suffix == other.suffix

    def __repr__(self):
        return "{}(prefix='{}')".format(type(self).__name__, os.path.relpath(self.prefix))

    __str__ = __repr__

    def __getitem__(self, key):
        if key not in self._dict_registry:
            self._dict_registry[key] = self.cls(os.path.join(self.prefix, key) + self.suffix)
        return self._dict_registry[key]

    def __setitem__(self, key, value):
        tmp_key = str(uuid.uuid4())
        try:
            self[tmp_key].update(value)
            if six.PY2:
                os.rename(self[tmp_key].filename, self[key].filename)
            else:
                os.replace(self[tmp_key].filename, self[key].filename)
        except (IOError, OSError) as error:
            if error.errno == errno.ENOENT and not len(value):
                raise ValueError("Cannot asssign empty value!")
            else:
                raise error
        except Exception as error:
            try:
                del self[tmp_key]
            except KeyError:
                pass
            raise error
        else:
            del self._dict_registry[key]

    def __delitem__(self, key):
        try:
            os.unlink(self[key].filename)
        except (IOError, OSError) as error:
            if error.errno == errno.ENOENT:
                raise KeyError(key)
            else:
                raise error

    def __getattr__(self, name):
        try:
            return super(DictManager, self).__getattribute__(name)
        except AttributeError:
            if name.startswith('__') or name in self.__slots__:
                raise
            try:
                return self.__getitem__(name)
            except KeyError:
                raise AttributeError(name)

    def __setattr__(self, name, value):
        if name.startswith('__') or name in self.__slots__:
            super(DictManager, self).__setattr__(name, value)
        else:
            self.__setitem__(name, value)

    def __delattr__(self, name, value):
        if name.startswith('__') or name in self.__slots__:
            super(DictManager, self).__delattr__(name, value)
        else:
            self.__delitem__(name, value)

    def __iter__(self):
        for fn in os.listdir(self.prefix):
            m = re.match('(.*){}'.format(self.suffix), fn)
            if m:
                yield m.groups()[0]

    def keys(self):
        return iter(self)

    def __len__(self):
        return len(list(self.keys()))
