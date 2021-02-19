# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements the :class:`AttrDict`.

This simple mixin class implements overloads for __setattr__, __getattr__, and
__delattr__.  While we do not want to offer this API generally for all
SyncedDict objects, some applications may want to add this feature, so this
simple mixin can be combined via inheritance without causing much difficulty.
"""

from typing import FrozenSet


class AttrDict:
    """A class that redirects attribute access methods to __getitem__.

    Although this class is called an :class:`AttrDict`, it does not directly
    inherit from any dict-like class or offer any relevant APIs. Its only purpose
    is to be used as a mixin with other dict-like classes to add attribute-based
    access to dictionary contents.

    Subclasses that inherit from this class must define the ``_PROTECTED_KEYS``
    class variable, which indicates known attributes of the object. This indication
    is necessary because otherwise accessing ``obj.data`` is ambiguous as to
    whether it is a reference to a special ``data`` attribute or whether it is
    equivalent to ``obj['data']``. Without this variable, a user could mask
    internal variables inaccessible via normal attribute access by adding dictionary
    keys with the same name.

    Examples
    --------
    >>> assert dictionary['foo'] == dictionary.foo

    """

    _PROTECTED_KEYS: FrozenSet[str] = frozenset()

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(f"{type(self)} has no attribute '{name}'")
        try:
            return self.__getitem__(name)
        except KeyError as e:
            raise AttributeError(e)

    def __setattr__(self, key, value):
        # This logic assumes that __setitem__ will not be called until after
        # the object has been fully instantiated. We may want to add a try
        # except in the else clause in case someone subclasses these and tries
        # to use d['foo'] inside a constructor prior to _data being defined.
        # The order of these checks assumes that setting protected keys will be
        # much more common than setting dunder attributes.
        if key in self._PROTECTED_KEYS or key.startswith("__"):
            super().__setattr__(key, value)
        else:
            self.__setitem__(key, value)

    def __delattr__(self, key):
        # The order of these checks assumes that deleting protected keys will be
        # much more common than deleting dunder attributes.
        if key in self._PROTECTED_KEYS or key.startswith("__"):
            super().__delattr__(key)
        else:
            self.__delitem__(key)
