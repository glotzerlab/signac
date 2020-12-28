# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define common utilities."""


class AbstractTypeResolver:
    r"""Mapping between recognized types and their abstract parents.

    Synced collections are heavily reliant on checking the types of objects to
    determine the appropriate type of behavior in various scenarios. For maximum
    generality, most of these checks use the ABCs defined in :py:mod:`collections.abc`.
    The price of this flexibility is that `isinstance` checks with these classes
    are very slow because the ``__instancecheck__`` hooks are implemented in pure
    Python and reuqire checking many different cases.

    Rather than attempting to directly optimize this behavior, this class provides
    a workaround by which we can amortize the cost of type checks. Given a set
    of types that must be resolved and a way to identify each of these (which
    may be expensive), it maintains a local cache of all instances of a given
    type that have previously been observed. This allows :math:`\mathcal{O}(1)`
    type checks except for the very first time a given type is seen.

    Parameters
    ----------
    abstract_type_identifiers : collections.abc.Mapping
        A mapping from a string identifier for a group of types (e.g. ``MAPPING``)
        to a callable that can be used to identify that type. Due to insertion order
        guarantees of dictionaries in Python>=3.6 (officially 3.7), it is beneficial
        to order this dictionary with the most frequently occuring types first.

    Attributes
    ----------
    abstract_type_identifiers : Dict[str, Callable[Any, bool]]
        A mapping from string identifiers for an abstract type to callables that
        accepts an object and returns True if the object is of the key type and
        False if not.
    type_map : Dict[Type, str]
        A mapping from concrete types to the corresponding named abstract type
        from :attr:`type_enum`.

    """

    def __init__(self, abstract_type_identifiers):
        self.abstract_type_identifiers = abstract_type_identifiers
        self.type_map = {}

    def get_type(self, obj):
        """Get the type string corresponding to this data type.

        Parameters
        ----------
        obj : Any
            Any object whose type to check

        Returns
        -------
        str
            The name of the type, where valid types are the keys of the dict
            argument to the constructor. If the object's type cannot be identified,
            will return ``None``.

        """
        dtype = type(obj)
        enum_type = None
        try:
            enum_type = self.type_map[dtype]
        except KeyError:
            for adt, id_func in self.abstract_type_identifiers.items():
                if id_func(obj):
                    enum_type = self.type_map[dtype] = adt
                    break

        return enum_type
