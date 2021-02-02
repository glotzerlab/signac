# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

# The API of the Collection class is adapted from the API of
# the pymongo.Collection class provided as part of the
# mongo-python-driver library [1], which is licensed under the
# Apache License 2.0.
#
# The implementation found here is designed to replicate a subset
# of the behavior of a pymongo.Collection while operating directly
# on files on the local file system instead of a MongoDB database.
#
# [1]: https://github.com/mongodb/mongo-python-driver
"""Collection in signac defined here."""

import argparse
import io
import logging
import operator
import re
import sys
from itertools import islice
from math import isclose
from numbers import Number

from ..core import json
from .filterparse import parse_filter_arg
from .utility import _nested_dicts_to_dotted_keys, _to_hashable

logger = logging.getLogger(__name__)


_INDEX_OPERATORS = (
    "$eq",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$ne",
    "$in",
    "$nin",
    "$regex",
    "$type",
    "$where",
    "$near",
)

_TYPES = {
    "int": int,
    "float": float,
    "bool": bool,
    "str": str,
    "list": tuple,
    "null": type(None),
}


MAX_DEFAULT_ID = int("F" * 32, 16)


def _flatten(container):
    """Yield elements from the container.

    Parameters
    ----------
    container : sequence
        List or tuple of elements.

    Yields
    ------
    Elements of container, recursively flattening sub-sequences.

    """
    for i in container:
        if isinstance(i, (list, tuple)):
            yield from _flatten(i)
        else:
            yield i


class _DictPlaceholder:
    pass


def _valid_filter(f, top=True):
    """Return True if the filter is valid.

    Parameters
    ----------
    f : dict
        The filter argument.
    top : bool
        Used to return True if the filter passed is of type dict.
        (Default value = True)

    Returns
    -------
    bool
        True if filter is valid.

    """
    if f is None:
        return True
    elif type(f) is dict:
        return all(_valid_filter(v, top=False) for v in f.values())
    elif type(f) is list:
        return not top
    else:
        return True


class _float(float):
    # Numerical objects of either integer or float type, that share the same numerical value,
    # but not the same type, are distinguished within a Collection, but considered equal within
    # Python. We manipulate the hash value, to enable the storage of both an int and a float
    # that share the same numerical value within a collection index (dict).

    # There is no risk of accidentally equating ints and floats with different values, since the
    # hash equality is only a necessary, not a sufficient condition for equality.
    def __hash__(self):
        return super().__hash__() + 1


class _TypedSetDefaultDict(dict):
    """Dictionary that is guaranteed to store differently typed values separately.

    This is necessary, because the hash value of integers with float type is identical
    to the same integer as int type, which means they cannot be stored separately in a
    standard dict.

    """

    def keys(self):
        for key in dict.keys(self):
            yield float(key) if type(key) is _float else key

    __iter__ = keys

    def items(self):
        for key, value in dict.items(self):
            yield float(key) if type(key) is _float else key, value

    def __missing__(self, key):
        value = set()
        dict.__setitem__(self, key, value)
        return value

    def __getitem__(self, key):
        return dict.__getitem__(self, _float(key) if type(key) is float else key)

    def __setitem__(self, key, value):
        return dict.__setitem__(self, _float(key) if type(key) is float else key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, _float(key) if type(key) is float else key)

    def get(self, key, default=None):
        """Get the value for given key.

        Parameters
        ----------
        key : str, float
            The key to get the value.
        default :
            Default value if type of key is not float (Default value = None).

        Returns
        -------
        The value for given key.

        """
        return dict.get(self, _float(key) if type(key) is float else key, default)


def _build_index(docs, key, primary_key):
    """Build an index for 'key'; highly performance critical code path.

    Parameters
    ----------
    docs : iterable
        iterable of doc to build index.
    key : str
        The key to build index.
    primary_key : str
        The primary key.

    Returns
    -------
    :class:`~_TypedSetDefaultDict`
        Index for key.

    Raises
    ------
    :class:`~signac.errors.InvalidKeyError`
        The document contains invalid keys.

    """
    nodes = key.split(".")
    index = _TypedSetDefaultDict()

    for doc in docs:
        try:
            v = doc[nodes[0]]
            for n in nodes[1:]:
                v = v[n]
            if type(v) is dict:
                v = _DictPlaceholder
        except (KeyError, TypeError):
            pass
        except Exception as error:
            raise RuntimeError(
                f"An unexpected error occurred while processing doc '{doc}': {error}."
            )
        else:
            # inlined for performance
            if type(v) is dict:
                continue
            elif type(v) is list:  # performance
                index[_to_hashable(v)].add(doc[primary_key])
            else:
                index[v].add(doc[primary_key])

        if len(nodes) > 1:
            try:
                v = doc[".".join(nodes)]
            except KeyError:
                pass
            else:
                from ..errors import InvalidKeyError

                raise InvalidKeyError(
                    "\nThe document contains invalid keys. "
                    "Specifically keys with dots ('.').\n\n"
                    "See https://signac.io/document-wide-migration/ "
                    "for a recipe on how to replace dots in existing keys."
                )
    return index


def _find_with_index_operator(index, op, argument):
    """Find index for given operator and argument.

    Parameters
    ----------
    index : dict
        Index for the operator.
    op : str
        logical operator.
    argument :
        Dependent on the choice of logical operator argument (op).
        For better understanding have a look at :meth:`~Collection.find`.

    Returns
    -------
    set
        Index for given operator and argument.

    Raises
    ------
    ValueError
        When unknown argument is given for $type operator (When the operator is $type).

    """
    if op == "$in":

        def op(value, argument):
            return value in argument

    elif op == "$nin":

        def op(value, argument):
            return value not in argument

    elif op == "$regex":

        def op(value, argument):
            if isinstance(value, str):
                return re.search(argument, value)
            else:
                return False

    elif op == "$type":

        def op(value, argument):
            if argument in _TYPES:
                t = _TYPES[argument]
            else:
                raise ValueError(f"Unknown argument for $type operator: '{argument}'.")
            return isinstance(value, t)

    elif op == "$where":

        def op(value, argument):
            return eval(argument)(value)

    elif op == "$near":
        rel_tol, abs_tol = 1e-9, 0.0  # default values
        if isinstance(argument, (list, tuple)):
            if len(argument) == 1:
                argument = argument[0]
            elif len(argument) == 2:
                argument, rel_tol = argument
            elif len(argument) == 3:
                argument, rel_tol, abs_tol = argument
            else:
                err_msg = "The argument of the $near operator must be a float "
                err_msg += "or a list of floats with length 1, 2, or 3."
                raise ValueError(err_msg)
        argument = float(argument)
        rel_tol = float(rel_tol)
        abs_tol = float(abs_tol)

        def op(value, argument):
            return isclose(value, argument, rel_tol=rel_tol, abs_tol=abs_tol)

    else:
        op = getattr(operator, {"$gte": "$ge", "$lte": "$le"}.get(op, op)[1:])
    matches = set()
    for value in index:
        if op(value, argument):
            matches.update(index[value])
    return matches


def _check_logical_operator_argument(op, argument):
    """Check arguments for the logical-operator.

    Parameters
    ----------
    op : str
        logical-operator.
    argument : list
        list of arguments for logical-operator.

    Raises
    ------
    ValueError
        The argument of logical-operator is not a list or is an empty list.

    """
    if not isinstance(argument, list):
        raise ValueError(f"The argument of logical-operator '{op}' must be a list!")
    if not len(argument):
        raise ValueError(f"The argument of logical-operator '{op}' cannot be empty!")


class _CollectionSearchResults:
    """Iterator for a Collection result vector."""

    def __init__(self, collection, _ids):
        self._collection = collection
        self._ids = _ids

    def __iter__(self):
        return (self._collection[_id] for _id in self._ids)

    def __len__(self):
        return len(self._ids)

    count = __len__


class JSONParseError(ValueError):
    """Error class for JSON Parse."""

    pass


class Collection:
    """A collection of documents.

    The Collection class manages a collection of documents in memory
    or in a file on disk. A document is defined as a dictionary mapping
    of key-value pairs.

    An instance of collection may be used to manage and search documents.
    For example, given a collection with member data, where each document
    contains a `name` entry and an `age` entry, we can find the name of
    all members that are at age 32 like this:

    .. code-block:: python

        members = [
            {'name': 'John',  'age': 32},
            {'name': 'Alice', 'age': 28},
            {'name': 'Kevin', 'age': 32},
            # ...
            ]

        member_collection = Collection(members)
        for doc in member_collection.find({'age': 32}):
            print(doc['name'])

    To iterate over all documents in the collection, use:

    .. code-block:: python

        for doc in collection:
            print(doc)

    By default a collection object will reside in memory. However, it is
    possible to manage a collection associated to a file on disk. To open
    a collection which is associated with a file on disk, use the
    :meth:`Collection.open` class method:

    .. code-block:: python

        with Collection.open('collection.txt') as collection:
            for doc in collection.find({'age': 32}):
                print(doc)

    The collection file is by default opened in `a+` mode, which means it can
    be read from and written to and will be created if it does not exist yet.

    Parameters
    ----------
    docs : iterable
        Initialize the collection with these documents.
    primary_key : str
        The name of the key which serves as the primary
        index of the collection. Selecting documents by primary key has
        time complexity of O(N) in the worst case and O(1) on average.
        All documents must have a primary key value. The default primary
        key is `_id`.
    compresslevel : int
        The level of compression to use. Any positive value
        implies compression and is used by the underlying gzip implementation.
        Default value is 0 (no compression).

    Raises
    ------
    ValueError
        When first argument is a string.

    """

    def __init__(self, docs=None, primary_key="_id", compresslevel=0, _trust=False):
        if isinstance(docs, str):
            raise ValueError(
                "First argument cannot be of str type. "
                "Did you mean to use {}.open()?".format(type(self).__name__)
            )
        self.index_rebuild_threshold = 0.1
        self._primary_key = primary_key
        if compresslevel > 0:
            self._file = io.BytesIO()
        else:
            self._file = io.StringIO()
        self._compresslevel = compresslevel
        self._requires_flush = False
        self._dirty = set()
        self._indexes = {}
        self._next_default_id_ = None
        self._docs = {}
        if docs is not None:
            for doc in docs:
                if self._primary_key not in doc:
                    doc[self._primary_key] = self._next_default_id()
                self.__setitem__(doc[self._primary_key], doc, _trust=_trust)
            self._update_indexes()

    def _assert_open(self):
        if self._docs is None:
            raise RuntimeError(
                "Trying to access closed {}.".format(type(self).__name__)
            )

    def _next_default_id(self):
        """Return next default id.

        Returns
        -------
        _id : str
            Next default id.

        Raises
        ------
        RuntimeError
            When unable to determine default id.

        """
        if self._next_default_id_ is None:
            self._next_default_id_ = len(self)
        for i in range(len(self) + 1):
            assert self._next_default_id_ < MAX_DEFAULT_ID
            _id = str(hex(self._next_default_id_))[2:].rjust(32, "0")
            self._next_default_id_ += 1
            if _id not in self:
                return _id
        raise RuntimeError("Unable to determine default id.")

    def _remove_from_indexes(self, _id):
        """Remove index corresponding to given id.

        Parameters
        ----------
        _id : str
            id to remove from the index.

        """
        for index in self._indexes.values():
            remove_keys = set()
            for key, group in index.items():
                if _id in group:  # faster than exception handling (performance)
                    group.remove(_id)
                if not len(group):
                    remove_keys.add(key)
            for key in remove_keys:
                del index[key]

    def _update_indexes(self):
        """Update the indexes."""
        if self._dirty:
            for _id in self._dirty:
                self._remove_from_indexes(_id)
            docs = [self[_id] for _id in self._dirty]
            for key, index in self._indexes.items():
                tmp = _build_index(docs, key, self._primary_key)
                for v, group in tmp.items():
                    index[v].update(group)
            self._dirty.clear()

    def _build_index(self, key):
        """Build index for given key.

        Parameters
        ----------
        key : str
            The key to build index for.

        """
        logger.debug(f"Building index for key '{key}'...")
        self._indexes[key] = _build_index(self._docs.values(), key, self._primary_key)
        logger.debug(f"Built index for key '{key}'.")

    def index(self, key, build=False):
        """Get (and optionally build) the index for a given key.

        An index allows to access documents by a specific key with
        minimal time complexity, e.g.:

        .. code-block:: python

            age_index = member_collection.index('age')
            for _id in age_index[32]:
                print(member_collection[_id]['name'])

        This means we can access documents by the 'age' key in O(1) time on
        average in addition to the primary key. Using the :meth:`.find`
        method will automatically build all required indexes for the particular
        search.

        Once an index has been built, it will be internally managed by the
        class and updated with subsequent changes. An index returned by this
        method is always current with the latest state of the collection.

        Parameters
        ----------
        key : str
            The primary key of the requested index.
        build : bool
            If True, build a non-existing index if necessary,
            otherwise raise KeyError (Default value = False).

        Returns
        -------
        dict
            Index for the given key.

        Raises
        ------
        KeyError
            In case the build is False and the index has not been built yet or
            no index is present for the key.

        """
        if key == self._primary_key:
            raise KeyError("Can't access index for primary key via index() method.")
        elif key in self._indexes:
            if len(self._dirty) > self.index_rebuild_threshold * len(self):
                logger.debug("Indexes outdated, rebuilding...")
                self._indexes.clear()
                self._build_index(key)
                self._dirty.clear()
            else:
                self._update_indexes()
        else:
            if build:
                self._build_index(key)
            else:
                raise KeyError(f"No index for key '{key}'.")
        return self._indexes[key]

    def __str__(self):
        return "<{} file={}>".format(type(self).__name__, self._file)

    def __iter__(self):
        try:
            return iter(self._docs.values())
        except AttributeError:
            raise RuntimeError(
                "Trying to access closed {}.".format(type(self).__name__)
            )

    @property
    def ids(self):
        """Get an iterator over the primary key in the collection.

        Returns
        -------
        iterable
            iterator over the primary key in the collection.

        """
        self._assert_open()
        return iter(self._docs)

    @property
    def primary_key(self):
        """Get the name of the collection's primary key (default='_id')."""
        return self._primary_key

    def __len__(self):
        self._assert_open()
        return len(self._docs)

    def __contains__(self, _id):
        return _id in self._docs

    def __getitem__(self, _id):
        # The _assert_open() check is only performed after an
        # exception has been caught, which is slightly faster
        # than running the check every time.
        try:
            return self._docs[_id].copy()
        except TypeError:
            self._assert_open()
            raise

    @staticmethod
    def _validate_key(key):
        """Emit a warning or raise an exception if key is invalid. Returns key.

        Parameters
        ----------
        key : str
            Key to validate.

        Returns
        -------
        str
            If key is validated.

        Raises
        ------
        InvalidKeyError
            When key given is invalid.

        """
        if "." in key:
            from ..errors import InvalidKeyError

            raise InvalidKeyError("Keys may not contain dots ('.').")
        return key

    @classmethod
    def _validate_doc(cls, doc):
        """Emit a warning or raise an exception if the document is invalid. Returns doc.

        Parameters
        ----------
        doc : dict
            The document to validate.

        Returns
        -------
        doc : dict
            If doc is validated.

        """
        try:
            for key in doc.keys():
                cls._validate_doc(doc[cls._validate_key(key)])
        except AttributeError:
            return
        return doc

    def __setitem__(self, _id, doc, _trust=False):
        self._assert_open()
        if not isinstance(_id, str):
            raise TypeError("The primary key must be of type str!")
        doc.setdefault(self._primary_key, _id)
        if _id != doc[self._primary_key]:
            raise ValueError("Primary key mismatch!")
        if _trust:
            self._docs[_id] = doc
        else:
            try:
                doc_ = json.loads(json.dumps(doc))
            except TypeError as error:
                raise TypeError(
                    f"Serialization of document '{doc}' failed with error: {error}"
                )
            self._docs[_id] = self._validate_doc(doc_)
        self._dirty.add(_id)
        self._requires_flush = True

    def insert_one(self, doc):
        """Insert one document into the collection.

        If the document does not have a value for the
        collection's primary key yet, it will be assigned one.

        .. code-block:: python

            _id = collection.insert_one(doc)
            assert _id in collection

        .. note::

            The document will be directly updated in case that
            it has no primary key and must therefore be mutable!

        Parameters
        ----------
        doc : dict
            The document to be inserted.

        Returns
        -------
        str
            The _id of the inserted documented.

        """
        self._assert_open()
        if self._primary_key in doc:
            _id = doc[self._primary_key]
        else:
            _id = doc[self._primary_key] = self._next_default_id()
        if _id in self:
            raise KeyError("Primary key collision!")
        self[_id] = doc
        return _id

    def __delitem__(self, _id):
        self._assert_open()
        del self._docs[_id]
        self._remove_from_indexes(_id)
        try:
            self._dirty.remove(_id)
        except KeyError:
            pass
        self._requires_flush = True

    def clear(self):
        """Remove all documents from the collection."""
        self._docs.clear()
        self._indexes.clear()
        self._dirty.clear()
        self._requires_flush = True

    def update(self, docs):
        """Update the collection with these documents.

        Any existing documents with the same primary key
        will be replaced.

        Parameters
        ----------
        docs : iterable
            A sequence of documents to be upserted into the collection.

        """
        for doc in docs:
            if self._primary_key in doc:
                _id = doc[self._primary_key]
            else:
                _id = doc[self._primary_key] = self._next_default_id()
            self[_id] = doc

    def _find_expression(self, key, value):
        """Find document for key value pair.

        Parameters
        ----------
        key : str
            The key for expression-operator.
        value :
            The value for expression-operator.

        Returns
        -------
        set
            The document for key value pair.

        Raises
        ------
        KeyError
            When Bad operator expression/ Bad operator placement or
            the expression-operator is unknown.
        ValueError
            The value is not bool when operator for '$exists' operator.

        """
        logger.debug(f"Find documents for expression '{key}: {value}'.")
        if "$" in key:
            if key.count("$") > 1:
                raise KeyError(f"Bad operator expression '{key}'.")
            nodes = key.split(".")
            op = nodes[-1]
            if not op.startswith("$"):
                raise KeyError(f"Bad operator placement '{key}'.")
            key = ".".join(nodes[:-1])
            if op in _INDEX_OPERATORS:
                index = self.index(key, build=True)
                return _find_with_index_operator(index, op, value)
            elif op == "$exists":
                if not isinstance(value, bool):
                    raise ValueError(
                        "The value of the '$exists' operator must be boolean."
                    )
                index = self.index(key, build=True)
                match = {elem for elems in index.values() for elem in elems}
                return match if value else set(self.ids).difference(match)
            else:
                raise KeyError(f"Unknown expression-operator '{op}'.")
        else:
            index = self.index(key, build=True)
            # Check to see if 'value' is a floating point type but an
            # integer value (e.g., 4.0), and search for both the int and float
            # values. This allows the user to find statepoints that have
            # integer-valued keys that are stored as floating point types.
            # Note that this both cases: 1) user searches for an int and hopes
            # to find values that are stored as integer-valued floats and 2) user
            # searches for a integer-valued float and hopes to find ints.
            # This way, both `signac find x 4.0` and `signac find x 4` would
            # return jobs where `sp.x` is stored as either 4.0 or 4.
            if isinstance(value, Number) and float(value).is_integer():
                result_float = index.get(_float(value), set())
                result_int = index.get(int(value), set())
                return result_int.union(result_float)
            else:
                return index.get(value, set())

    def _find_result(self, expr):
        """Find ids for given expression.

        Parameters
        ----------
        expr : str
            The expression for which to get ids.

        Returns
        -------
        set
            Set of all the ids if the given expression is empty.

        """
        if not len(expr):
            return set(self.ids)  # Empty expression yields all ids...

        result_ids = None

        def reduce_results(match):
            """Reduce the results by intersection of matches.

            Parameters
            ----------
            match : set
                match for the given expression.

            """
            nonlocal result_ids
            if result_ids is None:  # First match
                result_ids = match
            else:  # Update previous match
                result_ids = result_ids.intersection(match)

        # Check if filter contains primary key, in which case we can
        # immediately reduce the result.
        _id = expr.pop(self._primary_key, None)
        if _id is not None and _id in self:
            reduce_results({_id})

        # Extract all logical-operator expressions for now.
        or_expressions = expr.pop("$or", None)
        and_expressions = expr.pop("$and", None)
        not_expression = expr.pop("$not", None)

        # Reduce the result based on the remaining non-logical expression:
        for key, value in _nested_dicts_to_dotted_keys(expr):
            reduce_results(self._find_expression(key, value))
            if not result_ids:  # No match, no need to continue...
                return set()

        # Reduce the result based on the logical-operator expressions:
        if not_expression is not None:
            not_match = self._find_result(not_expression)
            reduce_results(set(self.ids).difference(not_match))

        if and_expressions is not None:
            _check_logical_operator_argument("$and", and_expressions)
            for expr_ in and_expressions:
                reduce_results(self._find_result(expr_))

        if or_expressions is not None:
            _check_logical_operator_argument("$or", or_expressions)
            or_results = set()
            for expr_ in or_expressions:
                or_results.update(self._find_result(expr_))
            reduce_results(or_results)

        assert result_ids is not None
        return result_ids

    def _find(self, filter=None, limit=0):
        """Return a result vector of ids for the given filter and limit.

        This function normalizes the filter argument and then attempts to
        build a result vector for the given key-value queries.
        For each key that is queried, an internal index is built and then
        searched.

        The result vector is a set of ids, where each id is the value of the
        primary key of a document that matches the given filter.

        The _find() method uses the following optimizations:

            1. If the filter is None, the result is directly returned, since
               all documents will match an empty filter.
            2. If the filter argument contains a primary key, the result
               is directly returned since no search operation is necessary.
            3. The filter is processed key by key, once the result vector is
               empty it is immediately returned.

        Parameters
        ----------
        filter : dict
            The filter argument that all documents must match (Default value = None).
        limit : int
            Limit the size of the result vector (Default value = 0).

        Returns
        -------
        set
            A set of ids of documents that match the given filter.

        Raises
        ------
        ValueError
            When the filter argument is invalid.

        """
        self._assert_open()
        if filter:
            filter = json.loads(json.dumps(filter))  # Normalize
            if not _valid_filter(filter):
                raise ValueError(filter)
            result = self._find_result(filter)
            return set(islice(result, limit if limit else None))
        else:
            return set(islice(self._docs.keys(), limit if limit else None))

    def find(self, filter=None, limit=0):
        """Find all documents matching filter, but not more than limit.

        This function searches the collection for all documents that match
        the given filter and returns a result vector. For example:

        .. code-block:: python

            for doc in collection.find(my_filter):
                print(doc)

        Nested values should be searched using the ``.`` operator, for example:

        .. code-block:: python

            docs = collection.find({'nested.value': 42})

        will return documents with a nested structure: ``{'nested': {'value': 42}}``.

        The result of :meth:`~.find` can be stored and iterated over multiple times.
        In addition, the result vector can be queried for its size:

        .. code-block:: python

            docs = collection.find(my_filter)

            print(len(docs))    # the number of documents matching

            for doc in docs:    # iterate over the result vector
                pass

        Arithmetic Operators

                * *$eq*: equal
                * *$ne*: not equal
                * *$gt*: greater than
                * *$gte*: greater or equal than
                * *$lt*: less than
                * *$lte*: less or equal than

            .. code-block:: python

                project.find({"a": {"$lt": 5})

            Matches all docs with *a* less than 5.

        Logical Operators

            That includes *$and* and *$or*; both expect a list of expressions.

                .. code-block:: python

                    project.find({"$or": [{"a": 4}, {"b": {"$gt": 3}}]})

            Matches all docs, where *a* is 4 or *b* is greater than 3.

        Exists operator

            Determines whether a specific key exists, or not, e.g.:

                    .. code-block:: python

                        project.find({"a": {"$exists": True}})

        Array operator

            To determine whether specific elements are in (*$in*), or not in (*$nin*)
            an array, e.g.:

                    .. code-block:: python

                        project.find({"a": {"$in": [0, 1, 2]}})

            Matches all docs, where *a* is either 0, 1, or 2. Usage of *$nin* is equivalent.

        Regular expression operator

            Allows the "on-the-fly" evaluation of regular expressoions, e.g.:

                    .. code-block:: python

                        project.find({"protocol": {"$regex": "foo"}})

            Will match all docs with a protocol that contains the term 'foo'.

        $type operator

            Matches when a value is of specific type, e.g.:

                    .. code-block:: python

                        project.find({"protocol": {"$type": str}})

            Finds all docs, where the value of protocol is of type str.
            Other types that can be checked are: *int*, *float*, *bool*, *list*, and *null*.

        $where operator

            Matches an arbitrary python expression, e.g.:

                    .. code-block:: python

                        project.find({"foo": {"$where": "lambda x: x.startswith('bar')"}})

            Matches all docs, where the value for foo starts with the word 'bar'.

        Parameters
        ----------
        filter : dict
            All documents must match the given filter (Default value = None).
        limit : int
            Do not return more than limit number of documents.
            A limit value of 0 (the default) means no limit.

        Returns
        -------
        :class:`~signac.contrib.collection._CollectionSearchResults`
            A result object that iterates over all matching documents.

        Raises
        ------
        ValueError
            In case that the filter argument is invalid.

        """
        return _CollectionSearchResults(self, self._find(filter, limit=limit))

    def find_one(self, filter=None):
        """Return one document that matches the filter or None.

        .. code-block:: python

            doc = collection.find_one(my_filter)
            if doc is None:
                print("No result found for filter", my_filter)
            else:
                print("Doc matching filter:", my_filter, doc)

        Parameters
        ----------
        filter : dict
            The returned document must match the given filter (Default value = None).

        Returns
        -------
        dict
            A matching document or None.

        Raises
        ------
        ValueError
            In case that the filter argument is invalid.

        """
        for doc in self.find(filter, limit=1):
            return doc

    def replace_one(self, filter, replacement, upsert=False):
        """Replace one document that matches the given filter.

        The first document matching the filter will be replaced
        by the given replacement document. If the `upsert` argument
        is True, the replacement will be inserted in case that
        no document matches the filter.

        Parameters
        ----------
        filter : dict
            A document that should be replaced must match this filter.
        replacement : dict
            The replacement document.
        upsert : bool
            If True, insert the replacement document in
            the case that no document matches the filter (Default value = False).

        Returns
        -------
        str
            The id of the replaced (or upserted) documented.

        Raises
        ------
        ValueError
            In case that the filter argument is invalid.

        """
        self._assert_open()
        if len(filter) == 1 and self._primary_key in filter:
            _id = filter[self._primary_key]
            if upsert or _id in self:
                self[_id] = replacement
                return _id
        else:
            for _id in self._find(filter):
                self[_id] = replacement
                return _id
            else:
                if upsert:
                    return self.insert_one(replacement)

    def delete_many(self, filter):
        """Delete all documents that match the filter.

        Parameters
        ----------
        filter : dict
            A document that should be deleted must match this filter.

        """
        to_delete = set(self._find(filter))
        for _id in to_delete:
            del self[_id]

    def delete_one(self, filter):
        """Delete one document that matches the filter.

        Parameters
        ----------
        filter : dict
            The document that should be deleted must match this filter.

        """
        to_delete = set(self._find(filter, limit=1))
        for _id in to_delete:
            del self[_id]

    def _dump(self, text_buffer):
        """Dump collection content serialized to JSON to text-buffer.

        Parameters
        ----------
        text_buffer :
            The file to write the content serialized to JSON (Default value = sys.stdout).

        """
        for doc in self._docs.values():
            text_buffer.write(json.dumps(doc) + "\n")

    def dump(self, file=sys.stdout):
        """Dump the collection in JSON-encoding to file.

        The file argument defaults to `sys.stdout`, which means
        the encoded blob will be printed to screen in case
        that no file argument is provided.

        For example, to dump to a file on disk, one could write:

        .. code-block:: python

            with open('my_collection.txt', 'w') as file:
                collection.dump(file)

        Parameters
        ----------
        file :
            The file to write the encoded blob to (Default value = sys.stdout).

        """
        self._assert_open()
        if self._compresslevel > 0:
            import gzip

            with gzip.GzipFile(
                compresslevel=self._compresslevel, fileobj=file, mode="wb"
            ) as gzipfile:
                text_io = io.TextIOWrapper(gzipfile, encoding="utf-8")
                self._dump(text_io)
                text_io.flush()
        else:
            self._dump(file)

    def to_json(self, file=None):
        """Dump the collection as a JSON file.

        This function returns the JSON-string directly if the
        file argument is None.

        Parameters
        ----------
        file :
            The filename or a file-like object to write the JSON string to (Default value = None).

        Returns
        -------
        JSON
            JSON-string when file argument is not provided.

        """
        json_string = json.dumps(list(self.find()))
        if file is None:
            return json_string
        elif isinstance(file, str):
            with open(file, "w") as json_file:
                json_file.write(json_string)
        else:
            file.write(json_string)

    @classmethod
    def read_json(cls, file=None):
        """Construct an instance of Collection from a JSON file.

        Parameters
        ----------
        file :
            The json file to read, provided as either a filename or a
            file-like object (Default value = None).

        Returns
        -------
        :class:`~Collection`
            A Collection containing the JSON file

        """
        if isinstance(file, str):
            with open(file) as json_file:
                json_data = json.load(json_file)
        else:
            json_data = json.load(file)
        return Collection(json_data)

    @classmethod
    def _open(cls, file, compresslevel=0):
        """Open a collection associated with a file on disk.

        Parameters
        ----------
        file :
            The file to read the documents from or create the file if it does not exist.
        compresslevel : int
            The level of compression to use. Any positive value
            implies compression and is used by the underlying gzip implementation.
            (Default value = 0)

        Returns
        -------
        :class:`~Collection`
            An instance of :class:`~Collection`.

        """
        try:
            if compresslevel > 0:
                import gzip

                with gzip.GzipFile(fileobj=file, mode="rb") as gzipfile:
                    text_io = io.TextIOWrapper(gzipfile, encoding="utf-8")
                    collection = cls(docs=(json.loads(line) for line in text_io))
                    text_io.detach()
            else:
                collection = cls(docs=(json.loads(line) for line in file))
        except (OSError, io.UnsupportedOperation) as error:
            if str(error) in ("not readable", "read"):
                collection = cls()
            else:
                raise error
        except ValueError as error:
            file.close()
            if hasattr(file, "name"):
                raise JSONParseError(
                    f"Error while trying to parse file '{file.name}': {error}."
                )
            else:
                raise JSONParseError(f"Error while trying to parse '{file}': {error}.")
        except AttributeError as e:
            # This error occurs in python27 and has been evaluated as being
            # fine to accept in this manner
            if str(e) == "'GzipFile' object has no attribute 'extrastart'":
                collection = cls()
            else:
                raise AttributeError(e)
        collection._file = file
        collection._compresslevel = compresslevel
        collection._requires_flush = False  # not needed after initial read
        return collection

    @classmethod
    def open(cls, filename, mode=None, compresslevel=None):
        """Open a collection associated with a file on disk.

        Using this factory method will return a collection that is
        associated with a collection file on disk. For example:

        .. code-block:: python

            with Collection.open('collection.txt') as collection:
                for doc in collection:
                    print(doc)

        will read all documents from the `collection.txt` file or create
        the file if it does not exist yet.

        Modifications to the file will be written to the file when the
        :meth:`~Collection.flush` method is called or the collection is
        explicitly closed by calling the :meth:`Collection.close` method or
        implicitly by leaving the `with`-clause:

        .. code-block:: python

            with Collection.open('collection.txt') as collection:
                collection.update(my_docs)
            # All changes to the collection have been written to collection.txt.

        The open-modes work as expected, so for example to open a collection
        file in *read-only* mode, use ``Collection.open('collection.txt', 'r')``.

        Opening a gzip (`*.gz`) file also works as expected. Because gzip does not
        support a combined read and write mode, `mode=*+` is not available. Be
        sure to open the file in read, write, or append mode as required. Due to
        the manner in which gzip works, opening a file in `mode=wt` will
        effectively erase the current file, so take care using `mode=wt`.

        Parameters
        ----------
        filename : str
            Name of file to read the documents from or create the file if it does not exist.
        mode : str
            Open the file with mode (Default value = None).
        compresslevel : int
            The level of compression to use. Any positive value
            implies compression and is used by the underlying gzip implementation.
            (Default value = None)

        Returns
        -------
        :class:`~Collection`
            An instance of :class:`~Collection`.

        Raises
        ------
        RuntimeError
            File open-mode is not None for in-memory collection or
            compressed collections are not opened in binary mode.

        """
        if compresslevel is None:
            compresslevel = 9 if filename.endswith(".gz") else 0

        logger.debug(f"Open collection '{filename}'.")
        if filename == ":memory:":
            if mode is not None:
                raise RuntimeError(
                    "File open-mode must be None for in-memory collection."
                )
            return cls(compresslevel=compresslevel)  # That's the default open mode.
        else:
            # Set default mode
            if mode is None:
                mode = "ab+"

            file = open(filename, mode)
            file.seek(0)

            if "b" in mode:
                if compresslevel > 0:
                    return cls._open(file, compresslevel=compresslevel)
                else:
                    return cls._open(io.TextIOWrapper(file, encoding="utf-8"))
            elif compresslevel > 0:
                raise RuntimeError(
                    "Compressed collections must be opened in binary mode, for example: 'ab+'."
                )
            else:
                return cls._open(file)

    def flush(self):
        """Write all changes to the associated file.

        If the collection instance is associated with a file-object,
        calling the :meth:`~Collection.flush` method will write all changes
        to this file.

        This method is also called when the collection is explicitly or
        implicitly closed.

        """
        self._assert_open()
        if self._requires_flush:
            if self._file is None:
                logger.debug("Flushed collection.")
            else:
                logger.debug(f"Flush collection to file '{self._file}'.")
                try:
                    self._file.truncate(0)
                except ValueError as error:
                    raise error
                else:
                    self.dump(self._file)
                    self._file.flush()
            self._requires_flush = False
        else:
            logger.debug("Flushed collection (no changes).")

    def close(self):
        """Close this collection instance.

        In case that the collection is associated with a file-object,
        all changes are flushed to the file and the file is closed.

        It is not possible to re-open the same collection instance
        after closing it.

        """
        if self._file is not None:
            try:
                self.flush()
            finally:
                self._file.close()
                self._indexes.clear()
                self._docs = None
                self._file = None

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()

    def main(self):
        """Start a command line interface for this Collection.

        Use this function to interact with this instance of Collection
        on the command line. For example, executing the following script:

        .. code-block:: python

            # find.py
            with Collection.open('my_collection.txt') as c:
                c.main()

        will enable us to search for documents on the command line like this:

        .. code-block:: bash

            $ python find.py '{"age": 32}'
            {"name": "John", "age": 32}
            {"name": "Kevin", "age": 32}

        Raises
        ------
        ValueError
            When both `--id` or `--indent` are selected.

        """
        parser = argparse.ArgumentParser(
            "Command line interface for instances of Collection."
        )
        parser.add_argument(
            "filter",
            nargs="*",
            help="The search filter provided in JSON encoding. "
            "Leave empty to return all documents.",
        )
        parser.add_argument(
            "-l",
            "--limit",
            type=int,
            default=0,
            help="Limit the number of search results that are "
            "maximally returned. A value of 0 (the default) "
            "means no limit.",
        )
        parser.add_argument(
            "--id",
            dest="_id",
            action="store_true",
            help="Print a document's primary key instead of the whole document.",
        )
        parser.add_argument(
            "-i",
            "--indent",
            action="store_true",
            help="Print results in indented format.",
        )
        args = parser.parse_args()
        if args._id and args.indent:
            raise ValueError("Select either `--id` or `--indent`, not both.")
        f = parse_filter_arg(args.filter)
        for doc in self.find(f, limit=args.limit):
            if args._id:
                print(doc[self._primary_key])
            else:
                if args.indent:
                    print(json.dumps(doc, indent=2))
                else:
                    print(json.dumps(doc))
