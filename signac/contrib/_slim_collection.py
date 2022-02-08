# Copyright (c) 2022 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""An optimized _SlimCollection class for indexing signac Projects."""

import json
import logging
from numbers import Number

from ..errors import InvalidKeyError
from .collection import (
    _INDEX_OPERATORS,
    _check_logical_operator_argument,
    _DictPlaceholder,
    _find_with_index_operator,
    _float,
    _TypedSetDefaultDict,
    _valid_filter,
)
from .utility import _nested_dicts_to_dotted_keys, _to_hashable

logger = logging.getLogger(__name__)

_PRIMARY_KEY = "_id"


class _SlimCollection(dict):
    """A collection of documents, optimized for minimal use cases in signac.

    The _SlimCollection class manages a collection of documents in memory.
    A document is defined as a dictionary mapping of key-value pairs. Each
    document is identified by a unique id. The _SlimCollection class is a
    mapping from ids to documents, inheriting from :py:class:`dict`.

    An instance of _SlimCollection may be used to manage and search documents.
    For example, given a collection with member data, where each document
    contains a `name` entry and an `age` entry, we can find the name of
    all members that are at age 32 like this:

    .. code-block:: python

        members = {
            '0': {'name': 'John',  'age': 32},
            '1': {'name': 'Alice', 'age': 28},
            '2': {'name': 'Kevin', 'age': 32},
            # ...
        }

        member_collection = _SlimCollection(members)
        for doc in member_collection.find({'age': 32}):
            print(doc)  # prints 0 and 2

    Parameters
    ----------
    docs : iterable
        Initialize the collection with these documents.

    """

    def build_index(self, key):
        """Build index for a given key.

        This is a highly performance critical code path.

        Parameters
        ----------
        key : str
            The key on which the index is built.

        Returns
        -------
        :class:`~_TypedSetDefaultDict`
            Index for key.

        Raises
        ------
        :class:`~signac.errors.InvalidKeyError`
            The document contains invalid keys.

        """
        logger.debug(f"Building index for key '{key}'...")
        nodes = key.split(".")
        index = _TypedSetDefaultDict()

        for _id, doc in self.items():
            try:
                v = doc[nodes[0]]
                for n in nodes[1:]:
                    v = v[n]
                if type(v) is dict:
                    v = _DictPlaceholder
            except (KeyError, TypeError):
                pass
            else:
                # inlined for performance
                if type(v) is list:  # avoid isinstance for performance
                    index[_to_hashable(v)].add(_id)
                else:
                    index[v].add(_id)

            if len(nodes) > 1:
                try:
                    v = doc[".".join(nodes)]
                except KeyError:
                    pass
                else:
                    raise InvalidKeyError(
                        "\nThe document contains invalid keys. "
                        "Specifically keys with dots ('.').\n\n"
                        "See https://signac.io/document-wide-migration/ "
                        "for a recipe on how to replace dots in existing keys."
                    )
        logger.debug(f"Built index for key '{key}'.")
        return index

    def _find_expression(self, key, value):
        """Find document for key value pair.

        Parameters
        ----------
        key : str
            The key for expression.
        value :
            The value for expression.

        Returns
        -------
        set
            The document for key value pair.

        Raises
        ------
        KeyError
            An invalid operator was given.
        ValueError
            The value is not bool for '$exists' operator or not a
            supported type for '$type' operator.

        """
        logger.debug(f"Find documents for expression '{key}: {value}'.")
        if "$" in key:
            if key.count("$") > 1:
                raise KeyError(f"Invalid operator expression '{key}'.")
            nodes = key.split(".")
            op = nodes[-1]
            if not op.startswith("$"):
                raise KeyError(f"Invalid operator placement '{key}'.")
            key = ".".join(nodes[:-1])
            if op in _INDEX_OPERATORS:
                index = self.build_index(key)
                return _find_with_index_operator(index, op, value)
            elif op == "$exists":
                if not isinstance(value, bool):
                    raise ValueError(
                        "The value of the '$exists' operator must be boolean."
                    )
                index = self.build_index(key)
                match = {elem for elems in index.values() for elem in elems}
                return match if value else set(self).difference(match)
            else:
                raise KeyError(f"Unknown operator '{op}'.")
        else:
            index = self.build_index(key)
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
        if not expr:
            # Empty expression yields all ids.
            return set(self)

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
        _id = expr.pop(_PRIMARY_KEY, None)
        if _id is not None:
            reduce_results({_id} if _id in self else set())

        # Extract all logical-operator expressions for now.
        or_expressions = expr.pop("$or", None)
        and_expressions = expr.pop("$and", None)
        not_expression = expr.pop("$not", None)

        # Reduce the result based on the remaining non-logical expression:
        for key, value in _nested_dicts_to_dotted_keys(expr):
            reduce_results(self._find_expression(key, value))
            if not result_ids:
                # No matches, so exit early.
                return set()

        # Reduce the result based on the logical-operator expressions:
        if not_expression is not None:
            not_match = self._find_result(not_expression)
            reduce_results(set(self).difference(not_match))
            if not result_ids:
                # No matches, so exit early.
                return set()

        if and_expressions is not None:
            _check_logical_operator_argument("$and", and_expressions)
            for expr_ in and_expressions:
                reduce_results(self._find_result(expr_))
                if not result_ids:
                    # No matches, so exit early.
                    return set()

        if or_expressions is not None:
            _check_logical_operator_argument("$or", or_expressions)
            or_results = set()
            for expr_ in or_expressions:
                or_results.update(self._find_result(expr_))
            reduce_results(or_results)

        assert result_ids is not None
        return result_ids

    def find(self, filter=None):
        """Return a result vector of ids for the given filter and limit.

        This function normalizes the filter argument and then attempts to
        build a result vector for the given key-value queries.
        For each key that is queried, an internal index is built and then
        searched.

        The results are a set of ids, where each id is the value of the
        primary key of a document that matches the given filter.

        The find() method uses the following optimizations:

            1. If the filter is None, the result is directly returned, since
               all documents will match an empty filter.
            2. The filter is processed key by key, once the result vector is
               empty it is immediately returned.

        Parameters
        ----------
        filter : dict
            The filter argument that all documents must match (Default value = None).

        Returns
        -------
        set
            A set of ids of documents that match the given filter.

        Raises
        ------
        ValueError
            When the filter argument is invalid.

        """
        if not filter:
            return set(self)

        filter = json.loads(json.dumps(filter))  # Normalize
        if not _valid_filter(filter):
            raise ValueError(filter)
        return set(self._find_result(filter))
