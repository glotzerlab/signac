# Copyright (c) 2022 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implement class for indexing signac Projects."""

import json
import logging
import operator
import re
from math import isclose
from numbers import Number

from ._utility import _nested_dicts_to_dotted_keys, _to_hashable
from .errors import InvalidKeyError

logger = logging.getLogger(__name__)

_PRIMARY_KEY = "_id"

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


class _DictPlaceholder:
    pass


class _float(float):
    # Numerical objects of either integer or float type, that share the same numerical value,
    # but not the same type, are distinguished within a search index, but considered equal within
    # Python. We manipulate the hash value, to enable the storage of both an int and a float
    # that share the same numerical value within a search index (dict).

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
        default : object, optional
            Default value if type of key is not float (Default value = None).

        Returns
        -------
        The value for given key.

        """
        return dict.get(self, _float(key) if type(key) is float else key, default)


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
        For better understanding have a look at :meth:`~_SearchIndexer.find`.

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
                err_msg = (
                    "The argument of the $near operator must be a float or a list of floats with "
                    "length 1, 2, or 3."
                )
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


class _SearchIndexer(dict):
    """A searchable index of dicts.

    The _SearchIndexer class is a :class:`dict` that maps from ids to
    :class:`dict`s. The :class:`dict`s stored as values can be searched by
    their contained keys and values, returning ids for the values matching the
    provided query. The query syntax is based on MongoDB, though this class
    does not aim to match the API of MongoDB's Collection class.

    The dictionary values may be nested (may contain other dicts or lists), but
    have two restrictions. First, the data must be JSON-encodable. Second, the
    keys in the dictionary may not contain dots (``.``).

    For example, suppose we are given dictionaries of member data containing a
    `name` key and an `age` key along with unique identifiers acting as a
    primary key for each member. We can find the name of all members that are
    age 32 like this:

    .. code-block:: python

        members = _SearchIndexer({
            '0': {'name': 'John',  'age': 32},
            '1': {'name': 'Alice', 'age': 28},
            '2': {'name': 'Kevin', 'age': 32},
            # ...
        })

        for member_id in members.find({'age': 32}):
            print(member_id)  # prints 0 and 2

    Because this class inherits from :class:`dict`, it can be constructed in
    any of the same ways as a :class:`dict`, like ``_SearchIndexer(**kwargs)``,
    ``_SearchIndexer(mapping, **kwargs)``, or
    ``_SearchIndexer(iterable, **kwargs)``.

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
            The dict contains invalid keys.

        """
        logger.debug(f"Building index for key '{key}'...")
        nodes = key.split(".")
        index = _TypedSetDefaultDict()

        for _id, doc in self.items():
            try:
                v = doc
                # Recursively access nested values from dotted keys.
                for n in nodes:
                    v = v[n]
            except (KeyError, TypeError):
                pass
            else:
                # `isinstance(instance, cls)` is typically faster than `type(instance) is cls`
                # when the answer is True, but it is slower when it is False. Since we
                # expect lists and dicts to occur infrequently here, we optimize for the
                # False path using the `type` based check.
                if type(v) is list:
                    index[_to_hashable(v)].add(_id)
                elif type(v) is dict:
                    index[_DictPlaceholder].add(_id)
                else:
                    index[v].add(_id)

            # Raise an exception if the original key is present and has dots.
            if len(nodes) > 1 and key in doc:
                raise InvalidKeyError(
                    "Keys with dots ('.') are invalid.\n\n"
                    "See https://signac.io/document-wide-migration/ "
                    "for a recipe on how to replace dots in existing keys."
                )
        logger.debug(f"Built index for key '{key}'.")
        return index

    def _find_expression(self, key, value):
        """Find ids of dicts with keys matching a value expression.

        Parameters
        ----------
        key : str
            The dict key to match.
        value
            The value expression to match.

        Returns
        -------
        set
            The ids of dicts matching the value expression.

        Raises
        ------
        KeyError
            An invalid operator was given.
        ValueError
            The value is not bool for '$exists' operator or not a
            supported type for '$type' operator.

        """
        logger.debug(f"Find ids matching expression '{key}: {value}'.")
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
        """Find ids of dicts matching a dict of filter expressions.

        Parameters
        ----------
        expr : dict
            The filter of expressions to match.

        Returns
        -------
        set
            A set of ids of dicts that match the given filter.

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

        return result_ids

    def find(self, filter_=None):
        """Find ids of dicts matching a dict of filter expressions.

        This function normalizes the filter argument and then attempts to
        build a set of ids matching the given key-value queries.
        For each key that is queried, an internal index is built and then
        searched.

        The results are a set of ids, where each id is the value of the
        primary key of a dict that matches the given filter.

        The find() method uses the following optimizations:

            1. If the filter is None, a set of all ids is returned.
            2. The filter is processed key by key. Once the set of matches is
               empty it is immediately returned.

        Parameters
        ----------
        filter_ : dict, optional
            The filter of expressions to match (Default value = None).

        Returns
        -------
        set
            A set of ids of dicts that match the given filter.

        Raises
        ------
        ValueError
            When the filter argument is invalid.

        """
        if not filter_:
            return set(self)

        filter_ = json.loads(json.dumps(filter_))  # Normalize
        if not isinstance(filter_, dict):
            raise ValueError(f"Invalid filter: {filter_}")
        return self._find_result(filter_)
