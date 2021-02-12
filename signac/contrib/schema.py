# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Project Schema."""

import itertools
from collections import defaultdict as ddict
from collections.abc import Mapping
from numbers import Number
from pprint import pformat


class _Vividict(dict):
    """A dict that returns an empty _Vividict for keys that are missing.

    Useful for automatically nesting keys with ``vividict['a']['b']['c'] = 'd'``.

    """

    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


def _collect_by_type(values):
    """Construct a mapping of types to a set of elements drawn from the input values.

    Parameters
    ----------
    values :
        An iterable of values.

    Returns
    -------
    defaultdict(set)
        A mapping of types to a set of input values of that type.

    """
    values_by_type = ddict(set)
    for v in values:
        values_by_type[type(v)].add(v)
    return values_by_type


def _build_job_statepoint_index(exclude_const, index):
    """Build index for job state points.

    Parameters
    ----------
    exclude_const : bool
        Excludes state point keys whose values are constant across the index.
    index :
        An iterable of state points.

    Yields
    ------
    statepoint_key : str
        State point key.
    statepoint_values : dict
        Dictionary mapping from a state point value to the set of job ids
        with that state point value.

    """
    from .collection import Collection, _DictPlaceholder
    from .utility import _nested_dicts_to_dotted_keys

    collection = Collection(index, _trust=True)
    for doc in collection.find():
        for key, _ in _nested_dicts_to_dotted_keys(doc):
            if key == "_id" or key.split(".")[0] != "statepoint":
                continue
            collection.index(key, build=True)
    indexes = collection._indexes

    def strip_prefix(key):
        return key[len("statepoint.") :]

    def remove_dict_placeholder(x):
        """Remove _DictPlaceholder elements from a mapping.

        Parameters
        ----------
        x : dict
            Dictionary from which ``_DictPlaceholder`` values will be removed.

        Returns
        -------
        dict
            Dictionary with ``_DictPlaceholder`` keys removed.

        """
        return {key: value for key, value in x.items() if key is not _DictPlaceholder}

    for key in sorted(indexes, key=lambda key: (len(indexes[key]), key)):
        if (
            exclude_const
            and len(indexes[key]) == 1
            and len(indexes[key][list(indexes[key].keys())[0]]) == len(collection)
        ):
            continue
        statepoint_key = strip_prefix(key)
        statepoint_values = remove_dict_placeholder(indexes[key])
        yield statepoint_key, statepoint_values


class ProjectSchema:
    """A description of a project's state point schema.

    Parameters
    ----------
    schema : dict
        Project schema.

    """

    def __init__(self, schema=None):
        if schema is None:
            schema = {}
        self._schema = schema

    @classmethod
    def detect(cls, statepoint_index):
        """Detect Project's state point schema.

        Parameters
        ----------
        statepoint_index :
            State point index.

        Returns
        -------
        :class:`~ProjectSchema`
            The detected project schema.

        """
        return cls({key: _collect_by_type(value) for key, value in statepoint_index})

    def format(self, depth=None, precision=None, max_num_range=None):
        """Format the schema for printing.

        Parameters
        ----------
        depth : int
            A non-zero value will return a nested formatting up to the specified depth,
            defaults to 0.
        precision : int
            Round numerical values to the given precision, defaults to unlimited precision.
        max_num_range : int
            The maximum number of entries shown for a value range, defaults to 5.

        Returns
        -------
        str
            A formatted representation of the project schema.

        """
        if depth is None:
            depth = 0
        if max_num_range is None:
            max_num_range = 5

        def _fmt_value(x):
            """Convert a value to a string, rounded to a given precision.

            Parameters
            ----------
            x :
                Value to convert to string.

            Returns
            -------
            str
                Formatted value.

            """
            if precision is not None and isinstance(x, Number):
                return str(round(x, precision))
            else:
                return str(x)

        def _fmt_range(type_, values):
            """Convert sequence of values into a comma-separated string.

            Inserts an ellipsis (...) if the number of values exceeds ``max_num_range``.

            Parameters
            ----------
            type_ : type
                Type of values.
            values :
                An iterable of values.

            Returns
            -------
            str
                Comma-separated string of the input values.

            """
            try:
                sorted_values = sorted(values)
            except TypeError:
                sorted_values = sorted(values, key=repr)
            if len(values) <= max_num_range:
                values_string = ", ".join(_fmt_value(value) for value in sorted_values)
            else:
                values_string = ", ".join(
                    _fmt_value(value) for value in sorted_values[: max_num_range - 2]
                )
                values_string += ", ..., "
                values_string += ", ".join(_fmt_value(v) for v in sorted_values[-2:])
            return "{type_name}([{values_string}], {length})".format(
                type_name=type_.__name__,
                values_string=values_string,
                length=len(values),
            )

        def _fmt_values(values):
            """Convert values into a single string.

            Parameters
            ----------
            values :
                An iterable of values.

            Returns
            -------
            str
                Comma-separated string of the input values.

            """
            return ", ".join(_fmt_range(*value) for value in values.items())

        if depth > 0:
            schema_dict = _Vividict()
            for key, values in self._schema.items():
                keys = key.split(".")
                if len(keys) > 1:
                    x = schema_dict[keys[0]]
                    for k in keys[1:-1]:
                        x = x[k]
                    x[keys[-1]] = _fmt_values(values)
                else:
                    schema_dict[keys[0]] = _fmt_values(values)
            return pformat(schema_dict, depth=depth)
        else:
            ret = ["{"]
            for key in sorted(self._schema):
                values = self._schema[key]
                if values:
                    ret.append(" '{}': '{}',".format(key, _fmt_values(values)))
            ret.append("}")
            return "\n".join(ret)

    def __len__(self):
        return len(self._schema)

    def __str__(self):
        return self.format()

    def __repr__(self):
        return "{}(<len={}>)".format(type(self).__name__, len(self))

    def _repr_html_(self):
        import html

        output = "<strong>" + html.escape(repr(self)) + "</strong>"
        output += "<pre>" + str(self) + "</pre>"
        return output

    def __contains__(self, key_or_keys):
        if isinstance(key_or_keys, str):
            return key_or_keys in self._schema
        key_or_keys = ".".join(key_or_keys)
        return key_or_keys in self._schema

    def __getitem__(self, key_or_keys):
        if isinstance(key_or_keys, str):
            return self._schema[key_or_keys]
        return self._schema[".".join(key_or_keys)]

    def __iter__(self):
        return iter(self._schema)

    def keys(self):
        """Return schema keys."""
        return self._schema.keys()

    def values(self):
        """Return schema values."""
        return self._schema.values()

    def items(self):
        """Return schema items."""
        return self._schema.items()

    def __eq__(self, other):
        """Check if two schemas are the same.

        Returns
        -------
        bool
            True if both schemas have the same keys and values.

        """
        return self._schema == other._schema

    def difference(self, other, ignore_values=False):
        """Determine the difference between this and another project schema.

        Parameters
        ----------
        ignore_values : bool
            Ignore if the value (range) of a specific keys differ,
            only return missing keys (Default value = False).
        other : :class:`~ProjectSchema`
            Other project schema.

        Returns
        -------
        set
            A set of key tuples that are either missing or different in the other schema.

        """
        ret = set(self.keys()).difference(other.keys())
        if not ignore_values:
            ret.update(
                {
                    key
                    for key, value in self.items()
                    if key in other and other[key] != value
                }
            )
        return ret

    def __call__(self, jobs_or_statepoints):
        """Evaluate the schema for the given state points.

        Parameters
        ----------
        jobs_or_statepoints :
            An iterable of jobs or state points.

        Returns
        -------
        :class:`~ProjectSchema`
            Schema of the project.

        """
        schema_data = {}
        iterators = itertools.tee(jobs_or_statepoints, len(self))
        for key, it in zip(self, iterators):
            values = []
            tokens = key.split(".")
            for statepoint in it:
                if not isinstance(statepoint, Mapping):
                    # Assumes that a job was provided instead of a state point
                    statepoint = statepoint.statepoint
                value = statepoint[tokens[0]]
                for token in tokens[1:]:
                    value = value[token]
                values.append(value)
            schema_data[key] = _collect_by_type(values)
        return ProjectSchema(schema_data)
