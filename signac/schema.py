# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Project Schema."""

from collections.abc import Mapping
from numbers import Number
from pprint import pformat

from ._search_indexer import _DictPlaceholder
from ._utility import _nested_dicts_to_dotted_keys


class _Vividict(dict):
    """A dict that returns an empty _Vividict for keys that are missing.

    Useful for automatically nesting keys with ``vividict['a']['b']['c'] = 'd'``.

    """

    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


def _strip_prefix(key):
    return key[len("sp.") :]


def _build_job_statepoint_index(exclude_const, index):
    """Build index for job state points.

    Parameters
    ----------
    exclude_const : bool
        Excludes state point keys whose values are constant across the index.
    index : _SearchIndexer
        A _SearchIndexer mapping from job ids to job state points.

    Yields
    ------
    statepoint_key : str
        State point key.
    statepoint_values : dict
        Dictionary mapping from a state point value to the set of job ids with
        that state point value.

    """
    indexes = {}
    for _id in index.find():
        doc = index[_id]
        for key, _ in _nested_dicts_to_dotted_keys(doc):
            if key.split(".")[0] == "sp":
                indexes[key] = index.build_index(key)

    for key in sorted(indexes, key=lambda key: (len(indexes[key]), key)):
        if (
            exclude_const
            and len(indexes[key]) == 1
            and len(indexes[key][next(indexes[key].keys())]) == len(index)
        ):
            continue
        statepoint_key = _strip_prefix(key)
        # Remove _DictPlaceholder keys from the index
        statepoint_values = indexes[key]
        statepoint_values.pop(_DictPlaceholder, None)
        yield statepoint_key, statepoint_values


class ProjectSchema(Mapping):
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
                    ret.append(f" '{key}': '{_fmt_values(values)}',")
            ret.append("}")
            return "\n".join(ret)

    def __len__(self):
        return len(self._schema)

    def __str__(self):
        return self.format()

    def __repr__(self):
        return f"{type(self).__name__}(<len={len(self)}>)"

    def _repr_html_(self):
        import html

        output = "<strong>" + html.escape(repr(self)) + "</strong>"
        output += "<pre>" + str(self) + "</pre>"
        return output

    def __getitem__(self, key_or_keys):
        return self._schema[key_or_keys]

    def __iter__(self):
        return iter(self._schema)

    def difference(self, other, ignore_values=False):
        """Determine the difference between this and another project schema.

        Parameters
        ----------
        ignore_values : bool, optional
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
