# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from pprint import pformat
from collections import defaultdict as ddict
from numbers import Number
import itertools

from ..common import six
if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping


class _Vividict(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


def _collect_by_type(values):
    values_by_type = ddict(set)
    for v in values:
        values_by_type[type(v)].add(v)
    return values_by_type


def _build_job_statepoint_index(jobs, exclude_const, index):
    from .collection import Collection
    from .collection import _traverse_filter
    from .collection import _DictPlaceholder
    collection = Collection(index, _trust=True)
    for doc in collection.find():
        for key, _ in _traverse_filter(doc):
            if key == '_id' or key.split('.')[0] != 'statepoint':
                continue
            collection.index(key, build=True)
    tmp = collection._indexes

    def strip_prefix(key): return k[len('statepoint.'):]

    def remove_dict_placeholder(x):
        return {k: v for k, v in x.items() if k is not _DictPlaceholder}

    for k in sorted(tmp, key=lambda k: (len(tmp[k]), k)):
        if exclude_const and len(tmp[k]) == 1 \
                and len(tmp[k][list(tmp[k].keys())[0]]) == len(collection):
            continue
        yield tuple(strip_prefix(k).split('.')), remove_dict_placeholder(tmp[k])


class ProjectSchema(object):
    "A description of a project's state point schema."

    def __init__(self, schema=None):
        if schema is None:
            schema = dict()
        self._schema = schema

    @classmethod
    def detect(cls, statepoint_index):
        return cls({k: _collect_by_type(v) for k, v in statepoint_index})

    def format(self, depth=None, precision=None, max_num_range=None):
        """Format the schema for printing.

        :param depth:
            A non-zero value will return a nested formatting up to the specified depth,
            defaults to 0.
        :type depth:
            int
        :param precision:
            Round numerical values up the give precision, defaults to unlimited precision.
        :type precision:
            int
        :param max_num_range:
            The maximum number of entries shown for a value range, defaults to 5.
        :type max_num_range:
            int
        :returns:
            A formatted representation of the project schema.
        """
        if depth is None:
            depth = 0
        if max_num_range is None:
            max_num_range = 5

        def _fmt_value(x):
            if precision is not None and isinstance(x, Number):
                return str(round(x, precision))
            else:
                return str(x)

        def _fmt_range(type_, values):
            sorted_values = sorted(values)
            if len(values) <= max_num_range:
                values_string = ', '.join((_fmt_value(v) for v in sorted_values))
            else:
                values_string = ', '.join((_fmt_value(v)
                                           for v in sorted_values[:max_num_range - 2]))
                values_string += ', ..., '
                values_string += ', '.join((_fmt_value(v)
                                            for v in sorted_values[-2:]))
            return '{type_name}([{values_string}], {length})'.format(
                type_name=type_.__name__, values_string=values_string, length=len(values))

        def _fmt_values(values):
            return ', '.join(_fmt_range(*v) for v in values.items())

        if depth > 0:
            schema_dict = _Vividict()
            for key, values in self._schema.items():
                if len(key) > 1:
                    for k in key[:-1]:
                        x = schema_dict[k]
                    x[key[-1]] = _fmt_values(values)
                else:
                    schema_dict[key[0]] = _fmt_values(values)
            return pformat(schema_dict, depth=depth)
        else:
            ret = ['{']
            for key in sorted(self._schema):
                values = self._schema[key]
                if values:
                    ret.append(" '{}': '{}',".format('.'.join(key), _fmt_values(values)))
            ret.append('}')
            return '\n'.join(ret)

    def __len__(self):
        return len(self._schema)

    def __str__(self):
        return self.format()

    def __repr__(self):
        return "{}(<len={}>)".format(type(self).__name__, len(self))

    def __contains__(self, key_or_keys):
        if isinstance(key_or_keys, basestring if six.PY2 else str):  # noqa
            key_or_keys = key_or_keys.split('.')
        return tuple(key_or_keys) in self._schema

    def __getitem__(self, key_or_keys):
        if isinstance(key_or_keys, basestring if six.PY2 else str):  # noqa
            key_or_keys = key_or_keys.split('.')
        return self._schema.__getitem__(tuple(key_or_keys))

    def __iter__(self):
        return iter(self._schema)

    def keys(self):
        return self._schema.keys()

    def values(self):
        return self._schema.values()

    def items(self):
        return self._schema.items()

    def __eq__(self, other):
        return self._schema == other._schema

    def difference(self, other, ignore_values=False):
        """Determine the difference between this and another project schema.

        :param ignore_values:
            Ignore if the value (range) of a specific keys differ, only return missing keys.
        :type ignore_values:
            bool
        :returns:
            A set of key tuples that are either missing or different in the other schema.
        """

        ret = set(self.keys()).difference(other.keys())
        if not ignore_values:
            ret.update({k for k, v in self.items() if k in other and other[k] != v})
        return ret

    def __call__(self, jobs_or_statepoints):
        "Evaluate the schema for the given state points."
        s = dict()
        iterators = itertools.tee(jobs_or_statepoints, len(self))
        for key, it in zip(self, iterators):
            values = []
            for sp in it:
                if not isinstance(sp, Mapping):
                    sp = sp.statepoint
                v = sp[key[0]]
                for k in key[1:]:
                    v = v[k]
                values.append(v)
            s[key] = _collect_by_type(values)
        return ProjectSchema(s)
