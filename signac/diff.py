# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from .contrib.collection import _traverse_filter


def _dotted_keys_to_nested_dicts(mapping):
    """Converts dictionaries with dot-separated keys into nested dictionaries.

    :param mapping: A mapping with dots in its keys, e.g. {'a.b': 'c'}
    :returns: A mapping with nested keys, e.g. {'a': {'b': 'c'}}
    """
    result = {}

    def make_nested_dict(d, keys):
        item = d
        for key in keys[:-1]:
            if key not in item:
                item[key] = {}
            item = item[key]
        return item

    for dotted_key, value in mapping.items():
        keys = dotted_key.split('.')
        make_nested_dict(result, keys)[keys[-1]] = value

    return result


def diff_jobs(*jobs):
    """Find differences among a list of jobs' state points.

        :param jobs: One or more jobs whose state points will be diffed.
    """
    if len(jobs) == 0:
        return {}

    else:
        sps = {}
        for job in jobs:
            sps[job] = set(_traverse_filter(job.sp()))

        intersection = set.intersection(*sps.values())

        diffs = {}
        for job in jobs:
            unique_sps = sps[job]-intersection
            diffs[job.id] = _dotted_keys_to_nested_dicts(dict(unique_sps))

        return(diffs)
