# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from .contrib.collection import _traverse_filter


def _dotted_keys_to_nested_dicts(mapping):
    """Converts dictionaries with dot-separated keys into nested dictionaries.

    :param mapping: A mapping with dots in its keys, e.g. {'a.b': 'c'}
    :returns: A mapping with nested keys, e.g. {'a': {'b': 'c'}}
    :rtype: dict
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
    r"""Find differences among a list of jobs' state points.

    The resulting diff is a dictionary where the keys are job ids and the
    values are each job's state point minus the intersection of all provided
    jobs' state points. The comparison is performed over the combined set
    of keys and values.

    Example:

    .. code-block:: python

        >>> import signac
        >>> project = signac.init_project('project_name')
        >>> job1 = project.open_job({'constant': 42, 'diff1': 0, 'diff2': 1}).init()
        >>> job2 = project.open_job({'constant': 42, 'diff1': 1, 'diff2': 1}).init()
        >>> job3 = project.open_job({'constant': 42, 'diff1': 2, 'diff2': 2}).init()
        >>> print(job1)
        c4af2b26f1fd256d70799ad3ce3bdad0
        >>> print(job2)
        b96b21fada698f8934d58359c72755c0
        >>> print(job3)
        e4289419d2b0e57e4852d44a09f167c0
        >>> signac.diff_jobs(job1, job2, job3)
        {'c4af2b26f1fd256d70799ad3ce3bdad0': {'diff2': 1, 'diff1': 0},
        'b96b21fada698f8934d58359c72755c0': {'diff2': 1, 'diff1': 1},
        'e4289419d2b0e57e4852d44a09f167c0': {'diff2': 2, 'diff1': 2}}
        >>> signac.diff_jobs(*project)
        {'c4af2b26f1fd256d70799ad3ce3bdad0': {'diff2': 1, 'diff1': 0},
        'b96b21fada698f8934d58359c72755c0': {'diff2': 1, 'diff1': 1},
        'e4289419d2b0e57e4852d44a09f167c0': {'diff2': 2, 'diff1': 2}}

    :param \*jobs: The jobs whose state points will be diffed.
    :type \*jobs: :py:class:`~.Job`
    :returns: A dictionary where the keys are job ids and values are the unique
        parts of that job's state point.
    :rtype: dict
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

        return diffs
