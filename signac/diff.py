# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Compute diffs of state points."""

from .contrib.utility import _dotted_dict_to_nested_dicts, _nested_dicts_to_dotted_keys


def diff_jobs(*jobs):
    r"""Find differences among a list of jobs' state points.

    The resulting diff is a dictionary where the keys are job ids and the
    values are each job's state point minus the intersection of all provided
    jobs' state points. The comparison is performed over the combined set of
    keys and values.

    See :ref:`signac diff <signac-cli-diff>` for the command line equivalent.

    Parameters
    ----------
    \*jobs : sequence[:class:`~signac.contrib.job.Job`]
        Sequence of jobs to diff.

    Returns
    -------
    dict
        A dictionary where the keys are job ids and values are the unique parts
        of that job's state point.

    Examples
    --------
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

    """
    if len(jobs) == 0:
        return {}
    else:
        statepoints = {}
        for job in jobs:
            statepoints[job] = set(_nested_dicts_to_dotted_keys(job.statepoint()))

        intersection = set.intersection(*statepoints.values())

        diffs = {}
        for job in jobs:
            unique_statepoints = statepoints[job] - intersection
            diffs[job.id] = _dotted_dict_to_nested_dicts(dict(unique_statepoints))

        return diffs
