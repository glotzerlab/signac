# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Functions for initializing workspaces for testing purposes."""
from itertools import cycle


def init_jobs(project, nested=False, listed=False, heterogeneous=False):
    """Initialize a dataspace for testing purposes

    :param project:
        The project to add the jobs to
    :type project:
        :py:class:`~.project.Project`
    :param nested:
        If True, included nested state points
    :type nested:
        bool
    :param listed:
        If True, include lists as values of state point parameters
    :type listed:
        bool
    :param heterogeneous:
        If True, include heterogeneous state point parameters
    :type heterogeneous:
        bool
    :returns:
        A list containing the initialized jobs
    :rtype:
        list of :py:class:`~.Job`
    """
    jobs_init = []
    vals = [1, 1.0, '1', False, True, None]
    if nested:
        vals += [{'b': v, 'c': 0} if heterogeneous else {'b': v} for v in vals]
    if listed:
        vals += [[v, 0] if heterogeneous else [v] for v in vals]
    if heterogeneous:
        for k, v in zip(cycle('ab'), vals):
            jobs_init.append(project.open_job({k: v}).init())
    else:
        for v in vals:
            jobs_init.append(project.open_job(dict(a=v)).init())
    return jobs_init
