# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from .contrib.collection import _traverse_filter


def diff_jobs(*jobs):
    """Find differences among a list of jobs' state points.

        :param jobs: One or more jobs whose state points will be diffed.
    """
    if len(jobs) == 0:
        return {}

    else:
        sps = {}
        for job in jobs:
            sps[job] = set(_traverse_filter(job.sp))

        intersection = set.intersection(*sps.values())

        diffs = {}
        for job in jobs:
            unique_sps = sps[job]-intersection
            diffs[job.get_id()] = dict(unique_sps)

        return(diffs)
