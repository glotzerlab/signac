# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import signac

def diff_jobs(*jobs):
    """Find the difference between jobs or an entire project.

        :param jobs:
    """

    sps = {}
    for job in jobs:
        sps[job] = set(signac.contrib.collection._traverse_filter(job.sp))

    intersection = set.intersection(*sps.values())

    diffs = {}
    for job in jobs:
        unique_sps = sps[job]-intersection
        if len(unique_sps) == 0:
            unique_sps = {}

        diffs[job.get_id()] = unique_sps

    return(diffs)