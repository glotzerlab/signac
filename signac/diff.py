# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

def diff_jobs(*jobs):
    sps = {job.get_id(): job.sp() for job in jobs}
    for i in list(sps.values())[0]:
        

