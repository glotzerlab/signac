# create_flat_view.py
import os
import json

import signac

project = signac.get_project()
statepoint_index = project.build_job_statepoint_index(exclude_const=True)

for key, job_ids in dict(statepoint_index).items():
    assert len(job_ids) == 1
    sp = json.loads(key)
    job = project.open_job(id=job_ids.pop())
    name = '_'.join(str(x) for x in sp)
    dst = name + '_V.txt'
    os.symlink(job.fn('V.txt'), dst)
