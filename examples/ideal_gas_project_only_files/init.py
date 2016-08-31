# init.py
import signac
import numpy as np

project = signac.get_project()
#for pressure in 0.1, 1.0, 10.0:
for pressure in np.linspace(0.1, 10.0, 10):
    statepoint = {'p': pressure, 'kT': 1.0, 'N': 1000}
    job = project.open_job(statepoint)
    job.init()
    print(job, 'initialized')
