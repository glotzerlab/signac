# examine.py
import os
import signac

def get_volume(job):
    "Return the computed volume for this job."
    with open(job.fn('V.txt')) as file:
        return float(file.read())

project = signac.get_project()

print('p    V')
for job in project.find_jobs():
    p = job.statepoint()['p']
    V = get_volume(job)
    print('{:04.1f} {}'.format(p, V))
