# create_flat_view.py
import os
import signac

project = signac.get_project()
variables = project.find_variable_parameters()[0]
for job in project.find_jobs():
    name = '_'.join('{}_{}'.format(p, job.statepoint()[p])
                    for p in variables)
    dst = name + '_V.txt'
    os.symlink(job.fn('V.txt'), name + '_V.txt')
