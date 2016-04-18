# examine.py
import signac

project = signac.get_project()

print('p    V')
for job in project.find_jobs():
    p = job.statepoint()['p']
    V = job.document.get('V')
    print('{:04.1f} {}'.format(p, V))
