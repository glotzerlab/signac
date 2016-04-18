# minimal.py
import signac
project = signac.get_project()
for p in 0.1, 1.0, 10.0:
    sp = {'p': p, 'T': 1.0, 'N': 1000}
    with project.open_job(sp) as job:
        if 'V' not in job.document:
            job.document['V'] = sp['N'] * sp['T'] / sp['p']
