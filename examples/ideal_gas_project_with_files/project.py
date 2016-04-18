# project.py
import signac

def classify(job):
    yield 'init'
    if job.isfile('V.txt'):
        yield 'volume-computed'

def next_operation(job):
    if 'volume-computed' not in classify(job):
        return 'compute_volume'

if __name__ == '__main__':
    project = signac.get_project()
    print(project)

    for job in project.find_jobs():
        labels = ','.join(classify(job))
        p = '{:04.1f}'.format(job.statepoint()['p'])
        print(job, p, labels)
