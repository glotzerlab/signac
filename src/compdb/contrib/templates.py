MINIMAL = {
    'run.py': """from compdb.contrib import open_job, get_project

def state_points():
    A = (0, 1, 2,)
    B = (0, 1, 2,)
    for a in A:
        for b in B:
            yield {'a': a, 'b': b}

def main():
    # The following commands activate the development mode
    # and clear any data previously created in development mode.
    # This makes it easiert to test your jobs without affecting
    # your actual data.
    project = get_project()             # Get a handle on the project.
    project.clear_develop()             # Clear previous data from develop mode.
    project.activate_develop_mode()     # Activate the develop mode.

    for state_point in state_points():
        with open_job('JOBNAME', state_point) as job:
            # Uncomment to use milestone for process flow.
            #if 'MY_MILESTONE' in job.milestones:
            #    print('skipping')
            #    continue

            # Execution code here
            pass

            #job.milestones.mark('MY_MILESTONE')

if __name__ == '__main__':
    import logging
    logging.basicConfig(level = logging.INFO)
    main()""",
}
EXAMPLE = {
    'run.py': """from compdb.contrib import open_job, get_project

def state_points():
    A = (0, 1, 2,)
    B = (0, 1, 2,)
    for a in A:
        for b in B:
            yield {'a': a, 'b': b}

def main():
    # The following commands activate the development mode
    # and clear any data previously created in development mode.
    # This makes it easiert to test your jobs without affecting
    # your actual data.
    project = get_project()             # Get a handle on the project.
    project.clear_develop()             # Clear previous data from develop mode.
    project.activate_develop_mode()     # Activate the develop mode.

    for state_point in state_points():
        with open_job('JOBNAME', state_point) as job:
            if 'basic' in job.milestones:
                print('skipping')
                continue

            # Execution code here
            p = job.parameters()
            job.document['result'] = p['a'] + p['b']

            job.milestones.mark('basic')

    # Extend a few jobs
    for job in project.find_jobs({'parameters.a': 0}):
        with job:
            if 'basic' in job.milestones and not 'extended' in job.milestones:
                job.document['result'] += 100
                job.milestones.mark('extended')

if __name__ == '__main__':
    import logging
    logging.basicConfig(level = logging.INFO)
    main()""",
    'analyze.py': """from compdb.contrib import open_job, get_project

def main():
    # The following commands activate the development mode
    # and clear any data previously created in development mode.
    # This makes it easiert to test your jobs without affecting
    # your actual data.
    project = get_project()              # Get a handle on the project.
    #project.clear_develop()             # Clear previous data from develop mode.
    project.activate_develop_mode()     # Activate the develop mode.

    # Search jobs based on their parameters and/or content.
    # job_spec: Refers to the job's meta data.
    # spec:     Refers to the job's content data.
    # Either argument is optional.
    jobs = project.find_jobs(
        job_spec = {
            'parameters.a': 0, 
            'parameters.b': {'$lt': 2}},
        spec = {'result': {'$exists': True}}) 

    for job in jobs:
        with job:
            pass

    # Accessing only database content does not require the opening
    # of jobs. In this case we can query the database directly.
    docs = project.find(
        job_spec = {
            'parameters.a': 0, 
            'parameters.b': {'$lt': 2}},
        spec = {'result': {'$exists': True}}) 
    print(list(docs))

if __name__ == '__main__':
    import logging
    logging.basicConfig(level = logging.INFO)
    main()""",
}

TEMPLATES = {
    'minimal': MINIMAL,
    'example': EXAMPLE,
}
