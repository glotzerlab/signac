MINIMAL = {
    'run.py': """from compdb.contrib import get_project

def state_points():
    A = (0, 1, 2,)
    B = (0, 1, 2,)
    for a in A:
        for b in B:
            yield {'a': a, 'b': b}

def main(project):
    for state_point in state_points():
        with project.open_job('JOBNAME', state_point) as job:
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
    # The following commands activate the development mode
    # and clear any data previously created in development mode.
    # This makes it easier to test your jobs without affecting
    # your actual data.
    project = get_project()             # Get a handle on the project.
    project.clear_develop()             # Clear previous data from develop mode.
    project.activate_develop_mode()     # Activate the develop mode.

    main(project)""",

    'concurrent_run.py': """from compdb.contrib import get_project
from multiprocessing import Pool, cpu_count
import run

NUM_PROCESSES = min(4, cpu_count())

# Execute your jobs concurrently.

def main(project):
    N = len(list(run.state_points())) // NUM_PROCESSES + 1
    with Pool(processes = NUM_PROCESSES) as p:
        p.map(run.main, [project for i in range(N)])

if __name__ == '__main__':
    import logging
    logging.basicConfig(level = logging.INFO)
    project = get_project()  # Get a handle on the project.
    project.clear_develop()  # Clear previous data from develop mode.
    project.activate_develop_mode()     # Activate the develop mode.
    main(project)""",
}

EXAMPLE = {
    'run.py': """from compdb.contrib import get_project
from time import sleep

def state_points():
    A = (0, 1, 2,)
    B = (0, 1, 2,)
    for a in A:
        for b in B:
            yield {'a': a, 'b': b}

def main(project):
    for state_point in state_points():
        with project.open_job('JOBNAME', state_point) as job:
            if 'basic' in job.milestones:
                print('skipping')
                continue

            # Execution code here
            p = job.parameters()
            job.document['result'] = p['a'] + p['b']
            sleep(1)

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
    # The following commands activate the development mode
    # and clear any data previously created in development mode.
    # This makes it easier to test your jobs without affecting
    # your actual data.
    project = get_project()             # Get a handle on the project.
    project.clear_develop()             # Clear previous data from develop mode.
    project.activate_develop_mode()     # Activate the develop mode.

    main(project)""",

    'concurrent_run.py': """from compdb.contrib import get_project
from multiprocessing import Pool, cpu_count
import run

NUM_PROCESSES = min(4, cpu_count())

# Execute your jobs concurrently.

def main(project):
    N = len(list(run.state_points())) // NUM_PROCESSES + 1
    with Pool(processes = NUM_PROCESSES) as p:
        p.map(run.main, [project for i in range(N)])

if __name__ == '__main__':
    import logging
    logging.basicConfig(level = logging.INFO)
    project = get_project()  # Get a handle on the project.
    project.clear_develop()  # Clear previous data from develop mode.
    project.activate_develop_mode()     # Activate the develop mode.
    main(project)""",

    'analyze.py': """from compdb.contrib import open_job, get_project

def main():
    # The following commands activate the development mode
    # and clear any data previously created in development mode.
    # This makes it easier to test your jobs without affecting
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

RESTORE_SH = """PROJECT={project}
DATABASE_HOST={db_host}
FILESTORAGE_DIR={fs_dir}
DATABASE_META={db_meta}
JOBS_COLLECTION={compdb_docs}
DOCS_COLLECTION={compdb_job_docs}

mongoimport --host ${{DATABASE_HOST}} -db ${{DATABASE_META}} --collection ${{JOBS_COLLECTION}} compdb_jobs.json
mongoimport --host ${{DATABASE_HOST}} -db ${{PROJECT}} --collection ${{DOCS_COLLECTION}} compdb_docs.json
if [ -d "{sn_storage_dir}" ]; then
    mv {sn_storage_dir}/* ${{FILESTORAGE_DIR}}
fi"""
