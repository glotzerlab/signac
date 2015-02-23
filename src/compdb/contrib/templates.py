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
            #if job.milestones.reached("MY_MILESTONE")
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
