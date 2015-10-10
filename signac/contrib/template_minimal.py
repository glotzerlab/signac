MINIMAL = {
    # job.py
    'job.py': """
import logging

from signac.contrib import get_project

# This list defines the parameter space.
state_points = []

# The code to be executed for each state point.
def run_job(state_point):
    project = get_project()
    with project.open_job(state_point) as job:
        # Insert code
        pass

def main():
    for state_point in state_points:
        run_job(state_point)

if __name__ == '__main__':
    logging.basicConfig(level = logging.WARNING)
    main()""",
    # submit.py
    'submit.py': """
import logging
import argparse

from signac.contrib import get_project

from job import run_job, state_points

def main(args):
    project = get_project()
    for state_point in state_points:
        project.job_queue.submit(run_job, state_point)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description = "Submit jobs to the project job queue.")
    args = parser.parse_args()
    logging.basicConfig(level = logging.INFO)
    main(args)""",
    # analyze.py
    'analyze.py': """
import logging

from signac.contrib import get_project

def main():
    project = get_project()
    docs = project.find()
    for doc in docs:
        print(doc)

if __name__ == '__main__':
    logging.basicConfig(level = logging.WARNING)
    main()"""
}
