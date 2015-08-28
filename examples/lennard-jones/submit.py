#!/usr/bin/env hoomd
# -*- coding: utf-8 -*-

import logging
from signac.contrib import get_project
from job import run_job, state_points

def main(args):
    project = get_project()
    for state_point in state_points:
        project.job_queue.submit(run_job, state_point)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description = "Submit jobs to the project job queue.")
    args = parser.parse_args()
    logging.basicConfig(level = logging.INFO)
    main(args)
