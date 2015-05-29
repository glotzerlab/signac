#!/usr/bin/env hoomd
# -*- coding: utf-8 -*-

import logging
from compdb.contrib import get_project

# This list defines the parameter space.
def get_state_points(debug = False):
    import numpy as np
    densities = [int(d*100) for d in np.linspace(0.18, 0.85, 5)]
    temperatures = np.linspace(0.5, 2.0, 3)
    for temperature in temperatures:
        for density in densities:
            yield{
                    'N':            2000,
                    'density':      density,
                    'T':            temperature,
                    'tau':          0.5,
                    'sigma':        1.0,
                    'epsilon':      1.0,
                    'r_cut':        3.0,
                    'num_steps':    int(1e5),
                    'random_seed':  1323124,
                }
state_points = list(get_state_points(debug = True))

# The code to be executed for each state point.
def run_job(state_point):
    project = get_project()
    with project.open_job(state_point) as job:
        job.storage.restore_files()
        from lj import simulate
        max_walltime = 10 * 60
        simulate(max_walltime = max_walltime, ** job.parameters())
        job.storage.store_files()
        job.document['num_steps_completed'] = job.parameters()['num_steps']

def main():
    for state_point in state_points:
        run_job(state_point)

if __name__ == '__main__':
    logging.basicConfig(level = logging.WARNING)
    main()
