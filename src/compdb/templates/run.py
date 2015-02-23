import logging
logging.basicConfig(level = logging.INFO)

def state_points():
    V0 = range(0, 100, 10)
    Beta = range(0, 90, 10)
    for v0 in V0:
        for beta in Beta:
            yield {
                'beta': beta,
                'v0': v0,
                'g': 9.81,
                'dt': 0.1
            }

def calc_trajectory(v0, beta, g, t0, t1, dt):
    from math import cos, sin, pi
    t = t0
    while t < t1:
        yield v0 * t * cos(pi / 180 * beta), v0 * t * sin(pi / 180 * beta) - g/2 * t * t
        t += dt

from compdb.contrib import open_job, get_project
# Get a handle on the project.
project = get_project() 
# Clear any previuos debug data
#project.clear_debug()

# Simulation

for state_point in state_points():
    with open_job('simulate_throw', state_point) as job:
        if job.milestones.reached("1stsimulation"):
            print('skipping')
            continue

        trajectory = list(calc_trajectory(
            job.parameters()['v0'], job.parameters()['beta'], 
            job.parameters()['g'], 0, 1, job.parameters()['dt']))

        # Store the results
        with open('trajectory.cvs', 'wb') as file:
            for x,y in trajectory:
                file.write('{},{}\n'.format(x,y).encode())
        job.storage.store_file('trajectory.cvs')
        # or open the file directly in storage
        # with job.storage.open_file('trajectory.cvs', 'wb') as file:
        # or store the results in the database (max 16MB per entry)
        job.document['trajectory'] = trajectory
        job.milestones.mark('1stsimulation')
