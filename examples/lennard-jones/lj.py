from hoomd_script import *
import random
from math import pi
from hoomd_util import try_restart, make_write_restart_file_callback

def simulate(max_walltime, N, density, epsilon, sigma, r_cut, T, random_seed, num_steps, tau):
    if init.is_initialized():
        init.reset()

    density /= 100
    r = sigma / 2
    phi_p = 4.0 / 3 * pi * pow(r, 3) * density

    try:
        system = try_restart('restart.xml')
    except FileNotFoundError:
        system = init.create_random(
            N = N, phi_p = phi_p, seed = int(random_seed))
        dump.xml('init.xml', vis = True)

    # simple lennard jones potential
    lj = pair.lj(r_cut=3.0)
    lj.pair_coeff.set('A', 'A', epsilon=epsilon, sigma=sigma)

    # integrate forward in the nvt ensemble
    all = group.all()
    integrate.mode_standard(dt=0.005)
    integrate.nvt(group=all, T = T, tau = tau)

    # Setup thermal analysis log
    analyze.log(
        filename = 'thermal.log',
        quantities = ['time', 'volume', 'temperature', 'pressure', 'potential_energy'],
        period = num_steps * 1e-4)

    dump.xml('init.xml', vis = True)
    # Setup trajectory storage
    dumping_period = max(1, int(num_steps / 10))
    dump.dcd(
        filename = 'trajectory.dcd',
        period = dumping_period)

    # run for 20k steps
    restart_callback = make_write_restart_file_callback('restart.xml')
    def callback(num_steps):
        from signac.contrib import walltime
        restart_callback(num_steps)
        walltime.exit_by(max_walltime)

    restart_period = max(1000, int(num_steps / 10))
    run_upto(num_steps, callback=callback, callback_period=restart_period)
