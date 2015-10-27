from hoomd_script import *
from math import pi

def simulate(N, density, epsilon, sigma, r_cut, T, random_seed, num_steps, tau):
    if init.is_initialized():
        init.reset()

    density /= 100
    r = sigma / 2
    phi_p = 4.0 / 3 * pi * pow(r, 3) * density

    try:
        init.read_xml(filename='init.xml', restart='restart.xml')
    except RuntimeError:
        init.create_random(
            N = N, phi_p = phi_p, seed = int(random_seed))

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
        period = max(1, int(num_steps / 10)))
    dump.xml(
        filename= 'restart.xml',
        period = max(1000, int(num_steps / 10)),
        phase = 0)

    # run for 20k steps
    run_upto(num_steps)
