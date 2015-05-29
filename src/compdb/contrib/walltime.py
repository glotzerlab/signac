import logging
logger = logging.getLogger(__name__)

STANDARD_TOLERANCE = 0.05

import time

time_start = time.time()
clock_start = time.clock()

class OutOfTimeError(EnvironmentError):
    pass

def walltime():
    return time.time() - time_start

def wallclock():
    return time.clock() - clock_start

def check_walltime(max_walltime, tolerance = None):
    if tolerance is None:
        tolerance = STANDARD_TOLERANCE
    ratio = walltime() / max_walltime
    return ratio < 1.0-tolerance

def exit_by(max_walltime, tolerance = None):
    if not check_walltime(max_walltime, tolerance):
        import sys
        sys.exit(2)

def raise_by(max_walltime, tolerance = None):
    if not check_walltime(max_walltime, tolerance):
        raise OutOfTimeError(walltime())
