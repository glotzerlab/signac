import logging
import pickle
import sys
import argparse

from ..core.mongodb_queue import Empty
from ..core.serialization import decode_callable
from . import get_project

logger = logging.getLogger(__name__)

DESCR_RUN = "Fetch a job from the compDB job queue and execute."

RUN_COMMAND = "from signac.contrib.run import execute_job; print(execute_job({BINARY}));"
RUN_EXIT = "import sys; sys.exit({EXIT_CODE});"

def execute_job(binary):
    c, args, kwargs = decode_callable(pickle.loads(binary))
    return c(* args, ** kwargs)

def run_next(project = None, num_jobs = 1, timeout = None, file = sys.stdout, * args, **kwargs):
    if project is None:
        project = get_project()
    for i in range(num_jobs):
        try:
            b = project.job_queue_.get(timeout = timeout)
            project.fetched_set.add(b)
            print(RUN_COMMAND.format(BINARY = pickle.dumps(b)))
        except Empty:
            logger.info("Queue is empty.")
            print(RUN_EXIT.format(EXIT_CODE=1))
            break

def run_next_with_args(args):
    return run_next(num_jobs = args.num_jobs, timeout = args.timeout)

def setup_parser(parser):
    parser.add_argument(
        '-n', '--num-jobs',
        type = int,
        default = 1,
        help = "The number of jobs to fetch and execute.")
    parser.add_argument(
        '-t', '--timeout',
        type = int,
        default = 60,
        help = "Seconds to wait for new jobs before exiting.")
    parser.add_argument(
        '--no-reload',
        action = 'store_true',
        help = "Do not reload modules before execution.")
    parser.set_defaults(func = run_next_with_args)

def main(arguments = None):
    parser = argparse.ArgumentParser(description = DESCR_SERVER)
    setup_parser(parser)
    try:
        from hoomd_script import option
        user_args = option.get_user()
        args = parser.parse_args(user_args)
    except ImportError:
        args = parser.parse_args(arguments)
    return run_next_with_args(args)

if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)
    sys.exit(main())
