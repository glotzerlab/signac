import logging
logger = logging.getLogger('compdb')

LEGAL_COMMANDS = ['init', 'config', 'snapshot', 'restore', 'cleanup', 'remove_project', 'show']

def show(args):
    from compdb.contrib import get_project
    project = get_project()
    print(project)
    if args.status:
        print(project.config)
    if args.jobs:
        legit_ids = project.find_job_ids()
        if args.jobs is True:
            known_ids = legit_ids
        else:
            job_ids = set(args.jobs.split(','))
            unknown_ids = job_ids.difference(legit_ids)
            known_ids = job_ids.intersection(legit_ids)
            if unknown_ids:
                print("Unknown ids: {}".format(','.join(unknown_ids)))
        if args.status:
            print(project)
            print("Job ID{}".format(' ' * 26), "Open Instances")
        for known in known_ids:
            job = project.get_job(known)
            if args.status:
                print(job, job.num_open_instances())
            else:
                print(job)
            if args.more:
                print(job.spec['parameters'])
    if args.pulse:
        from datetime import datetime
        jobs = list(project.job_pulse())
        if jobs:
            for uid, age in jobs:
                delta = datetime.utcnow() - age
                msg = "UID: {uid}, last signal: {age} seconds"
                print(msg.format(
                    uid = uid, 
                    age = delta.total_seconds()))
        else:
            print("No active jobs found.")

def store_snapshot(args):
    from . import get_project
    from . utility import query_yes_no
    from os.path import exists
    if not args.overwrite and exists(args.snapshot):
        q = "File with name '{}' already exists. Overwrite?"
        if args.yes or query_yes_no(q.format(args.snapshot), 'no'):
            pass
        else:
            return
    project = get_project()
    try:
        if args.database_only:
            print("Creating project database snapshot.")
        else:
            print("Creating project snapshot.")
        project.create_snapshot(args.snapshot, not args.database_only)
    except Exception as error:
        msg = "Failed to create snapshot."
        print(msg)
        raise
    else:
        print("Success.")

def restore_snapshot(args):
    from . import get_project
    project = get_project()
    print("Trying to restore from: {}".format(args.snapshot))
    project.restore_snapshot(args.snapshot)
    print("Success.")

def clean_up(args):
    from . import get_project
    args = parser.parse_args(raw_args)
    project = get_project()
    logger.info("Killing dead jobs...")
    project.kill_dead_jobs(seconds = args.tolerance_time)

def remove(args):
    from . import get_project
    from . utility import query_yes_no
    project = get_project()
    if args.project:
        question = "Are you sure you want to remove project '{}'?"
        if args.yes or query_yes_no(question.format(project.get_id()), default = 'no'):
            try:
                project.remove()
            except RuntimeError as error:
                print("Error during project removal.")
                print("This can be caused by currently executed jobs.")
                print("Try 'compdb clenaup'.")
                if args.yes or query_yes_no("Ignore this warning and remove anywas?", default = 'no'):
                    project.remove(force = True)
    elif args.job:
        job_ids = set(args.job.split(','))
        legit_ids = project.find_job_ids()
        unknown_ids = job_ids.difference(legit_ids)
        known_ids = job_ids.intersection(legit_ids)
        print(known_ids, unknown_ids)
        return
        if len(unknown_ids):
            if not(args.yes or query_yes_no(q)):
                return
        for id_ in known_ids:
            job = project.get_job(id_)
            print(job)
    else:
        print("No selection.")

def main():
    import sys
    from . utility import add_verbosity_argument, set_verbosity_level, EmptyIsTrue
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-y', '--yes',
        action = 'store_true',
        help = "Assume yes to all questions.",)
    add_verbosity_argument(parser)

    subparsers = parser.add_subparsers()

    from compdb.contrib import init_project
    parser_init = subparsers.add_parser('init')
    init_project.setup_parser(parser_init)
    parser_init.set_defaults(func = init_project.init_project)
    
    from compdb.contrib import configure
    parser_config = subparsers.add_parser('config')
    configure.setup_parser(parser_config)
    parser_config.set_defaults(func = configure.configure)

    parser_remove = subparsers.add_parser('remove')
    parser_remove.add_argument(
        '-j', '--job',
        nargs = '*',
        help = "A list of jobs, that are to be removed as a comma separated list of job ids.",
        )
    parser_remove.add_argument(
        '-p', '--project',
        action = 'store_true',
        help = 'Remove the whole project.'
        )
    parser_remove.set_defaults(func = remove)

    parser_snapshot = subparsers.add_parser('snapshot')
    parser_snapshot.add_argument(
        'snapshot',
        type = str,
        help = "Name of the file used to create the snapshot.",)
    parser_snapshot.add_argument(
        '--database-only',
        action = 'store_true',
        help = "Create only a snapshot of the database, without a copy of the value storage.",)
    parser_snapshot.add_argument(
        '--overwrite',
        action = 'store_true',
        help = "Overwrite existing snapshots with the same name without asking.",
        )
    parser_snapshot.set_defaults(func = store_snapshot)

    parser_restore = subparsers.add_parser('restore')
    parser_restore.add_argument(
        'snapshot',
        type = str,
        help = "Name of the snapshot file or directory, used for restoring.",
        )
    parser_restore.set_defaults(func = restore_snapshot)

    parser_cleanup = subparsers.add_parser('cleanup')
    from . job import PULSE_PERIOD
    parser_cleanup.add_argument(
        '-t', '--tolerance-time',
        type = int,
        help = "Tolerated time in seconds since last pulse before a job is declared dead.",
        default = int(5 * PULSE_PERIOD))
    parser_cleanup.set_defaults(func = clean_up)

    parser_show = subparsers.add_parser('show')
    parser_show.add_argument(
        '-j', '--jobs',
        nargs = '?',
        action = EmptyIsTrue,
        help = "Lists the jobs of this project. Provide a comma-separated list to show only a subset.",
        )
    parser_show.add_argument(
        '-s', '--status',
        action = 'store_true',
        help = "Print status information.")
    parser_show.add_argument(
        '-p', '--pulse',
        action = 'store_true',
        help = "Print job pulse status.")
    parser_show.add_argument(
        '-m', '--more',
        action = 'store_true',
        help = "Show more details.")
    parser_show.set_defaults(func = show)
    
    args = parser.parse_args()
    set_verbosity_level(args.verbosity)
    try:
        if 'func' in args:
            args.func(args)
        else:
            parser.print_usage()
    except Exception as error:
        if args.verbosity > 0:
            raise
        else:
            print("Error: {}".format(error))
            print("Use -v to increase verbosity of messages.")
    else:
        sys.exit(0)
