import logging
logger = logging.getLogger('compdb')

def info(args):
    from compdb.contrib import get_project
    project = get_project()
    if args.all:
        args.status = True
        args.jobs = True
        args.pulse = True
        args.more = True

    print(project)
    if args.more:
        print(project.config)
    if args.status:
        print("{} registered job(s)".format(len(list(project.find_job_ids()))))
        print("{} active job(s)".format(len(list(project.active_jobs()))))
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
            print("Job ID{}".format(' ' * 26), "Open Instances")
        for known in known_ids:
            job = project.get_job(known)
            if args.status:
                print(job, job.num_open_instances())
            else:
                print(job)
            if args.more:
                from json import dumps
                print(dumps(job.spec['parameters'], sort_keys = True))
    if args.pulse:
        from datetime import datetime
        from compdb.contrib.job import PULSE_PERIOD
        jobs = list(project.job_pulse())
        if jobs:
            print("Pulse period (expected): {}s.".format(PULSE_PERIOD))
            for uid, age in jobs:
                delta = datetime.utcnow() - age
                msg = "UID: {uid}, last signal: {age:.2f} seconds"
                print(msg.format(
                    uid = uid, 
                    age = delta.total_seconds()))
        else:
            print("No active jobs found.")

def view(args):
    from compdb.contrib import get_project
    from os.path import join, exists
    from os import listdir
    project = get_project()
    if args.url:
        url = join(args.prefix, args.url)
    else:
        url = join(args.prefix, project.get_default_view_url())
    if args.copy:
        q = "Are you sure you want to create copy of the whole dataset? This might create extremely high network load!"
        if not(args.yes or query_yes_no(q, 'no')):
            return
    if args.script:
        for line in project.create_view_script(url = url, cmd = args.script):
            print(line)
    else:
        if exists(args.prefix) and listdir(args.prefix):
            print("Path '{}' is not empty.".format(args.prefix))
            return
        project.create_view(url = url, copy = args.copy)

def check(args):
    from . import check
    checks = [
        ('Checking database connection...',
        check.check_database_connection),
        ('Checking global configuration...',
        check.check_global_config),
        ('Checking project configuration...',
        check.check_project_config),
        ]
    for msg, check in checks:
        print(msg)
        try:
            check()
        except Exception as error:
            print("Error: {}".format(error))
            if args.verbosity > 0:
                raise
        else:
            print("OK")

def submit(args):
    from . job_submit import submit_mpi
    submit_mpi(args.module)

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
    from . utility import query_yes_no
    from . project import RollBackupExistsError
    project = get_project()
    print("Trying to restore from: {}".format(args.snapshot))
    try:
        project.restore_snapshot(args.snapshot)
    except FileNotFoundError as error:
        raise RuntimeError("File not found: {}".format(error))
    except RollBackupExistsError as dst:
        q = "A backup from a previous restore attempt exists. "
        q += "Do you want to try to recover from that?"
        if query_yes_no(q, 'no'):
            try:
                project._restore_rollbackup(str(dst))
            except Exception as error:
                print("The recovery failed. The corrupted recovery backup lies in '{}'. It is probably safe to delete it after inspection.".format(dst))
                raise
            else:
                print("Successfully recovered.")
                project._remove_rollbackup(str(dst))
        else:
            q = "Do you want to delete it?"
            if query_yes_no(q, 'no'):
                project._remove_rollbackup(str(dst))
                print("Removed.")
    else:
        print("Success.")

def clean_up(args):
    from . import get_project
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
                project.remove(force = args.force)
            except RuntimeError as error:
                print("Error during project removal.")
                if not args.force:
                    print("This can be caused by currently executed jobs.")
                    print("Try 'compdb clenaup'.")
                    if args.yes or query_yes_no("Ignore this warning and remove anywas?", default = 'no'):
                        project.remove(force = True)
            else:
                print("Project removed from database.")
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
    parser_remove.add_argument(
        '-f', '--force',
        action = 'store_true',
        help = "Ignore errors during removal. May lead to data loss!")
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

    parser_info = subparsers.add_parser('info')
    parser_info.add_argument(
        '-j', '--jobs',
        nargs = '?',
        action = EmptyIsTrue,
        help = "Lists the jobs of this project. Provide a comma-separated list to show only a subset.",
        )
    parser_info.add_argument(
        '-s', '--status',
        action = 'store_true',
        help = "Print status information.")
    parser_info.add_argument(
        '-p', '--pulse',
        action = 'store_true',
        help = "Print job pulse status.")
    parser_info.add_argument(
        '-m', '--more',
        action = 'store_true',
        help = "Show more details.")
    parser_info.add_argument(
        '-a', '--all',
        action = 'store_true',
        help = "Show everything.")
    parser_info.set_defaults(func = info)
    
    parser_view = subparsers.add_parser('view')
    parser_view.add_argument(
        '--prefix',
        type = str,
        default = 'view/',
        help = "Prefix the given view url.")
    parser_view.add_argument(
        '-u', '--url',
        type = str,
        help = "Provide a url for the view in the form: abc/{a}/{b}, where each value in curly brackets denotes the name of a parameter."
        )
    parser_view.add_argument(
        '-c', '--copy',
        action = 'store_true',
        help = "Generate a copy of the whole dataset instead of linking to it. WARNING: This option may create very high network load!")
    parser_view.add_argument(
        '-s', '--script',
        type = str,
        nargs = '?',
        const = 'mkdir -p {head}\nln -s {src} {head}/{tail}',
        help = r"Output a line foreach job where {src} is replaced with the the job's storage directory path, {head} and {tail} combined represent your view path. Default: 'mkdir -p {head}\nln -s {src} {head}/{tail}'."
        )
    parser_view.set_defaults(func = view)

    parser_check = subparsers.add_parser('check')
    parser_check.set_defaults(func = check)

    parser_submit = subparsers.add_parser('submit')
    parser_submit.add_argument(
        'module',
        type = str,
        help = "The path to the python module containing job_pools.")
    parser_submit.set_defaults(func = submit)
    
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
