# PYTHON_ARGCOMPLETE_NOT_OK

import logging
import sys
import warnings
import datetime
import os
import argparse

import pymongo
from bson.json_util import dumps

from . import admin, update, get_project, init_project, configure, check
from .job import PULSE_PERIOD
from .errors import ConnectionFailure
from .utility import add_verbosity_argument, set_verbosity_level, EmptyIsTrue, SmartFormatter, query_yes_no

logger = logging.getLogger(__name__)

def info(args):
    project = get_project()
    if args.regex:
        args.separator = '|'
        args.no_title = True
        args.jobs = True
    if args.all:
        args.status = True
        args.jobs = True
        args.pulse = True
    if not args.no_title:
        print(project)
    if args.more:
        print(project.root_directory())
    if args.status:
        n_registered = len(list(project.find_job_ids()))
        n_active = project.num_active_jobs()
        n_pot_dead = 0
        n_w_pulse = 0
        for uid, age in project.job_pulse():
            n_w_pulse += 1
            delta = datetime.datetime.utcnow() - age
            if delta.total_seconds() > PULSE_PERIOD:
                n_pot_dead += 1
        print("{} registered job(s)".format(n_registered))
        print("{} active job(s)".format(n_active))
        if n_pot_dead:
            print("{} potentially dead job(s)".format(n_pot_dead))
            if not args.pulse:
                print("Use 'signac info -p' to check job status and 'signac cleanup' to remove dead jobs from the database.")
        if n_w_pulse != n_active:
            print("The database records for the number of active jobs and the number of pulse processes deviates. Inform the DB administrator.")
    if args.jobs:
        if args.regex:
            print('(', end='')
        if args.open_only:
            legit_ids = project._active_job_ids()
        else:
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
            if args.separator == '\n':
                print("Job ID{}".format(' ' * 26), "Open Instances")
            else:
                print("JobID OpenInstances")
        sep = None
        for known in known_ids:
            if sep:
                print(sep, end='')
            if args.status:
                job = project.open_job_from_id(known)
                print(job, job.num_open_instances(), end='')
            else:
                print(known, end='')
            if args.more:
                job = project.open_job_from_id(known)
                print('\n' + dumps(job.parameters(), sort_keys = True), end='')
            sep = args.separator
        if args.regex:
            print(')', end='')
        print()
    if args.pulse:
        jobs = list(project.job_pulse())
        if jobs:
            print("Pulse period (expected): {}s.".format(PULSE_PERIOD))
            for uid, age in jobs:
                delta = datetime.datetime.utcnow() - age
                msg = "UID: {uid}, last signal: {age:.2f} seconds"
                print(msg.format(
                    uid = uid, 
                    age = delta.total_seconds()))
        else:
            print("No active jobs found.")

def view(args):
    project = get_project()
    if args.flat:
        if args.url or args.workspace or args.copy or args.script:
            raise ValueError("Illegal combination of arguments: '--flat' can only be combined with prefix argument.")
        project.create_flat_view(prefix=args.prefix)
        return
    if args.copy:
        q = "Are you sure you want to create copy of the whole dataset? This might create extremely high network load!"
        if not(args.yes or query_yes_no(q, 'no')):
            return
    if args.script:
        for line in project.create_view_script(url=args.url, prefix=args.prefix, cmd=args.script, workspace=args.workspace):
            print(line)
    else:
        if os.path.exists(args.prefix) and os.listdir(args.prefix):
            print("Path '{}' is not empty.".format(args.prefix))
            return
        project.create_view(url=args.url, make_copy=args.copy, workspace=args.workspace, prefix=args.prefix)

def run_checks(args):
    project = get_project()
    encountered_error = False
    checks = [
        ('global configuration',
        check.check_global_config),
        ('database connection',
        check.check_database_connection)]
    try:
        project_id = project.get_id()
    except LookupError:
        print("Current working directory is not configured as a project directory.")
    else:
        print("Found project: '{}'.".format(project_id))
        checks.extend([
            ('project configuration (offline)',
            check.check_project_config_offline)])
        checks.extend([
            ('project configuration (online, readonly)',
            check.check_project_config_online_readonly)])
        checks.extend([
            ('project configuration (online)',
            check.check_project_config_online)])
        checks.extend([
            ('project version',
            check.check_project_version)])
    for msg, check_ in checks:
        print()
        print("Checking {} ... ".format(msg), end='', flush=True)
        try:
            ok = check_()
        except ConnectionFailure as error:
            print()
            print("Error: {}".format(error))
            print("You can set a different host with 'signac config set database_host $YOURHOST'.")
            if args.verbosity > 0:
                raise
            encountered_error = True
        except pymongo.errors.OperationFailure as error:
            print()
            print("Possible authorization problem.")
            auth_mechanism = project.config['database_auth_mechanism']
            print("Your current auth mechanism is set to '{}'. Is that correct?".format(auth_mechanism))
            print("Configure the auth mechanism with:")
            print("signac config set database_auth_mechanism [none|SCRAM-SHA-1|SSL-x509]")
            if args.verbosity > 0:
                raise
            encountered_error = True
        except Exception as error:
            print()
            print("Error: {}".format(error))
            if args.verbosity > 0:
                raise
            encountered_error = True
        else:
            if ok:
                print("OK.")
            else:
                encountered_error = True
                print("Failed.")
    print()
    if encountered_error:
        print("Not all checks passed.")
        v = '-' + 'v' * (args.verbosity + 1)
        print("Use 'signac {} check' to increase verbosity of messages.".format(v))
    else:
        print("All tests passed. No errors.")

def show_log(args):
    formatter = logging.Formatter(
        fmt = args.format,
        #datefmt = "%Y-%m-%d %H:%M:%S",
        style = '{')
    project = get_project()
    showed_log = False
    for record in project.get_logs(
            level = args.level, limit = args.lines):
        print(formatter.format(record))
        showed_log = True
    if not showed_log:
        print("No logs available.")

def clean_up(args):
    project = get_project()
    msg = "Clearing database for all jobs without sign of life for more than {} seconds."
    print(msg.format(args.tolerance_time))
    project.kill_dead_jobs(seconds = args.tolerance_time)

def clear(args):
    project = get_project()
    question = "Are you sure you want to clear project '{}'?"
    if args.yes or query_yes_no(question.format(project.get_id()), default = 'no'):
        try:
            project.clear(force = args.force)
        except RuntimeError:
            print("Error during project clearance.")
            if not args.force:
                print("This can be caused by currently executed jobs.")
                print("Try 'signac cleanup'.")
                if args.yes or query_yes_no("Ignore this warning and remove anyways?", default = 'no'):
                    project.clear(force = True)
            raise


def remove(args):
    project = get_project()
    if args.project:
        question = "Are you sure you want to remove project '{}'?"
        if args.yes or query_yes_no(question.format(project.get_id()), default = 'no'):
            try:
                project.remove(force = args.force)
            except RuntimeError:
                print("Error during project removal.")
                if not args.force:
                    print("This can be caused by currently executed jobs.")
                    print("Try 'signac cleanup'.")
                    if args.yes or query_yes_no("Ignore this warning and remove anyways?", default = 'no'):
                        project.remove(force = True)
                raise
            else:
                print("Project removed from database.")
    if args.job:
        if len(args.job) == 1 and args.job[0] == 'all':
            match = set(project.find_job_ids())
        else:
            job_ids = set(args.job)
            legit_ids = project.find_job_ids()
            match = set()
            for legit_id in legit_ids:
                for selected in job_ids:
                    if legit_id.startswith(selected):
                        match.add(legit_id)
        if args.release:
            print("{} job(s) selected for release.".format(len(match)))
            for id_ in match:
                job = project.open_job_from_id(id_)
                job.force_release()
            print("Released selected jobs.")
        else:
            print("{} job(s) selected for removal.".format(len(match)))
            q = "Are you sure you want to delete the selected jobs?"
            if not(args.yes or query_yes_no(q)):
                return
            for id_ in match:
                job = project.open_job_from_id(id_)
                job.remove(force = args.force)
            print("Removed selected jobs.")
    if args.logs:
        question = "Are you sure you want to clear all logs from project '{}'?"
        if args.yes or query_yes_no(question.format(project.get_id())):
            project.clear_logs()
    if not (args.project or args.job or args.logs):
        print("Nothing selected for removal.")

def main(argv=None):
    parser = argparse.ArgumentParser(
        description = "signac - computational database",
        formatter_class = SmartFormatter)
    parser.add_argument(
        '-y', '--yes',
        action = 'store_true',
        help = "Assume yes to all questions.",)
    add_verbosity_argument(parser)
    parser.add_argument(
        '-W', '--warnings',
        type = str,
        default = 'ignore',
        choices = ['ignore', 'default', 'all', 'module', 'once', 'error'],
        help = "Control the handling of warnings. By default all warnings are ignored.")
    parser.add_argument(
        '--version',
        action = 'store_true',
        help = "Print the signac version and exit.")

    subparsers = parser.add_subparsers()

    parser_init = subparsers.add_parser('init')
    init_project.setup_parser(parser_init)
    parser_init.set_defaults(func = init_project.init_project)
    
    parser_config = subparsers.add_parser('config',
        description = "Configure signac for your environment.",
        formatter_class = SmartFormatter)
    configure.setup_parser(parser_config)
    parser_config.set_defaults(func = configure.configure)

    parser_clear = subparsers.add_parser('clear')
    parser_clear.add_argument(
        '-f', '--force',
        action = 'store_true',
        help = "Ignore errors during clearance. May lead to data loss!")
    parser_clear.set_defaults(func = clear)

    parser_remove = subparsers.add_parser('remove')
    parser_remove.add_argument(
        '-j', '--job',
        nargs = '*',
        help = "Remove all jobs that match the provided ids. Use 'all' to select all jobs. Example: '-j ed05b' or '-j=ed05b,59255' or '-j all'.",
        )
    parser_remove.add_argument(
        '-r', '--release',
        action = 'store_true',
        help = "Release locked jobs instead of removing them.",
        )
    parser_remove.add_argument(
        '-p', '--project',
        action = 'store_true',
        help = 'Remove the whole project.',
        )
    parser_remove.add_argument(
        '-l', '--logs',
        action = 'store_true',
        help = "Remove all logs.",
        )
    parser_remove.add_argument(
        '-f', '--force',
        action = 'store_true',
        help = "Ignore errors during removal. May lead to data loss!")
    parser_remove.set_defaults(func = remove)

    parser_cleanup = subparsers.add_parser('cleanup')
    default_wait = int(20 *  PULSE_PERIOD)
    parser_cleanup.add_argument(
        '-t', '--tolerance-time',
        type = int,
        help = "Tolerated time in seconds since last pulse before a job is declared dead (default={}).".format(default_wait),
        default = default_wait)
    parser_cleanup.add_argument(
        '-r', '--release',
        action = 'store_true',
        help = "Release locked jobs.")
    parser_cleanup.set_defaults(func = clean_up)

    parser_info = subparsers.add_parser('info')
    parser_info.add_argument(
        '-j', '--jobs',
        nargs = '?',
        action = EmptyIsTrue,
        help = "Lists the jobs of this project. Provide a comma-separated list to show only a subset.",
        )
    parser_info.add_argument(
        '--open-only',
        action = 'store_true',
        help = "Only list open jobs.")
    parser_info.add_argument(
        '--separator',
        type = str,
        default = '\n',
        help = "Character with which to seperate job ids.")
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
    parser_info.add_argument(
        '--no-title',
        action = 'store_true',
        help = "Do not print the title. Useful for parsing scripts.")
    parser_info.add_argument(
        '--regex',
        action = 'store_true',
        help = "Print job ids as regex expression.")
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
    parser_view.add_argument(
        '-w', '--workspace',
        action = 'store_true',
        help = "Generate a view of the workspace instead of the filestorage.",
        )
    parser_view.add_argument(
        '--flat',
        action = 'store_true',
        help = "Generate a flat view of workspace and storage.")
    parser_view.set_defaults(func = view)

    parser_check = subparsers.add_parser('check')
    #parser_check.add_argument(
    #    '--offline',
    #    action = 'store_true',
    #    help = 'Perform offline checks.',)
    parser_check.set_defaults(func = run_checks)

    parser_log = subparsers.add_parser('log')
    parser_log.add_argument(
        '-l', '--level',
        type = str,
        default = logging.INFO,
        help = "The minimum log level to be retrieved, either as numeric value or name. Ex. -l DEBUG"
        )
    parser_log.add_argument(
        '-n', '--lines',
        type = int,
        default = 100,
        help = "Only output the last n lines from the log.",
        )
    parser_log.add_argument(
        '-f', '--format',
        type = str,
        default = "{asctime} {levelname} {message}",
        help = "The formatting of log messages.",
        )
    parser_log.set_defaults(func = show_log)

    parser_admin = subparsers.add_parser('user')
    admin.setup_parser(parser_admin)

    parser_update = subparsers.add_parser('update')
    update.setup_parser(parser_update)

    try:
        import argcomplete
        argcomplete.autocomplete(parser)
    except ImportError:
        pass
    
    args = parser.parse_args(argv)
    if args.version:
        from .. import VERSION
        print("signac {}".format(VERSION))
        return 0
    set_verbosity_level(args.verbosity)
    warnings.simplefilter(args.warnings)
    try:
        if 'func' in args:
            args.func(args)
        else:
            parser.print_usage()
    except Exception as error:
        if args.verbosity > 1:
            raise
        else:
            print("Error: {}".format(error))
            v = '-' + 'v' * (args.verbosity + 1)
            print("Use signac {} to increase verbosity of messages.".format(v))
            return 1
    except KeyboardInterrupt:
        if args.verbosity > 1:
            raise
        else:
            print()
            print("Interrupted.")
            return 1
    else:
        return 0

if __name__ == '__main__':
    sys.exit(main())
