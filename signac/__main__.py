# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import os
import sys
import argparse
import json
import logging
import getpass
import difflib
import errno
from pprint import pprint, pformat

from . import get_project, init_project, index
from . import __version__
from .common import config
from .common.configobj import flatten_errors, Section
from .common import six
from .common.crypt import get_crypt_context, parse_pwhash, get_keyring
from .contrib.utility import query_yes_no, prompt_password
from .contrib.filterparse import parse_filter_arg
from .errors import DestinationExistsError
try:
    from .common.host import get_client, get_database, get_credentials, make_uri
except ImportError:
    HOST = False
else:
    HOST = True

PW_ENCRYPTION_SCHEMES = ['None']
DEFAULT_PW_ENCRYPTION_SCHEME = PW_ENCRYPTION_SCHEMES[0]
if get_crypt_context() is not None:
    PW_ENCRYPTION_SCHEMES.extend(get_crypt_context().schemes())
    DEFAULT_PW_ENCRYPTION_SCHEME = get_crypt_context().default_scheme()


CONFIG_HOST_DEFAULTS = {
    'url': 'mongodb://localhost',
    'username': getpass.getuser(),
    'auth_mechanism': 'none',
    'ssl_cert_reqs': 'required',
}


CONFIG_HOST_CHOICES = {
    'auth_mechanism': ('none', 'SCRAM-SHA-1', 'SSL-x509')
}


def _print_err(msg=None):
    print(msg, file=sys.stderr)


def _passlib_available():
    try:
        import passlib  # noqa
    except ImportError:
        return False
    else:
        return True


def _hide_password(line):
    if line.strip().startswith('password'):
        return ' ' * line.index('password') + 'password = ***'
    else:
        return line


def _prompt_for_new_password(attempts=3):
    for i in range(attempts):
        if i > 0:
            _print_err("Attempt {}:".format(i + 1))
        new_pw = prompt_password('New password: ')
        new_pw2 = prompt_password('New password (repeat): ')
        if new_pw == new_pw2:
            return new_pw
        else:
            _print_err("Passwords do not match!")
    else:
        raise ValueError("Too many failed attempts.")


def _update_password(config, hostname, scheme=None, new_pw=None):
    def hashpw(pw):
        if scheme is None:
            return pw
        else:
            return get_crypt_context().encrypt(
                pw, scheme=scheme)
    hostcfg = config['hosts'][hostname]
    hostcfg['password'] = get_credentials(hostcfg)
    db_auth = get_database(
        hostcfg.get('db_auth', 'admin'),
        hostname=hostname, config=config)
    if new_pw is None:
        new_pw = _prompt_for_new_password()
    pwhash = hashpw(new_pw)
    db_auth.add_user(hostcfg['username'], pwhash)
    return pwhash


def _read_index(project, fn_index=None):
    if fn_index is not None:
        _print_err("Reading index from file '{}'...".format(fn_index))
        fd = open(fn_index)
        return (json.loads(l) for l in fd)


def _open_job_by_id(project, job_id):
    "Attempt to open a job by id and provide user feedback on error."
    try:
        return project.open_job(id=job_id)
    except KeyError as error:
        close_matches = difflib.get_close_matches(
            job_id, [jid[:len(job_id)] for jid in project.find_job_ids()])
        msg = "Did not find job corresponding to id '{}'.".format(job_id)
        if len(close_matches) == 1:
            msg += " Did you mean '{}'?".format(close_matches[0])
        elif len(close_matches) > 1:
            msg += " Did you mean any of [{}]?".format('|'.join(close_matches))
        raise KeyError(msg)
    except LookupError as error:
        n = project.min_len_unique_id()
        raise LookupError("Multiple matches for abbreviated id '{}'. "
                          "Use at least {} characters for guaranteed "
                          "unique ids.".format(job_id, n))


def find_with_filter(args):
    if getattr(args, 'job_id', None):
        if args.filter or args.doc_filter:
            raise ValueError("Can't provide both 'job-id' and filter arguments!")
        else:
            return args.job_id

    project = get_project()
    index = _read_index(project, args.index)

    f = parse_filter_arg(args.filter)
    df = parse_filter_arg(args.doc_filter)
    return get_project().find_job_ids(index=index, filter=f, doc_filter=df)


def main_project(args):
    project = get_project()
    if args.access:
        fn = project.create_access_module()
        _print_err("Created access module '{}'.".format(fn))
        return
    if args.index:
        for doc in project.index():
            print(json.dumps(doc))
        return
    if args.workspace:
        print(project.workspace())
    else:
        print(project)


def main_job(args):
    project = get_project()
    if args.statepoint is '-':
        sp = input()
    else:
        sp = args.statepoint
    try:
        statepoint = json.loads(sp)
    except ValueError:
        _print_err("Error while reading statepoint: '{}'".format(sp))
        raise
    job = project.open_job(statepoint)
    if args.create:
        job.init()
    if args.workspace:
        print(job.workspace())
    else:
        print(job)


def main_statepoint(args):
    project = get_project()
    if args.job_id:
        jobs = (_open_job_by_id(project, jid) for jid in args.job_id)
    else:
        jobs = project
    for job in jobs:
        if args.pretty:
            pprint(job.statepoint(), depth=args.pretty)
        else:
            print(json.dumps(job.statepoint(), indent=args.indent, sort_keys=args.sort))


def main_document(args):
    project = get_project()
    for job_id in find_with_filter(args):
        job = _open_job_by_id(project, job_id)
        if args.pretty:
            pprint(dict(job.document), depth=args.pretty)
        else:
            print(json.dumps(dict(job.document), indent=args.indent, sort_keys=args.sort))


def main_move(args):
    project = get_project()
    dst_project = get_project(root=args.project)
    for job_id in args.job_id:
        try:
            job = _open_job_by_id(project, job_id)
            job.move(dst_project)
        except DestinationExistsError as error:
            _print_err("Destination already exists: '{}' in '{}'.".format(job, dst_project))
        else:
            _print_err("Moved '{}' to '{}'.".format(job, dst_project))


def main_clone(args):
    project = get_project()
    dst_project = get_project(root=args.project)
    for job_id in args.job_id:
        try:
            job = _open_job_by_id(project, job_id)
            dst_project.clone(job)
        except DestinationExistsError as error:
            _print_err("Destination already exists: '{}' in '{}'.".format(job, dst_project))
        else:
            _print_err("Cloned '{}' to '{}'.".format(job, dst_project))


def main_index(args):
    _print_err("Compiling master index for path '{}'...".format(
        os.path.realpath(args.root)))
    if args.tags:
        args.tags = set(args.tags)
        _print_err("Provided tags: {}".format(', '.join(sorted(args.tags))))
    for doc in index(root=args.root, tags=args.tags, raise_on_error=args.debug):
        print(json.dumps(doc))


def main_find(args):
    project = get_project()

    if args.show:
        len_id = max(6, project.min_len_unique_id())

        def format_lines(cat, _id, s):
            if args.one_line:
                if isinstance(s, dict):
                    s = json.dumps(s, sort_keys=True)
                return _id[:len_id] + ' ' + cat + '\t' + s
            else:
                return pformat(s, depth=args.show)

    try:
        for job_id in find_with_filter(args):
            if args.show:
                job = project.open_job(id=job_id)
                jid = job.get_id()
                print(jid)
                print(format_lines('sp ', jid, job.statepoint()))
                print(format_lines('doc', jid, dict(job.document)))
            else:
                print(job_id)
    except IOError as error:
        if error.errno == errno.EPIPE:
            sys.stderr.close()
        else:
            raise


def main_view(args):
    project = get_project()
    project.create_linked_view(
        prefix=args.prefix,
        job_ids=find_with_filter(args),
        index=_read_index(args.index))


def main_init(args):
    project = init_project(
        name=args.project_id,
        root=os.getcwd(),
        workspace=args.workspace)
    _print_err("Initialized project '{}'.".format(project))


def verify_config(cfg, preserve_errors=True):
    verification = cfg.verify(
        preserve_errors=preserve_errors, skip_missing=True)
    if verification is True:
        _print_err("Passed.")
    else:
        for entry in flatten_errors(cfg, verification):
            # each entry is a tuple
            section_list, key, error = entry
            if key is not None:
                section_list.append(key)
            else:
                section_list.append('[missing section]')
            section_string = '.'.join(section_list)
            if error is False:
                error = 'Possibly invalid or missing.'
            else:
                error = type(error).__name__
            _print_err(' '.join((section_string, ':', error)))


def main_config_show(args):
    cfg = None
    if args.local and args.globalcfg:
        raise ValueError(
            "You can specify either -l/--local or -g/--global, not both.")
    elif args.local:
        for fn in config.CONFIG_FILENAMES:
            if os.path.isfile(fn):
                if cfg is None:
                    cfg = config.read_config_file(fn)
                else:
                    cfg.merge(config.read_config_file(fn))
    elif args.globalcfg:
        cfg = config.read_config_file(config.FN_CONFIG)
    else:
        cfg = config.load_config()
    if cfg is None:
        if args.local and args.globalcfg:
            mode = ' local or global '
        elif args.local:
            mode = ' local '
        elif args.globalcfg:
            mode = ' global '
        else:
            mode = ''
        _print_err("Did not find a{}configuration file.".format(mode))
        return
    for key in args.key:
        for kt in key.split('.'):
            cfg = cfg.get(kt)
            if cfg is None:
                break
    if not isinstance(cfg, Section):
        print(cfg)
    else:
        for line in config.Config(cfg).write():
            print(_hide_password(line))


def main_config_verify(args):
    cfg = None
    if args.local and args.globalcfg:
        raise ValueError(
            "You can specify either -l/--local or -g/--global, not both.")
    elif args.local:
        for fn in config.CONFIG_FILENAMES:
            if os.path.isfile(fn):
                if cfg is None:
                    cfg = config.read_config_file(fn)
                else:
                    cfg.merge(config.read_config_file(fn))
    elif args.globalcfg:
        cfg = config.read_config_file(config.FN_CONFIG)
    else:
        cfg = config.load_config()
    if cfg is None:
        if args.local and args.globalcfg:
            mode = ' local or global '
        elif args.local:
            mode = ' local '
        elif args.globalcfg:
            mode = ' global '
        else:
            mode = ''
        raise RuntimeWarning(
            "Did not find a{}configuration file.".format(mode))
    if cfg.filename is not None:
        _print_err("Verifcation of config file '{}'.".format(cfg.filename))
    verify_config(cfg)


def main_config_set(args):
    if not (args.local or args.globalcfg):
        args.local = True
    fn_config = None
    if args.local and args.globalcfg:
        raise ValueError(
            "You can specify either -l/--local or -g/--global, not both.")
    elif args.local:
        for fn_config in config.CONFIG_FILENAMES:
            if os.path.isfile(fn_config):
                break
    elif args.globalcfg:
        fn_config = config.FN_CONFIG
    else:
        raise ValueError(
            "You need to specify either -l/--local or -g/--global "
            "to specify which configuration to modify.")
    try:
        cfg = config.read_config_file(fn_config)
    except OSError:
        cfg = config.get_config(fn_config)
    keys = args.key.split('.')
    if keys[-1].endswith('password'):
        raise RuntimeError(
            "Passwords need to be set with `{} config host "
            "HOSTNAME -p`!".format(os.path.basename(sys.argv[0])))
    else:
        if len(args.value) == 0:
            raise ValueError("No value argument provided!")
        elif len(args.value) == 1:
            args.value = args.value[0]
    sec = cfg
    for key in keys[:-1]:
        sec = sec.setdefault(key, dict())
    try:
        sec[keys[-1]] = args.value
        _print_err("Updated value for '{}'.".format(args.key))
    except TypeError:
        raise KeyError(args.key)
    _print_err("Writing configuration to '{}'.".format(
        os.path.abspath(fn_config)))
    cfg.write()


def main_config_host(args):
    if args.update_pw is True:
        args.update_pw = DEFAULT_PW_ENCRYPTION_SCHEME
    if not HOST:
        raise ImportError("pymongo is required for host configuration!")
    from pymongo.uri_parser import parse_uri
    if not (args.local or args.globalcfg):
        args.globalcfg = True
    fn_config = None
    if args.local and args.globalcfg:
        raise ValueError(
            "You can specify either -l/--local or -g/--global, not both.")
    elif args.local:
        for fn_config in config.CONFIG_FILENAMES:
            if os.path.isfile(fn_config):
                break
    elif args.globalcfg:
        fn_config = config.FN_CONFIG
    else:
        raise ValueError(
            "You need to specify either -l/--local or -g/--global "
            "to specify which configuration to modify.")
    try:
        cfg = config.read_config_file(fn_config)
    except OSError:
        cfg = config.get_config(fn_config)

    def hostcfg():
        return cfg.setdefault(
            'hosts', dict()).setdefault(args.hostname, dict())

    if sum((args.test, args.remove, args.show_pw)) > 1:
        raise ValueError(
            "Please select only one of the following options: "
            "[--test | -r/--remove | --show-pw].")

    if args.test:
        if hostcfg():
            _print_err("Trying to connect to host '{}'...".format(args.hostname))
            try:
                client = get_client(hostcfg())
                client.address
            except Exception:
                _print_err("Encountered error while tyring to "
                           "connect to host '{}'.".format(args.hostname))
                raise
            else:
                print("Successfully connected to host '{}'.".format(args.hostname))
        else:
            _print_err("Host '{}' is not configured.".format(args.hostname))
        return

    if args.remove:
        if hostcfg():
            q = "Are you sure you want to remove host '{}'."
            if args.yes or query_yes_no(q.format(args.hostname), 'no'):
                kr = get_keyring()
                if kr:
                    if kr.get_password('signac', make_uri(hostcfg())):
                        kr.delete_password('signac', make_uri(hostcfg()))
                del cfg['hosts'][args.hostname]
                cfg.write()
        else:
            _print_err("Nothing to remove.")
        return

    if args.show_pw:
        pw = get_credentials(hostcfg(), ask=False)
        if pw is None:
            raise RuntimeError("Did not find stored password!")
        else:
            print(pw)
            return

    if hostcfg():
        _print_err("Configuring host '{}'.".format(args.hostname))
    else:
        _print_err("Configuring new host '{}'.".format(args.hostname))

    def hide_password(k, v):
        "Hide all fields containing sensitive information."
        return '***' if k.endswith('password') else v

    def update_hostcfg(** update):
        "Update the host configuration."
        store = False
        for k, v in update.items():
            if v is None:
                if k in hostcfg():
                    logging.info("Deleting key {}".format(k))
                    del cfg['hosts'][args.hostname][k]
                    store = True
            elif k not in hostcfg() or v != hostcfg()[k]:
                logging.info("Setting {}={}".format(k, hide_password(k, v)))
                cfg['hosts'][args.hostname][k] = v
                store = True
        if store:
            cfg.write()

    def requires_username():
        if 'username' not in hostcfg():
            raise ValueError("Please specify a username!")

    if args.uri:
        parse_uri(args.uri)
        update_hostcfg(url=args.uri)
    elif 'url' not in hostcfg():
        update_hostcfg(url='mongodb://localhost')

    if args.username:
        update_hostcfg(
            username=args.username,
            auth_mechanism='SCRAM-SHA-1')

    if args.update_pw:
        requires_username()
        if not _passlib_available():
            _print_err(
                "WARNING: It is highly recommended to install passlib "
                "to encrypt your password!")
        pwhash = _update_password(
            cfg, args.hostname,
            scheme=None if args.update_pw == 'None' else args.update_pw,
            new_pw=None if args.password is True else args.password)
        if args.password:
            update_hostcfg(
                password=pwhash, password_config=None)
        elif args.update_pw == 'None':
            update_hostcfg(
                password=None, password_config=None)
        else:
            update_hostcfg(
                password=None, password_config=parse_pwhash(pwhash))
    elif args.password:
        requires_username()
        if args.password is True:
            new_pw = prompt_password()
        else:
            new_pw = args.password
        update_hostcfg(password=new_pw, password_config=None)

    _print_err("Configured host '{}':".format(args.hostname))
    print("[hosts]")
    for line in config.Config({args.hostname: hostcfg()}).write():
        print(_hide_password(line))


def main():
    parser = argparse.ArgumentParser(
        description="signac aids in the management, access and analysis of "
                    "large-scale computational investigations.")
    parser.add_argument(
        '--debug',
        action='store_true',
        help="Show traceback on error for debugging.")
    parser.add_argument(
        '--version',
        action='store_true',
        help="Display the version number and exit.")
    parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help="Answer all questions with yes. Useful for scripted interaction.")
    subparsers = parser.add_subparsers()

    parser_init = subparsers.add_parser('init')
    parser_init.add_argument(
        'project_id',
        type=str,
        help="Initialize a project with the given project id.")
    parser_init.add_argument(
        '-w', '--workspace',
        type=str,
        default='workspace',
        help="The path to the workspace directory.")
    parser_init.set_defaults(func=main_init)

    parser_project = subparsers.add_parser('project')
    parser_project.add_argument(
        '-w', '--workspace',
        action='store_true',
        help="Print the project's workspace path instead of the project id.")
    parser_project.add_argument(
        '-i', '--index',
        action='store_true',
        help="Generate and print an index for the project.")
    parser_project.add_argument(
        '-a', '--access',
        action='store_true',
        help="Create access module for indexing.")
    parser_project.set_defaults(func=main_project)

    parser_job = subparsers.add_parser('job')
    parser_job.add_argument(
        'statepoint',
        nargs='?',
        default='-',
        type=str,
        help="The job's statepoint in JSON format. "
             "Omit this argument to read from STDIN.")
    parser_job.add_argument(
        '-w', '--workspace',
        action='store_true',
        help="Print the job's workspace path instead of the job id.")
    parser_job.add_argument(
        '-c', '--create',
        action='store_true',
        help="Create the job's workspace directory if necessary.")
    parser_job.set_defaults(func=main_job)

    parser_statepoint = subparsers.add_parser(
        'statepoint',
        description="Print the statepoint(s) corresponding to one or "
                    "more job ids.")
    parser_statepoint.add_argument(
        'job_id',
        nargs='*',
        type=str,
        help="One or more job ids. The job corresponding to a job "
             "id must be initialized.")
    parser_statepoint.add_argument(
        '-p', '--pretty',
        type=int,
        nargs='?',
        const=3,
        help="Print state point in pretty format. "
             "An optional argument to this flag specifies the maximal "
             "depth a state point is printed.")
    parser_statepoint.add_argument(
        '-i', '--indent',
        type=int,
        nargs='?',
        const='2',
        help="Specify the indentation of the JSON formatted state point.")
    parser_statepoint.add_argument(
        '-s', '--sort',
        action='store_true',
        help="Sort the state point keys for output.")
    parser_statepoint.set_defaults(func=main_statepoint)

    parser_document = subparsers.add_parser(
        'document',
        description="Print the document(s) corresponding to one or "
                    "more job ids.")
    parser_document.add_argument(
        'job_id',
        nargs='*',
        type=str,
        help="One or more job ids. The job corresponding to a job "
             "id must be initialized.")
    parser_document.add_argument(
        '-p', '--pretty',
        type=int,
        nargs='?',
        const=3,
        help="Print document in pretty format. "
             "An optional argument to this flag specifies the maximal "
             "depth a document is printed.")
    parser_document.add_argument(
        '-i', '--indent',
        type=int,
        nargs='?',
        const='2',
        help="Specify the indentation of the JSON formatted state point.")
    parser_document.add_argument(
        '-s', '--sort',
        action='store_true',
        help="Sort the document keys for output in JSON format.")
    parser_document.add_argument(
        '-f', '--filter',
        type=str,
        nargs='+',
        help="Show documents of jobs matching this state point filter.")
    parser_document.add_argument(
        '-d', '--doc-filter',
        type=str,
        nargs='+',
        help="Show documents of job matching this document filter.")
    parser_document.add_argument(
        '--index',
        type=str,
        help="The filename of an index file.")
    parser_document.set_defaults(func=main_document)

    parser_move = subparsers.add_parser('move')
    parser_move.add_argument(
        'project',
        type=str,
        help="The root directory of the project to move one or more jobs to.")
    parser_move.add_argument(
        'job_id',
        nargs='+',
        type=str,
        help="One or more job ids of jobs to move. The job corresponding to a "
             "job id must be initialized.")
    parser_move.set_defaults(func=main_move)

    parser_clone = subparsers.add_parser('clone')
    parser_clone.add_argument(
        'project',
        type=str,
        help="The root directory of the project to clone one or more jobs in.")
    parser_clone.add_argument(
        'job_id',
        nargs='+',
        type=str,
        help="One or more job ids of jobs to clone. The job corresponding to a "
             "job id must be initialized.")
    parser_clone.set_defaults(func=main_clone)

    parser_index = subparsers.add_parser('index')
    parser_index.add_argument(
        'root',
        nargs='?',
        default='.',
        help="Specify the root path from where the master index is to be compiled.")
    parser_index.add_argument(
        '-t', '--tags',
        nargs='+',
        help="Specify tags for this master index compilation.")
    parser_index.set_defaults(func=main_index)

    parser_find = subparsers.add_parser(
        'find',
        description="""All filter arguments may be provided either directly in JSON
                       encoding or in a simplified form, e.g., -- $ signac find a 42 --
                       is equivalent to -- $ signac find '{"a": 42}'."""
                       )
    parser_find.add_argument(
        'filter',
        type=str,
        nargs='*',
        help="A JSON encoded state point filter (key-value pairs).")
    parser_find.add_argument(
        '-d', '--doc-filter',
        type=str,
        nargs='+',
        help="A document filter.")
    parser_find.add_argument(
        '-i', '--index',
        type=str,
        help="The filename of an index file.")
    parser_find.add_argument(
        '-s', '--show',
        type=int,
        nargs='?',
        const=3,
        help="Show the state point and document of each job.")
    parser_find.add_argument(
        '-1', '--one-line',
        action='store_true',
        help="Print output in JSON and on one line.")
    parser_find.set_defaults(func=main_find)

    parser_view = subparsers.add_parser('view')
    parser_view.add_argument(
        'prefix',
        type=str,
        nargs='?',
        default='view',
        help="The path where the view is to be created.")
    selection_group = parser_view.add_argument_group('select')
    selection_group.add_argument(
        '-f', '--filter',
        type=str,
        nargs='+',
        help="Limit the view to jobs matching this state point filter.")
    selection_group.add_argument(
        '-d', '--doc-filter',
        type=str,
        nargs='+',
        help="Limit the view to jobs matching this document filter.")
    selection_group.add_argument(
        '-j', '--job-id',
        type=str,
        nargs='+',
        help="Limit the view to jobs with these job ids.")
    selection_group.add_argument(
        '-i', '--index',
        type=str,
        help="The filename of an index file.")
    parser_view.set_defaults(func=main_view)

    parser_config = subparsers.add_parser('config')
    parser_config.add_argument(
        '-g', '--global',
        dest='globalcfg',
        action='store_true',
        help="Modify the global configuration.")
    parser_config.add_argument(
        '-l', '--local',
        action='store_true',
        help="Modify the local configuration.")
    parser_config.add_argument(
        '-f', '--force',
        action='store_true',
        help="Skip sanity checks when modifying the configuration.")
    config_subparsers = parser_config.add_subparsers()

    parser_show = config_subparsers.add_parser('show')
    parser_show.add_argument(
        'key',
        type=str,
        nargs='*',
        help="The key(s) to show, omit to show the full configuration.")
    parser_show.set_defaults(func=main_config_show)

    parser_set = config_subparsers.add_parser('set')
    parser_set.add_argument(
        'key',
        type=str,
        help="The key to modify.")
    parser_set.add_argument(
        'value',
        type=str,
        nargs='*',
        help="The value to set key to.")
    parser_set.add_argument(
        '-f', '--force',
        action='store_true',
        help="Override any validation warnings.")
    parser_set.set_defaults(func=main_config_set)

    parser_host = config_subparsers.add_parser('host')
    parser_host.add_argument(
        'hostname',
        type=str,
        help="The name of the specified resource. "
             "Note: The name can be arbitrarily chosen.")
    parser_host.add_argument(
        'uri',
        type=str,
        nargs='?',
        help="Set the URI of the specified resource, for "
             "example: 'mongodb://localhost'.")
    parser_host.add_argument(
        '-u', '--username',
        type=str,
        help="Set the username for this resource.")
    parser_host.add_argument(
        '-p', '--password',
        type=str,
        nargs='?',
        const=True,
        help="Store a password for the specified resource.")
    parser_host.add_argument(
        '--update-pw',
        type=str,
        nargs='?',
        const=True,
        choices=PW_ENCRYPTION_SCHEMES,
        help="Update the password of the specified resource. "
             "Use in combination with -p/--password to store the "
             "new password. You can optionally specify the hashing "
             "algorithm used for the password encryption. Anything "
             "else but 'None' requires passlib! (default={})".format(
                 DEFAULT_PW_ENCRYPTION_SCHEME))
    parser_host.add_argument(
        '--show-pw',
        action='store_true',
        help="Show the password if it was stored and exit.")
    parser_host.add_argument(
        '-r', '--remove',
        action='store_true',
        help="Remove the specified resource.")
    parser_host.add_argument(
        '--test',
        action='store_true',
        help="Attempt connecting to the specified host.")
    parser_host.set_defaults(func=main_config_host)

    parser_verify = config_subparsers.add_parser('verify')
    parser_verify.set_defaults(func=main_config_verify)

    # This is a hack, as argparse itself does not
    # allow to parse only --version without any
    # of the other required arguments.
    if '--version' in sys.argv:
        print('signac', __version__)
        sys.exit(0)

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif six.PY2:
        logging.basicConfig(level=logging.WARNING)
    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(2)
    try:
        args.func(args)
    except KeyboardInterrupt:
        _print_err()
        _print_err("Interrupted.")
        if args.debug:
            raise
        sys.exit(1)
    except Exception as error:
        _print_err('Error: {}'.format(str(error)))
        if args.debug:
            raise
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
