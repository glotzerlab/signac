# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import os
import sys
import argparse
import json
import logging
import getpass

from . import get_project
from . import __version__
from .common import config
from .common.configobj import flatten_errors, Section
from .common import six
from .common.crypt import get_crypt_context, parse_pwhash, get_keyring
from .contrib.utility import query_yes_no, prompt_password
try:
    from .common.host import get_database, get_credentials, make_uri
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


def main_project(args):
    project = get_project()
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
        job.document
    if args.workspace:
        print(job.workspace())
    else:
        print(job)


def main_init(args):
    try:
        get_project()
    except LookupError:
        with open("signac.rc", 'a') as file:
            file.write('project={}\n'.format(args.project_id))
        assert str(get_project()) == args.project_id
        _print_err("Initialized project '{}'.".format(args.project_id))
    else:
        raise RuntimeError(
            "Failed to initialize project '{}', '{}' is already a "
            "project root path.".format(args.project_id, os.getcwd()))


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

    parser_project = subparsers.add_parser('project')
    parser_project.add_argument(
        '-w', '--workspace',
        action='store_true',
        help="Print the project's workspace path instead of the project id.")
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
        help="print the job's workspace path instead of the job id.")
    parser_job.add_argument(
        '-c', '--create',
        action='store_true',
        help="Create the job's workspace directory if necessary.")
    parser_job.set_defaults(func=main_job)

    parser_init = subparsers.add_parser('init')
    parser_init.add_argument(
        'project_id',
        type=str,
        help="Initialize a project with the given project id.")
    parser_init.set_defaults(func=main_init)

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
