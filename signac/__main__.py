# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the MIT License.
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
from .common.configobj import flatten_errors
from .common import six
from .common.crypt import get_crypt_context, parse_pwhash
from .common.host import get_database, get_current_password
from .contrib.utility import query_yes_no, prompt_password


CONFIG_HOST_DEFAULTS = {
    'url': 'localhost',
    'username': getpass.getuser(),
    'auth_mechanism': 'none',
    'ssl_cert_reqs': 'required',
}


CONFIG_HOST_CHOICES = {
    'auth_mechanism': ('none', 'SCRAM-SHA-1', 'SSL-x509')
}


def _print_err(msg):
    _print_err(msg)


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


def _update_password(config, hostname):
    hostcfg = config['hosts'][hostname]
    hostcfg['password'] = get_current_password(hostcfg)
    db_auth = get_database(
        hostcfg.get('db_auth', 'admin'),
        hostname=hostname, config=config)
    new_pw = _prompt_for_new_password()
    pwhash = get_crypt_context().encrypt(new_pw)
    db_auth.add_user(hostcfg['username'], pwhash)
    return pwhash


def _format_prompt(
        key, default=None, choices=None,
        prompt="Enter value for {key}{choices}{default}: "):
    input_ = raw_input if six.PY2 else input  # noqa
    return input_(prompt.format(
        key=key,
        choices='' if choices is None else ' [{}]'.format('|'.join(choices)),
        default='' if default is None else ' ({})'.format(default)))


def _select_from_choices(value, choices):
    m = [c for c in choices if c.startswith(value)]
    if not m:
        m = [c for c in choices if c.lower().startswith(value.lower())]
    if len(m) == 0:
        raise ValueError("Illegal value '{}', not in [{}].".format(
            value, '|'.join(choices)))
    elif len(m) == 1:
        return m[0]
    else:
        raise ValueError("Ambigious value '{}', choices=[{}].".format(
            value, '|'.join(choices)))


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
    verification = cfg.verify(preserve_errors=preserve_errors)
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
            _print_err(section_string, ':', error)


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
    for line in config.Config(cfg).write():
        print(line)


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
            "Passwords need to be updated with '{} config host'!".format(
                os.path.basename(sys.argv[0])))
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

    if hostcfg():
        _print_err("Configuring host '{}'.".format(args.hostname))
    else:
        _print_err("Configuring new host '{}'.".format(args.hostname))

    def hide_password(k, v):
        return '***' if k.endswith('password') else v

    def update_hostcfg(update):
        store = False
        for k, v in update.items():
            if v is None and k in hostcfg():
                _print_err("Deleting key {}".format(k))
                del cfg['hosts'][args.hostname][k]
                store = True
            elif k not in hostcfg() or v != hostcfg()[k]:
                _print_err("Setting {}={}".format(k, hide_password(k, v)))
                cfg['hosts'][args.hostname][k] = v
                store = True
        if store:
            cfg.write()

    def prompt_pw():
        q = "Do you want to update the password?"
        if query_yes_no(q, default='no'):
            pwhash = _update_password(cfg, args.hostname)
            if query_yes_no("Do you want to store the password?"):
                return dict(password=pwhash, password_config=None)
            else:
                pwcfg = parse_pwhash(pwhash)
                return dict(password=None, password_config=pwcfg)
        else:
            return {}

    def prompt(key):
        if key.endswith('password'):
            return prompt_pw()
        default = hostcfg().get(key, CONFIG_HOST_DEFAULTS.get(key))
        ret = _format_prompt(key=key, default=default,
                             choices=CONFIG_HOST_CHOICES.get(key))
        if args.force:
            return {key: ret}
        elif ret:
            choices = CONFIG_HOST_CHOICES.get(key)
            if choices is None:
                return {key: ret}
            elif key in choices:
                return {key: ret}
            else:
                return {key: _select_from_choices(ret, choices)}
        else:
            return {key: default}

    for key in ('url', 'auth_mechanism'):
        update_hostcfg(prompt(key))
    authm = hostcfg()['auth_mechanism']
    _print_err("Selected authentication mechanism: {}".format(authm))
    if authm in (None, 'none'):
        pass
    elif authm == 'SCRAM-SHA-1':
        for key in ('username', 'password'):
            update_hostcfg(prompt(key))
    elif authm in ('SSL', 'SSL-x509'):
        _print_err(
            "Warning: SSL authentication is currently not fully supported!")
        for key in ('ssl_keyfile', 'ssl_certfile',
                    'ssl_cert_reqs', 'ssl_ca_certs'):
            update_hostcfg(prompt(key))
    else:
        raise ValueError("Unsupported auth mechanism '{}'.".format(authm))


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
        help="The name of the host to configure.")
    parser_host.add_argument(
        '--update-pw',
        action='store_true',
        help="Update the password!")
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
    try:
        args.func(args)
    except AttributeError:
        raise
        parser.print_usage()
        sys.exit(2)
    except KeyboardInterrupt:
        _print_err("Interrupted.")
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
