# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import sys
import shutil
import click
import json
import logging
import getpass
import difflib
import atexit
import code
import importlib
import platform
from rlcompleter import Completer
import re
import errno
from pprint import pprint, pformat
from tqdm import tqdm

try:
    import readline
except ImportError:
    READLINE = False
else:
    READLINE = True

from . import Project, get_project, init_project, index
from .version import __version__
from .common import config
from .common.configobj import flatten_errors, Section
from .common.crypt import get_crypt_context, parse_pwhash, get_keyring
from .contrib.utility import query_yes_no, prompt_password, add_verbosity_argument
from .contrib.filterparse import parse_filter_arg
from .contrib.import_export import export_jobs, _SchemaPathEvaluationError
from .errors import DestinationExistsError
from .sync import FileSync
from .sync import DocSync
from .errors import SyncConflict
from .errors import FileSyncConflict
from .errors import DocumentSyncConflict
from .errors import SchemaSyncConflict
from .diff import diff_jobs

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


MSG_SYNC_SPECIFY_KEY = """
Synchronization conflict occurred, no strategy defined to synchronize keys:
{keys}

Use the `-k/--key` option to specify a regular expression pattern matching
all keys that should be overwritten, `--all-keys` to overwrite all conflicting
keys, or `--no-keys` to overwrite none of the conflicting keys."""


MSG_SYNC_FILE_CONFLICT = """
Synchronization conflict occurred, no strategy defined to synchronize files:
{files}

Use the `-s/--strategy` option to specify a file synchronization strategy,
or the `-u/--update` option to overwrite all files which have a more recent
modification time stamp."""


MSG_SYNC_STATS = """
Number of files transferred: {stats.num_files}
Total transfer volume:       {stats.volume}
"""


SHELL_BANNER = """Python {python_version}
signac {signac_version}

Project:\t{project_id}{job_banner}
Root:\t\t{root_path}
Workspace:\t{workspace_path}
Size:\t\t{size}

Interact with the project interface using the "project" or "pr" variable.
Type "help(project)" or "help(signac)" for more information."""


SHELL_BANNER_INTERACTIVE_IMPORT = SHELL_BANNER + """

The data from origin '{origin}' has been imported into a temporary project.
Synchronize your project with the temporary project, for example with:

                    project.sync(tmp_project, recursive=True)
"""


def _print_err(msg=None, *args):
    print(msg, *args, file=sys.stderr)


def _fmt_bytes(nbytes, suffix='B'):
    "Adapted from: https://stackoverflow.com/a/1094933"
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(nbytes) < 1024.0:
            return "%3.1f %s%s" % (nbytes, unit, suffix)
        nbytes /= 1024.0
    return "%.1f %s%s" % (nbytes, 'Yi', suffix)


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
    except KeyError:
        close_matches = difflib.get_close_matches(
            job_id, [jid[:len(job_id)] for jid in project.find_job_ids()])
        msg = "Did not find job corresponding to id '{}'.".format(job_id)
        if len(close_matches) == 1:
            msg += " Did you mean '{}'?".format(close_matches[0])
        elif len(close_matches) > 1:
            msg += " Did you mean any of [{}]?".format('|'.join(close_matches))
        raise KeyError(msg)
    except LookupError:
        n = project.min_len_unique_id()
        raise LookupError("Multiple matches for abbreviated id '{}'. "
                          "Use at least {} characters for guaranteed "
                          "unique ids.".format(job_id, n))

def transform_option(opt):
    if len(opt)<1:
        return None
    if opt[0] is None:
        return list()
    return list(opt)

def find_with_filter_or_none(**kwargs):
    if kwargs['job_id'] or kwargs['filter'] or kwargs['doc_filter']:
        return find_with_filter(**kwargs)


def find_with_filter(**kwargs):
    if getattr(kwargs, 'job_id', None):
        if kwargs['filter'] or kwargs['doc_filter']:
            raise ValueError("Can't provide both 'job-id' and filter arguments!")
        else:
            return kwargs.job_id

    project = get_project()
    if hasattr(kwargs, 'index'):
        index = _read_index(project, kwargs['index'])
    else:
        index = None

    f = parse_filter_arg(kwargs['filter'])
    df = parse_filter_arg(kwargs['doc_filter'])
    return get_project().find_job_ids(index=index, filter=f, doc_filter=df)

class MultipleOptionalArgument(click.Option):

    def __init__(self, *args, **kwargs):
        self._nargs = kwargs.pop('nargs', '*')
        self._const = kwargs.pop('const', None)
        kwargs['nargs'] = 0
        super(MultipleOptionalArgument, self).__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._multi_parser = None

    def add_to_parser(self, parser, ctx):

        def parser_process(value, state):
            # method to hook to the parser.process
            done = False
            value = list(value)
            while state.rargs and not done:
                for prefix in self._multi_parser.prefixes:
                    if state.rargs[0].startswith(prefix):
                        done = True
                if not done:
                    value.append(state.rargs.pop(0))
            if not len(value):
                if self._nargs is '+':
                    raise click.ClickException('ERROR: {} option requires an argument'.format('/'.join(self.opts)))
                value = [ self._const if self._nargs is '?' else None]
            value = tuple(value)

            # call the actual process
            self._previous_parser_process(value, state)
        
        retval = super(MultipleOptionalArgument, self).add_to_parser(parser, ctx)
        
        for name in self.opts:
            new_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if new_parser:
                self._multi_parser = new_parser
                self._previous_parser_process = new_parser.process
                new_parser.process = parser_process
                break
        return retval

@click.group()
@click.version_option(__version__)
@click.option('--debug', is_flag=True)
@click.option('--verbosity','-v', 'verbosity', count=True)
@click.pass_context
def main(ctx, debug, verbosity):
    log_level = logging.DEBUG if debug else [
        logging.CRITICAL, logging.ERROR,
        logging.WARNING, logging.INFO,
        logging.MORE, logging.DEBUG][min(verbosity, 5)]
    logging.basicConfig(level=log_level)
    

@main.command()
@click.argument('project_id', type=click.STRING)
@click.option('--workspace','-w', type=click.STRING)
def init(project_id, workspace):    
    project = init_project(
        name= project_id,
        root=os.getcwd(),
        workspace=workspace)
    _print_err("Initialized project '{}'.".format(project))


@main.command()
@click.option('--workspace', is_flag=True)
@click.option('--access', is_flag=True)
@click.option('--index', is_flag=True)
def project(access, index, workspace):
    project = get_project()
    if access:
        fn = project.create_access_module()
        _print_err("Created access module '{}'.".format(fn))
        return
    if index:
        for doc in project.index():
            print(json.dumps(doc))
        return
    if workspace:
        print(project.workspace())
    else:
        print(project)


@main.command()
@click.argument('filter', nargs= -1, type=click.STRING)
@click.option('-d', '--doc-filter', type=click.STRING, cls=MultipleOptionalArgument, nargs='+')
@click.option('-i', '--index', type=click.STRING)
@click.option('--show', '-s', is_flag=True)
@click.option('--sp', cls=MultipleOptionalArgument,
              type=click.STRING)
@click.option('--doc', cls=MultipleOptionalArgument, type=click.STRING)
@click.option('-p','--pretty', cls=MultipleOptionalArgument,
              type=click.INT, nargs='?', const=3)
@click.option('-1','--one-line', is_flag=True)
def find(**kwargs):
    
    for opt in ['filter', 'doc_filter', 'sp', 'doc', 'pretty']:
        kwargs[opt] = transform_option(kwargs[opt])

    kwargs['pretty'] = 3 if kwargs['pretty'] is None else kwargs['pretty']
    
    project = get_project()

    len_id = max(6, project.min_len_unique_id())

    # --show = --sp --doc --pretty 3
    # if --sp or --doc are also specified, those subsets of keys will be used

    if kwargs['show']:
        if kwargs['sp'] is None:
           kwargs['sp'] = []
        if kwargs['doc'] is None:
            kwargs['doc'] = []


    def format_lines(cat, _id, s):
        if kwargs['one_line']:
            if isinstance(s, dict):
                s = json.dumps(s, sort_keys=True)
            return _id[:len_id] + ' ' + cat + '\t' + s
        else:
            return pformat(s, depth=kwargs['pretty'])

    try:
        for job_id in find_with_filter(**kwargs):
            print(job_id)
            job = project.open_job(id=job_id)

            if kwargs['sp'] is not None:
                sp = job.statepoint()
                if len(kwargs['sp']) != 0:
                    sp = {key: sp[key] for key in kwargs['sp'] if key in sp}
                print(format_lines('sp ', job_id, sp))

            if kwargs['doc'] is not None:
                doc = job.document()
                if len(kwargs['doc']) != 0:
                    doc = {key: doc[key] for key in kwargs['doc'] if key in doc}
                print(format_lines('sp ', job_id, doc))
    except IOError as error:
        if error.errno == errno.EPIPE:
            sys.stderr.close()
        else:
            raise

if __name__ == '__main__':
    main()
