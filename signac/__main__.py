# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import sys
import click
import json
import logging
import getpass
import difflib
import errno
from pprint import pformat


from . import Project, get_project, init_project, index
from .version import __version__
from .common.crypt import get_crypt_context, parse_pwhash, get_keyring
from .contrib.utility import query_yes_no, prompt_password, add_verbosity_argument
from .contrib.filterparse import parse_filter_arg

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


def _read_index(project, fn_index=None):
    if fn_index is not None:
        _print_err("Reading index from file '{}'...".format(fn_index))
        fd = open(fn_index)
        return (json.loads(l) for l in fd)


def transform_option(opt):
    if len(opt) < 1:
        return None
    if opt[0] is None:
        return list()
    return list(opt)


def find_with_filter(**kwargs):
    if getattr(kwargs, 'job_id', None):
        if kwargs['filter'] or kwargs['doc_filter']:
            raise ValueError("Can't provide both 'job-id' and filter arguments!")
        else:
            return kwargs['job_id']

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
                if self._nargs == '+':
                    raise click.ClickException(
                            'ERROR: {} option requires an argument'.format('/'.join(self.opts)))
                value = [self._const if self._nargs == '?' else None]
            value = tuple(value)

            # call the actual process
            self._previous_parser_process(value, state)

        super(MultipleOptionalArgument, self).add_to_parser(parser, ctx)
        for name in self.opts:
            new_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if new_parser:
                self._multi_parser = new_parser
                self._previous_parser_process = new_parser.process
                new_parser.process = parser_process
                break


@click.group()
@click.version_option(__version__)
@click.option('--debug', '-d', is_flag=True)
@click.option('--verbosity', '-v', 'verbosity', count=True)
@click.pass_context
def main(ctx, debug, verbosity):
    log_level = logging.DEBUG if debug else [
        logging.CRITICAL, logging.ERROR,
        logging.WARNING, logging.INFO,
        logging.MORE, logging.DEBUG][min(verbosity, 5)]
    logging.basicConfig(level=log_level)


@main.command()
@click.argument('project_id', type=click.STRING)
@click.option('--workspace', '-w', type=click.STRING)
def init(project_id, workspace):
    project = init_project(
        name=project_id,
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
@click.argument('filter', nargs=-1, type=click.STRING)
@click.option('-d', '--doc-filter', type=click.STRING, cls=MultipleOptionalArgument, nargs='+')
@click.option('-i', '--index', type=click.STRING)
@click.option('--show', '-s', is_flag=True)
@click.option('--sp', cls=MultipleOptionalArgument, type=click.STRING)
@click.option('--doc', cls=MultipleOptionalArgument, type=click.STRING)
@click.option('-p', '--pretty', cls=MultipleOptionalArgument, type=click.INT, nargs='?', const=3)
@click.option('-1', '--one-line', is_flag=True)
def find(**kwargs):

    for opt in ['filter', 'doc_filter', 'sp', 'doc', 'pretty']:
        kwargs[opt] = transform_option(kwargs[opt])

    # setting default values
    kwargs['pretty'] = 3 if kwargs['pretty'] is None else kwargs['pretty']

    project = get_project()

    len_id = max(6, project.min_len_unique_id())

    # --show = --sp --doc --pretty 3
    # if --sp or --doc are also specified, those subsets of keys will be used

    if kwargs['show']:
        kwargs['sp'] = [] if kwargs['sp'] is None else kwargs['sp']
        kwargs['doc'] = [] if kwargs['doc'] is None else kwargs['doc']

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
