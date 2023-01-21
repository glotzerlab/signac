# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Command line interface (CLI) for signac."""

import argparse
import atexit
import code
import difflib
import errno
import importlib
import json
import logging
import os
import platform
import re
import shutil
import sys
import warnings
from pprint import pformat, pprint
from rlcompleter import Completer

from tqdm import tqdm

try:
    import readline
except ImportError:
    READLINE = False
else:
    READLINE = True

from . import get_project, init_project
from ._config import (
    PROJECT_CONFIG_FN,
    USER_CONFIG_FN,
    _Config,
    _load_config,
    _locate_config_dir,
    _read_config_file,
)
from ._utility import _print_err, _query_yes_no, _safe_relpath
from ._vendor.configobj import Section, flatten_errors
from .diff import diff_jobs
from .errors import (
    DestinationExistsError,
    DocumentSyncConflict,
    FileSyncConflict,
    SchemaSyncConflict,
    SyncConflict,
)
from .filterparse import parse_filter_arg
from .import_export import _SchemaPathEvaluationError, export_jobs
from .sync import DocSync, FileSync
from .version import __version__

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
signac {signac_version} 🎨

Project:\t{path}{job_banner}
Size:\t\t{size}

Interact with the project interface using the "project" or "pr" variable.
Type "help(project)" or "help(signac)" for more information."""


SHELL_BANNER_INTERACTIVE_IMPORT = (
    SHELL_BANNER
    + """

The data from origin '{origin}' has been imported into a temporary project.
Synchronize your project with the temporary project, for example with:

                    project.sync(tmp_project, recursive=True)
"""
)


SHELL_HISTORY_FN = os.sep.join((".signac", "shell_history"))

warnings.simplefilter("default")


def _fmt_bytes(nbytes, suffix="B"):
    """Format number of bytes.

    Adapted from: https://stackoverflow.com/a/1094933
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(nbytes) < 1024.0:
            return f"{nbytes:3.1f} {unit}{suffix}"
        nbytes /= 1024.0
    return f"{nbytes:.1f} Yi{suffix}"


def _open_job_by_id(project, job_id):
    """Attempt to open a job by id and provide user feedback on error."""
    try:
        return project.open_job(id=job_id)
    except KeyError:
        close_matches = difflib.get_close_matches(
            job_id, [job.id[: len(job_id)] for job in project.find_jobs()]
        )
        msg = f"Did not find job corresponding to id '{job_id}'."
        if len(close_matches) == 1:
            msg += f" Did you mean '{close_matches[0]}'?"
        elif len(close_matches) > 1:
            msg += " Did you mean any of [{}]?".format("|".join(close_matches))
        raise KeyError(msg)
    except LookupError:
        n = project.min_len_unique_id()
        raise LookupError(
            "Multiple matches for abbreviated id '{}'. "
            "Use at least {} characters for guaranteed "
            "unique ids.".format(job_id, n)
        )


def _find_with_filter_or_none(args):
    """Return a filtered subset of jobs or None."""
    if args.job_id or args.filter:
        return _find_with_filter(args)
    else:
        return None


def _find_with_filter(args):
    """Return a filtered subset of jobs."""
    if getattr(args, "job_id", None):
        if args.filter:
            raise ValueError("Can't provide both 'job-id' and filter arguments!")
        else:
            return args.job_id

    project = get_project()
    filter_ = parse_filter_arg(args.filter) or {}
    if not filter_:
        filter_ = None
    return project._find_job_ids(filter=filter_)


def main_job(args):
    """Handle job subcommand."""
    project = get_project()
    if args.statepoint == "-":
        sp = input()
    else:
        sp = args.statepoint
    try:
        statepoint = json.loads(sp)
    except ValueError:
        _print_err(f"Error while reading state point: '{sp}'")
        raise
    job = project.open_job(statepoint)
    if args.create:
        job.init()
    if args.path:
        print(job.path)
    else:
        print(job)


def main_statepoint(args):
    """Handle statepoint subcommand."""
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
    """Handle document subcommand."""
    project = get_project()
    for job_id in _find_with_filter(args):
        job = _open_job_by_id(project, job_id)
        if args.pretty:
            pprint(job.document(), depth=args.pretty)
        else:
            print(json.dumps(job.document(), indent=args.indent, sort_keys=args.sort))


def main_remove(args):
    """Handle remove subcommand."""
    project = get_project()
    for job_id in args.job_id:
        job = _open_job_by_id(project, job_id)
        if args.interactive and not _query_yes_no(
            "Are you sure you want to {action} job with id '{job.id}'?".format(
                action="clear" if args.clear else "remove", job=job
            ),
            default="no",
        ):
            continue
        if args.clear:
            job.clear()
            logging.info("Cleared f{job_id}")
        else:
            job.remove()
            logging.info("Removing f{job_id}")


def main_move(args):
    """Handle move subcommand."""
    project = get_project()
    dst_project = get_project(path=args.project)
    for job_id in args.job_id:
        try:
            job = _open_job_by_id(project, job_id)
            job.move(dst_project)
        except DestinationExistsError:
            _print_err(f"Destination already exists: '{job}' in '{dst_project}'.")
        else:
            _print_err(f"Moved '{job}' to '{dst_project}'.")


def main_clone(args):
    """Handle clone subcommand."""
    project = get_project()
    dst_project = get_project(path=args.project)
    for job_id in args.job_id:
        try:
            job = _open_job_by_id(project, job_id)
            dst_project.clone(job)
        except DestinationExistsError:
            _print_err(f"Destination already exists: '{job}' in '{dst_project}'.")
        else:
            _print_err(f"Cloned '{job}' to '{dst_project}'.")


def main_find(args):
    """Handle find subcommand."""
    project = get_project()

    len_id = 6
    if args.one_line:
        # Only get the project's minimum length of unique id if it is needed.
        len_id = max(len_id, project.min_len_unique_id())

    # --show = --sp --doc --pretty 3
    # if --sp or --doc are also specified, those subsets of keys will be used
    if args.show:
        if args.sp is None:
            args.sp = []
        if args.doc is None:
            args.doc = []

    def format_lines(cat, id_, s):
        if args.one_line:
            if isinstance(s, dict):
                s = json.dumps(s, sort_keys=True)
            return f"{id_[:len_id]} {cat}\t{s}"
        else:
            return pformat(s, depth=args.pretty)

    try:
        for job_id in _find_with_filter(args):
            print(job_id)
            job = project.open_job(id=job_id)

            if args.sp is not None:
                statepoint = job.statepoint()
                if len(args.sp) != 0:
                    statepoint = {
                        key: statepoint[key] for key in args.sp if key in statepoint
                    }
                print(format_lines("sp ", job_id, statepoint))

            if args.doc is not None:
                doc = job.document()
                if len(args.doc) != 0:
                    doc = {key: doc[key] for key in args.doc if key in doc}
                print(format_lines("sp ", job_id, doc))
    except OSError as error:
        if error.errno == errno.EPIPE:
            sys.stderr.close()
        else:
            raise


def main_diff(args):
    """Handle diff subcommand."""
    project = get_project()

    jobs = _find_with_filter_or_none(args)
    jobs = (
        (_open_job_by_id(project, job) for job in jobs) if jobs is not None else project
    )

    diff = diff_jobs(*jobs)

    for job_id, statepoint in diff.items():
        print(job_id)
        pprint(statepoint)


def main_view(args):
    """Handle view subcommand."""
    project = get_project()
    project.create_linked_view(
        prefix=args.prefix,
        path=args.path,
        job_ids=_find_with_filter(args),
    )


def main_init(args):
    """Handle init subcommand."""
    init_project(path=os.getcwd())
    _print_err("Initialized project.")


def main_schema(args):
    """Handle schema subcommand."""
    project = get_project()
    print(
        project.detect_schema(
            exclude_const=args.exclude_const, subset=_find_with_filter_or_none(args)
        ).format(
            depth=args.depth, precision=args.precision, max_num_range=args.max_num_range
        )
    )


def main_sync(args):
    """Handle sync subcommand."""
    # Validate provided argument combinations
    if args.archive:
        args.recursive = True
        args.links = True
        args.times = True
        args.perms = True
        args.owner = True
        args.group = True

    if args.update:
        if args.strategy is not None:
            raise ValueError(
                "Can't provide both the '-u/--update' and a '-s/--strategy argument!"
            )
        args.strategy = "update"

    if args.times and not args.perms:
        raise NotImplementedError(
            "The '-t/--times' option can only be used in combination with the "
            "'-p/--perms argument."
        )

    if args.size_only or args.round_times:
        # Apply monkey patch
        import filecmp
        import stat

        if args.size_only:

            def _sig(st):
                return (stat.S_IFMT(st.st_mode), st.st_size)

        else:

            def _sig(st):
                return (stat.S_IFMT(st.st_mode), st.st_size, int(st.st_mtime))

        filecmp._sig = _sig

    # Setup synchronization process

    source = get_project(path=args.source)
    try:
        destination = get_project(path=args.destination)
    except LookupError:
        _print_err("WARNING: The destination does not appear to be a project path.")
        raise
    selection = _find_with_filter_or_none(args)

    if args.strategy:
        if args.strategy[0].isupper():
            strategy = getattr(FileSync, args.strategy)()
        else:
            strategy = getattr(FileSync, args.strategy)
    else:
        strategy = None

    if sum((args.all_keys, args.no_keys, args.key is not None)) > 1:
        raise ValueError("You can only provide one key argument!")
    elif args.all_keys:
        doc_sync = DocSync.ByKey(lambda key: True)
    elif args.no_keys:
        doc_sync = DocSync.ByKey(lambda key: False)
    elif args.key:
        try:
            re.compile(args.key)
        except re.error as e:
            raise RuntimeError(f"Illegal regular expression '{args.key}': '{e}'.")
        doc_sync = DocSync.ByKey(lambda key: re.match(args.key, key))
    else:
        doc_sync = DocSync.ByKey()

    try:
        _print_err(f"Synchronizing '{source}' -> '{destination}'...")
        stats = destination.sync(
            other=source,
            strategy=strategy,
            recursive=args.recursive,
            follow_symlinks=not args.links,
            preserve_permissions=args.perms,
            preserve_times=args.times,
            preserve_owner=args.owner,
            preserve_group=args.group,
            exclude=args.exclude,
            doc_sync=doc_sync,
            selection=selection,
            check_schema=not (args.merge or args.force),
            dry_run=args.dry_run,
            parallel=args.parallel,
            deep=args.deep,
            collect_stats=args.stats,
        )
        if stats is not None:
            if args.human_readable:
                stats = stats._replace(volume=_fmt_bytes(stats.volume))
            print("\n# Transfer statistics", "(dry run)" if args.dry_run else "")
            if args.json:
                print(json.dumps(stats._asdict()))
            else:
                print(MSG_SYNC_STATS.format(stats=stats))
    except SchemaSyncConflict as error:
        _print_err(
            "Synchronizing two projects with different schema requires the -m/--merge option."
        )
        diff_src = error.schema_src.difference(error.schema_dst)
        diff_dst = error.schema_dst.difference(error.schema_src)
        only_in_dst = diff_dst.difference(diff_src)
        only_in_src = diff_src.difference(diff_dst)
        diff_value = diff_src.intersection(diff_dst)
        if only_in_src:
            _print_err(
                "Keys found only in the source schema: {}".format(
                    ", ".join(only_in_src)
                )
            )
        if only_in_dst:
            _print_err(
                "Keys found only in the destination schema: {}".format(
                    ", ".join(only_in_dst)
                )
            )
        if diff_value:
            _print_err(
                "Keys having different values in source and destination: {}".format(
                    ", ".join(diff_value)
                )
            )
    except DocumentSyncConflict as error:
        _print_err(MSG_SYNC_SPECIFY_KEY.format(keys=", ".join(error.keys)))
    except FileSyncConflict as error:
        _print_err(MSG_SYNC_FILE_CONFLICT.format(files=error))
    else:
        if doc_sync.skipped_keys:
            _print_err("Skipped key(s):", ", ".join(sorted(doc_sync.skipped_keys)))
        _print_err("Done.")
        return
    raise RuntimeWarning("Synchronization aborted.")


def _main_import_interactive(project, origin, args):
    from .import_export import _prepare_import_into_project

    if args.move:
        raise ValueError(
            "Cannot use '--move' in combination with '--sync-interactive'."
        )

    with project.temporary_project() as tmp_project:
        _print_err("Prepare data space for import...")
        with _prepare_import_into_project(
            origin, tmp_project, args.schema_path
        ) as data_mapping:
            paths = {}
            for src, copy_executor in tqdm(
                dict(data_mapping).items(), desc="Import to temporary project"
            ):
                paths[src] = copy_executor()

            local_ns = dict(
                signac=importlib.import_module(__package__),
                project=project,
                pr=project,
                tmp_project=tmp_project,
            )
            if READLINE:
                readline.set_completer(Completer(local_ns).complete)
                readline.parse_and_bind("tab: complete")
            code.interact(
                local=local_ns,
                banner=SHELL_BANNER_INTERACTIVE_IMPORT.format(
                    python_version=sys.version,
                    signac_version=__version__,
                    job_banner="",
                    path=project.path,
                    size=len(project),
                    origin=args.origin,
                ),
            )

            return paths


def _main_import_non_interactive(project, origin, args):
    from .import_export import _prepare_import_into_project

    try:
        paths = {}
        if args.sync:
            with project.temporary_project() as tmp_project:
                _print_err("Prepare data space for import...")
                with _prepare_import_into_project(
                    origin, tmp_project, args.schema_path
                ) as mapping:
                    for src, copy_executor in tqdm(
                        dict(mapping).items(), desc="Import to temporary project"
                    ):
                        paths[src] = copy_executor()
                    _print_err("Synchronizing project with temporary project...")
                    project.sync(tmp_project, recursive=True)
        else:
            _print_err("Prepare data space for import...")
            with _prepare_import_into_project(
                origin, project, args.schema_path
            ) as data_mapping:
                for src, copy_executor in tqdm(dict(data_mapping).items(), "Importing"):
                    paths[src] = copy_executor(
                        copytree=shutil.move if args.move else None
                    )
    except DestinationExistsError as error:
        _print_err(f"Destination '{error.destination}' already exists.")
        if not args.sync:
            _print_err("Consider using '--sync' or '--sync-interactive'!")
    except SyncConflict as error:
        _print_err(f"Synchronization failed with error: {error}")
        _print_err("Consider using '--sync-interactive'!")
    else:
        return paths


def main_import(args):
    """Handle import subcommand."""
    if args.move and os.path.isfile(args.origin):
        raise ValueError("Cannot use '--move' when importing from a file.")
    if args.move and (args.sync or args.sync_interactive):
        raise ValueError(
            "Cannot use '--move' in combination with '--sync' or '--sync-interactive'."
        )

    project = get_project()
    if args.sync_interactive:
        paths = _main_import_interactive(project, args.origin, args)
    else:
        paths = _main_import_non_interactive(project, args.origin, args)

    if paths is None:
        _print_err("Import failed.")
    elif len(paths):
        _print_err(f"Imported {len(paths)} job(s).")
    elif paths is not None:
        _print_err("Nothing to import.")


def main_export(args):
    """Handle export subcommand."""
    if args.move and os.path.splitext(args.target)[1] != "":
        raise RuntimeError(
            "The '--move' argument can only be used when exporting to directories."
        )
    copytree = shutil.move if args.move else None

    project = get_project()
    jobs = [project.open_job(id=job_id) for job_id in _find_with_filter(args)]

    paths = {}
    with tqdm(total=len(jobs), desc="Export") as pbar:
        try:
            for src, dst in export_jobs(
                jobs=jobs, target=args.target, path=args.schema_path, copytree=copytree
            ):
                paths[src] = dst
                pbar.update(1)
        except _SchemaPathEvaluationError as error:
            raise RuntimeWarning(
                f"An error occurred while evaluating the schema path: {error}"
            )

    if paths:
        _print_err(f"Exported {len(paths)} job(s).")
    else:
        _print_err("No jobs to export.")


def main_update_cache(args):
    """Handle update-cache subcommand."""
    project = get_project()
    _print_err("Updating cache...")
    n = project.update_cache()
    if n is None:
        _print_err("Cache is up to date.")
    else:
        _print_err(f"Updated cache (size={n}).")


def main_migrate(args):
    """Migrate the project's schema to the current schema version."""
    from .migration import _get_config_schema_version, apply_migrations
    from .version import SCHEMA_VERSION

    root = args.root_directory if args.root_directory else os.getcwd()

    schema_version = int(SCHEMA_VERSION)
    config_schema_version = _get_config_schema_version(root, schema_version)

    if config_schema_version > schema_version:
        _print_err(
            f"The schema version of the project ({config_schema_version}) is "
            "newer than the schema version {schema_version} supported by "
            f"signac version {__version__}. Try updating signac."
        )
    elif config_schema_version == schema_version:
        _print_err(
            f"The schema version of the project ({config_schema_version}) is "
            "up to date. Nothing to do."
        )
    elif args.yes or _query_yes_no(
        "Do you want to migrate this project's schema version from "
        f"'{config_schema_version}' to '{schema_version}'? WARNING: THIS "
        "PROCESS IS IRREVERSIBLE!",
        "no",
    ):
        apply_migrations(root)


def main_config_show(args):
    """Handle config show subcommand."""
    cfg = None
    if args.local and args.globalcfg:
        raise ValueError("You can specify either -l/--local or -g/--global, not both.")
    elif args.local:
        if os.path.isfile(PROJECT_CONFIG_FN):
            cfg = _read_config_file(PROJECT_CONFIG_FN)
    elif args.globalcfg:
        cfg = _read_config_file(USER_CONFIG_FN)
    else:
        cfg = _load_config(_locate_config_dir(os.getcwd()))
    if not cfg:
        if args.local:
            mode = "local"
        elif args.globalcfg:
            mode = "global"
        else:
            mode = "local or global"
        _print_err(f"Did not find a {mode} configuration file.")
        return
    for key in args.key:
        for kt in key.split("."):
            cfg = cfg.get(kt)
            if cfg is None:
                break
    if not isinstance(cfg, Section):
        print(cfg)
    else:
        for line in _Config(cfg).write():
            print(line)


def _verify_config(cfg, preserve_errors=True):
    """Verify provided configuration."""
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
                section_list.append("[missing section]")
            section_string = ".".join(section_list)
            if error is False:
                error = "Possibly invalid or missing."
            else:
                error = type(error).__name__
            _print_err(" ".join((section_string, ":", error)))


def main_config_verify(args):
    """Handle config verify subcommand."""
    cfg = None
    if args.local and args.globalcfg:
        raise ValueError("You can specify either -l/--local or -g/--global, not both.")
    elif args.local:
        if os.path.isfile(PROJECT_CONFIG_FN):
            cfg = _read_config_file(PROJECT_CONFIG_FN)
    elif args.globalcfg:
        cfg = _read_config_file(USER_CONFIG_FN)
    else:
        cfg = _load_config(_locate_config_dir(os.getcwd()))
    if not cfg:
        if args.local:
            mode = "local"
        elif args.globalcfg:
            mode = "global"
        else:
            mode = "local or global"
        _print_err(f"Did not find a {mode} configuration file.")
        return
    else:
        if cfg.filename is not None:
            _print_err(f"Verification of config file '{cfg.filename}'.")
        _verify_config(cfg)


def main_config_set(args):
    """Handle config set subcommand."""
    if not (args.local or args.globalcfg):
        args.local = True
    fn_config = None
    if args.local and args.globalcfg:
        raise ValueError("You can specify either -l/--local or -g/--global, not both.")
    elif args.local:
        if os.path.isfile(PROJECT_CONFIG_FN):
            fn_config = PROJECT_CONFIG_FN
    elif args.globalcfg:
        fn_config = USER_CONFIG_FN
    else:
        raise ValueError(
            "You need to specify either -l/--local or -g/--global "
            "to specify which configuration to modify."
        )
    cfg = _read_config_file(fn_config)
    keys = args.key.split(".")
    if len(args.value) == 0:
        raise ValueError("No value argument provided!")
    elif len(args.value) == 1:
        args.value = args.value[0]
    sec = cfg
    for key in keys[:-1]:
        sec = sec.setdefault(key, {})
    try:
        sec[keys[-1]] = args.value
        _print_err(f"Updated value '{args.key}'='{args.value}'.")
    except TypeError:
        raise KeyError(args.key)
    _print_err(f"Writing configuration to '{os.path.abspath(fn_config)}'.")
    cfg.write()


def main_shell(args):
    """Handle shell subcommand."""
    if args.file and args.command:
        raise ValueError(
            "Cannot provide file and -c/--command argument at the same time!"
        )

    try:
        project = get_project()
    except LookupError:
        print("signac", __version__)
        print("No project within this directory.")
        print("If you want to initialize a project, execute `$ signac init`.")
    else:
        _jobs = _find_with_filter(args)

        def jobs():
            for id_ in _jobs:
                yield project.open_job(id=id_)

        if len(_jobs) == 1:
            job = _open_job_by_id(project, list(_jobs)[0])
        else:
            try:
                job = project.get_job()
            except LookupError:
                job = None

        local_ns = dict(
            project=project,
            pr=project,
            jobs=iter(jobs()),
            job=job,
            signac=sys.modules["signac"],
        )

        if args.file or args.command:
            interpreter = code.InteractiveInterpreter(locals=local_ns)
            if args.file and args.file == "-":
                try:
                    while True:
                        interpreter.runsource(
                            input(), filename="<input>", symbol="exec"
                        )
                except EOFError:
                    pass
            elif args.file:
                with open(args.file) as file:
                    interpreter.runsource(
                        file.read(), filename=args.file, symbol="exec"
                    )
            else:
                interpreter.runsource(args.command, filename="<input>", symbol="exec")
        else:  # interactive
            if READLINE:
                if "PyPy" not in platform.python_implementation():
                    fn_hist = project.fn(SHELL_HISTORY_FN)
                    try:
                        readline.read_history_file(fn_hist)
                        readline.set_history_length(1000)
                    except FileNotFoundError:
                        pass
                    except PermissionError:
                        print(
                            "Warning: Shell history could not be read from "
                            "{}.".format(_safe_relpath(fn_hist))
                        )

                    def write_history_file():
                        try:
                            readline.write_history_file(fn_hist)
                        except PermissionError:
                            print(
                                "Warning: Shell history could not be written to "
                                "{}.".format(_safe_relpath(fn_hist))
                            )

                    atexit.register(write_history_file)
                readline.set_completer(Completer(local_ns).complete)
                readline.parse_and_bind("tab: complete")
            code.interact(
                local=local_ns,
                banner=SHELL_BANNER.format(
                    python_version=sys.version,
                    signac_version=__version__,
                    job_banner=f"\nJob:\t\t{job.id}" if job is not None else "",
                    path=project.path,
                    size=len(project),
                ),
            )


def main():
    """Provide command line interface."""
    parser = argparse.ArgumentParser(
        description="signac aids in the management, access and analysis of "
        "large-scale computational investigations."
    )
    parser.add_argument(
        "--debug", action="store_true", help="Show traceback on error for debugging."
    )
    parser.add_argument(
        "--version", action="store_true", help="Display the version number and exit."
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        help="Set level of verbosity.",
        action="count",
        default=2,
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Answer all questions with yes. Useful for scripted interaction.",
    )
    subparsers = parser.add_subparsers()

    parser_init = subparsers.add_parser("init")
    parser_init.set_defaults(func=main_init)

    parser_job = subparsers.add_parser("job")
    parser_job.add_argument(
        "statepoint",
        nargs="?",
        default="-",
        type=str,
        help="The job's state point in JSON format. Omit this argument to read from STDIN.",
    )
    parser_job.add_argument(
        "-w",
        "--workspace",
        action="store_true",
        help="Print the job's workspace path instead of the job id.",
    )
    parser_job.add_argument(
        "-p",
        "--path",
        action="store_true",
        help="Print the job's path instead of the job id.",
    )
    parser_job.add_argument(
        "-c",
        "--create",
        action="store_true",
        help="Create the job directory if necessary.",
    )
    parser_job.set_defaults(func=main_job)

    parser_statepoint = subparsers.add_parser(
        "statepoint",
        description="Print the state point(s) corresponding to one or more job ids.",
    )
    parser_statepoint.add_argument(
        "job_id",
        nargs="*",
        type=str,
        help="One or more job ids. The corresponding jobs must be initialized.",
    )
    parser_statepoint.add_argument(
        "-p",
        "--pretty",
        type=int,
        nargs="?",
        const=3,
        help="Print state point in pretty format. "
        "An optional argument to this flag specifies the maximal "
        "depth a state point is printed.",
    )
    parser_statepoint.add_argument(
        "-i",
        "--indent",
        type=int,
        nargs="?",
        const="2",
        help="Specify the indentation of the JSON formatted state point.",
    )
    parser_statepoint.add_argument(
        "-s",
        "--sort",
        action="store_true",
        help="Sort the state point keys for output.",
    )
    parser_statepoint.set_defaults(func=main_statepoint)

    parser_diff = subparsers.add_parser(
        "diff", description="Find the difference among job state points."
    )
    parser_diff.add_argument(
        "job_id",
        nargs="*",
        type=str,
        help="One or more job ids. The corresponding jobs must be initialized.",
    )
    parser_diff.add_argument(
        "-p",
        "--pretty",
        type=int,
        nargs="?",
        const=3,
        help="Print state point in pretty format. "
        "An optional argument to this flag specifies the maximal "
        "depth a state point is printed.",
    )
    parser_diff.add_argument(
        "-i",
        "--indent",
        type=int,
        nargs="?",
        const="2",
        help="Specify the indentation of the JSON formatted state point.",
    )
    parser_diff.add_argument(
        "-f",
        "--filter",
        type=str,
        nargs="+",
        help="Limit the diff to jobs matching the filter.",
    )
    parser_diff.set_defaults(func=main_diff)

    parser_document = subparsers.add_parser(
        "document",
        description="Print the document(s) corresponding to one or more job ids.",
    )
    parser_document.add_argument(
        "job_id",
        nargs="*",
        type=str,
        help="One or more job ids. The job corresponding to a job id must be initialized.",
    )
    parser_document.add_argument(
        "-p",
        "--pretty",
        type=int,
        nargs="?",
        const=3,
        help="Print document in pretty format. "
        "An optional argument to this flag specifies the maximal "
        "depth a document is printed.",
    )
    parser_document.add_argument(
        "-i",
        "--indent",
        type=int,
        nargs="?",
        const="2",
        help="Specify the indentation of the JSON formatted state point.",
    )
    parser_document.add_argument(
        "-s",
        "--sort",
        action="store_true",
        help="Sort the document keys for output in JSON format.",
    )
    parser_document.add_argument(
        "-f",
        "--filter",
        type=str,
        nargs="+",
        help="Show documents of jobs matching the filter.",
    )
    parser_document.set_defaults(func=main_document)

    parser_remove = subparsers.add_parser("rm")
    parser_remove.add_argument(
        "job_id", type=str, nargs="+", help="One or more job ids of jobs to remove."
    )
    parser_remove.add_argument(
        "-c",
        "--clear",
        action="store_true",
        help="Do not completely remove, but only clear the job.",
    )
    parser_remove.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Request confirmation before attempting to remove/clear each job.",
    )
    parser_remove.set_defaults(func=main_remove)

    parser_move = subparsers.add_parser("move")
    parser_move.add_argument(
        "project",
        type=str,
        help="The destination project directory.",
    )
    parser_move.add_argument(
        "job_id",
        nargs="+",
        type=str,
        help="One or more job ids. The corresponding jobs must be initialized.",
    )
    parser_move.set_defaults(func=main_move)

    parser_clone = subparsers.add_parser("clone")
    parser_clone.add_argument(
        "project",
        type=str,
        help="The directory of the project to clone one or more jobs in.",
    )
    parser_clone.add_argument(
        "job_id",
        nargs="+",
        type=str,
        help="One or more job ids. The corresponding jobs must be initialized.",
    )
    parser_clone.set_defaults(func=main_clone)

    parser_find = subparsers.add_parser(
        "find",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="All filter arguments may be provided either directly in "
        "JSON encoding or in a simplified form using the query API. For "
        "example, the following two bash commands are equivalent:\n\n"
        "$ signac find key 42\n"
        "$ signac find '{\"a\": 42}'\n\n"
        "For the full syntax, see:\n"
        "https://docs.signac.io/en/latest/query.html#simplified-filter",
    )
    parser_find.add_argument(
        "filter",
        type=str,
        nargs="*",
        help="Find jobs matching the filter.",
    )
    parser_find.add_argument(
        "-s",
        "--show",
        type=int,
        nargs="?",
        const=3,
        help="Show the state point and document of each job. Equivalent to "
        "--sp --doc --pretty 3.",
    )
    parser_find.add_argument(
        "--sp",
        type=str,
        nargs="*",
        help="Show the state point of each job. Can be passed the list of "
        "state point keys to print (if they exist for a given job).",
    )
    parser_find.add_argument(
        "--doc",
        type=str,
        nargs="*",
        help="Show the document of each job. Can be passed the list of "
        "document keys to print (if they exist for a given job).",
    )
    parser_find.add_argument(
        "-p",
        "--pretty",
        type=int,
        nargs="?",
        const=3,
        default=3,
        help="Pretty print output when using --sp, --doc, or ---show. "
        "Argument is the depth to which keys are printed.",
    )
    parser_find.add_argument(
        "-1",
        "--one-line",
        action="store_true",
        help="Print output in JSON and on one line.",
    )
    parser_find.set_defaults(func=main_find)

    parser_view = subparsers.add_parser(
        "view",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Generate a human readable set of paths representing "
        "state points in the workspace, e.g. "
        "view/param_name_1/param_value_1/param_name_2/param_value_2/job.\n\n"
        "The leaf nodes of this directory structure are symlinks (named "
        '"job") into the corresponding job directory.\n\n'
        "The jobs can be filtered using the query API used by the find "
        "command. For the full syntax, see:\n"
        "https://docs.signac.io/en/latest/query.html#simplified-filter\n\n"
        "All positional arguments must be provided before any keyword "
        "arguments. In particular, the prefix and path must be "
        "specified before arguments such as the filters, like in the "
        "following bash command:\n\n"
        "$ signac view $PREFIX $VIEW_PATH -f $FILTERS -d $DOC_FILTERS.",
    )
    parser_view.add_argument(
        "-p",
        "--prefix",
        type=str,
        nargs="?",
        default="view",
        help="The path where the view is to be created. Defaults to view.",
    )
    parser_view.add_argument(
        "path",
        type=str,
        nargs="?",
        default="{{auto}}",
        help="The path used for the generation of the linked view hierarchy, "
        "defaults to '{{auto}}' (see Project.export_to for information "
        "on how this is expanded).",
    )
    selection_group = parser_view.add_argument_group("select")
    selection_group.add_argument(
        "-f",
        "--filter",
        type=str,
        nargs="+",
        help="Limit the view to jobs matching the filter.",
    )
    selection_group.add_argument(
        "-j",
        "--job-id",
        type=str,
        nargs="+",
        help="Limit the view to jobs with these job ids.",
    )
    parser_view.set_defaults(func=main_view)

    parser_schema = subparsers.add_parser("schema")
    parser_schema.add_argument(
        "-x",
        "--exclude-const",
        action="store_true",
        help="Exclude state point parameters, which are constant over the "
        "complete project data space.",
    )
    parser_schema.add_argument(
        "-t",
        "--depth",
        type=int,
        default=0,
        help="A non-zero value will format the schema in a nested representation "
        "up to the specified depth. The default is a flat view (depth=0).",
    )
    parser_schema.add_argument(
        "-p",
        "--precision",
        type=int,
        help="Round all numerical values up to the given precision.",
    )
    parser_schema.add_argument(
        "-r",
        "--max-num-range",
        type=int,
        default=5,
        help="The maximum number of entries shown for a value range, defaults to 5.",
    )
    selection_group = parser_schema.add_argument_group("select")
    selection_group.add_argument(
        "-f",
        "--filter",
        type=str,
        nargs="+",
        help="Detect schema only for jobs matching the filter.",
    )
    selection_group.add_argument(
        "-j",
        "--job-id",
        type=str,
        nargs="+",
        help="Detect schema only for jobs with the given job ids.",
    )
    parser_schema.set_defaults(func=main_schema)

    parser_shell = subparsers.add_parser("shell")
    parser_shell.add_argument(
        "file", type=str, nargs="?", help="Execute Python script in file."
    )
    parser_shell.add_argument(
        "-c", "--command", type=str, help="Execute Python program passed as string."
    )
    selection_group = parser_shell.add_argument_group(
        "select",
        description="Specify one or more jobs to preset the `jobs` variable as a generator "
        "over all job handles associated with the given selection. If the selection "
        "contains only one job, an additional `job` variable is referencing that "
        "single job, otherwise it is `None`.",
    )
    selection_group.add_argument(
        "-f",
        "--filter",
        type=str,
        nargs="+",
        help="Reduce selection to jobs matching the filter.",
    )
    selection_group.add_argument(
        "-j",
        "--job-id",
        type=str,
        nargs="+",
        help="Reduce selection to jobs matching the given job ids.",
    )
    parser_shell.set_defaults(func=main_shell)

    parser_sync = subparsers.add_parser(
        "sync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Use this command to synchronize this project with "
        "another project, similar to the synchronization of two directories "
        "with `rsync`. Data is always copied from the source to the "
        "destination.\n\n"
        "For example, the following bash command will synchronize all jobs "
        "within this project with jobs in the other project, overwrite files "
        "if the source files are newer, and overwrite all conflicting keys in "
        "the project and job documents:\n\n"
        "$ signac sync /path/to/other/project --update --all-keys",
    )
    parser_sync.add_argument(
        "source",
        help="The directory of the project that this project should be synchronized with.",
    )
    parser_sync.add_argument(
        "destination",
        nargs="?",
        help="Optional: The directory of the project that should be modified for "
        "synchronization, defaults to the local project.",
    )

    sync_group = parser_sync.add_argument_group("copy options")
    sync_group.add_argument(
        "-a",
        "--archive",
        action="store_true",
        help="archive mode; equivalent to: '-rltpog'",
    )
    sync_group.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Do not skip sub-directories, but synchronize recursively.",
    )
    sync_group.add_argument(
        "-l",
        "--links",
        action="store_true",
        help="Copy symbolic links as symbolic links pointing to the original source.",
    )
    sync_group.add_argument(
        "-p", "--perms", action="store_true", help="Preserve permissions."
    )
    sync_group.add_argument(
        "-o", "--owner", action="store_true", help="Preserve owner."
    )
    sync_group.add_argument(
        "-g", "--group", action="store_true", help="Preserve group."
    )
    sync_group.add_argument(
        "-t",
        "--times",
        action="store_true",
        help="Preserve file modification times (requires -p).",
    )
    sync_group.add_argument(
        "-x",
        "--exclude",
        type=str,
        nargs="?",
        const=".*",
        help="Exclude all files matching the given pattern. Exclude all files "
        "if this option is provided without any argument.",
    )
    sync_group.add_argument(
        "-I",
        "--ignore-times",
        action="store_true",
        dest="deep",
        help="Never rely on file metadata such as the size or the modification time "
        "when determining file differences.",
    )
    sync_group.add_argument(
        "--size-only",
        action="store_true",
        help="Ignore modification times during file comparison. Useful when synchronizing "
        "between file systems with different timestamp resolution.",
    )
    sync_group.add_argument(
        "--round-times",
        action="store_true",
        help="Round modification times during file comparison. Useful when synchronizing "
        "between file systems with different timestamp resolution.",
    )
    sync_group.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Do not actually execute the synchronization. Increase the output verbosity "
        "to see messages about what would potentially happen.",
    )
    sync_group.add_argument(
        "-u",
        "--update",
        action="store_true",
        help="Skip files with newer modification time stamp."
        "This is a short-cut for: --strategy=update.",
    )

    strategy_group = parser_sync.add_argument_group("sync strategy")
    strategy_group.add_argument(
        "-s",
        "--strategy",
        type=str,
        choices=FileSync.keys(),
        help="Specify a synchronization strategy, for differing files.",
    )
    strategy_group.add_argument(
        "-k",
        "--key",
        type=str,
        help="Specify a regular expression for keys that should be overwritten "
        "as part of the project and job document synchronization.",
    )
    strategy_group.add_argument(
        "--all-keys",
        action="store_true",
        help="Overwrite all conflicting keys. Equivalent to `--key='.*'`.",
    )
    strategy_group.add_argument(
        "--no-keys", action="store_true", help="Never overwrite any conflicting keys."
    )

    parser_sync.add_argument(
        "--force", action="store_true", help="Ignore all warnings, just synchronize."
    )
    parser_sync.add_argument(
        "-m",
        "--merge",
        action="store_true",
        help="Clone all the jobs that are not present in destination from source.",
    )
    parser_sync.add_argument(
        "--parallel",
        type=int,
        nargs="?",
        const=True,
        help="Use multiple threads for synchronization."
        "You may optionally specify how many threads to "
        "use, otherwise all available processing units will be utilized.",
    )
    parser_sync.add_argument(
        "--stats", action="store_true", help="Provide file transfer statistics."
    )
    parser_sync.add_argument(
        "-H",
        "--human-readable",
        action="store_true",
        help="Provide statistics with human readable formatting.",
    )
    parser_sync.add_argument(
        "--json", action="store_true", help="Print statistics in JSON formatting."
    )

    selection_group = parser_sync.add_argument_group("select")
    selection_group.add_argument(
        "-f",
        "--filter",
        type=str,
        nargs="+",
        help="Only synchronize jobs matching the filter.",
    )
    selection_group.add_argument(
        "-j",
        "--job-id",
        type=str,
        nargs="+",
        help="Only synchronize jobs with the given job ids.",
    )
    parser_sync.set_defaults(func=main_sync)

    parser_import = subparsers.add_parser(
        "import",
        description="""Import an existing dataset into this project. Optionally provide a file path
 based schema to specify the state point metadata. Providing a path based schema is only necessary
 if the data set was not previously exported from a signac project.""",
    )
    parser_import.add_argument(
        "origin",
        default=".",
        nargs="?",
        help="The origin to import from. May be a path to a directory, a zipfile, or a tarball. "
        "Defaults to the current working directory.",
    )
    parser_import.add_argument(
        "schema_path",
        nargs="?",
        help="Specify an optional import path, such as 'foo/{foo:int}'. Possible type definitions "
        "include bool, int, float, and str. The type is assumed to be 'str' if no type is "
        "specified.",
    )
    parser_import.add_argument(
        "--move",
        action="store_true",
        help="Move the data upon import instead of copying. Can only be used when importing from "
        "a directory.",
    )
    parser_import.add_argument(
        "--sync",
        action="store_true",
        help="Attempt recursive synchronization with default arguments.",
    )
    parser_import.add_argument(
        "--sync-interactive",
        action="store_true",
        help="Synchronize the project with the origin data space interactively.",
    )
    parser_import.set_defaults(func=main_import)

    parser_export = subparsers.add_parser(
        "export",
        description="""Export the project data space (or a subset) to a directory, a zipfile,
 or a tarball.""",
    )
    parser_export.add_argument(
        "target",
        help="The target to export to. May be a path to a directory, a zipfile, or a tarball.",
    )
    parser_export.add_argument(
        "schema_path",
        nargs="?",
        help="Specify an optional export path, based on the job state point, e.g., "
        "'foo/{job.sp.foo}'.",
    )
    parser_export.add_argument(
        "--move",
        action="store_true",
        help="Move data to export target instead of copying. Can only be used when exporting "
        "to a directory target.",
    )
    selection_group = parser_export.add_argument_group("select")
    selection_group.add_argument(
        "-f",
        "--filter",
        type=str,
        nargs="+",
        help="Limit the jobs to export to those matching the filter.",
    )
    selection_group.add_argument(
        "-j",
        "--job-id",
        type=str,
        nargs="+",
        help="Limit the jobs to export to those matching the provided job ids.",
    )
    parser_export.set_defaults(func=main_export)

    parser_update_cache = subparsers.add_parser(
        "update-cache",
        description="Use this command to update the project's persistent state point cache.",
    )
    parser_update_cache.set_defaults(func=main_update_cache)

    parser_config = subparsers.add_parser("config")
    parser_config.add_argument(
        "-g",
        "--global",
        dest="globalcfg",
        action="store_true",
        help="Modify the global configuration.",
    )
    parser_config.add_argument(
        "-l", "--local", action="store_true", help="Modify the local configuration."
    )
    parser_config.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Skip sanity checks when modifying the configuration.",
    )
    config_subparsers = parser_config.add_subparsers()

    parser_show = config_subparsers.add_parser("show")
    parser_show.add_argument(
        "key",
        type=str,
        nargs="*",
        help="The key(s) to show, omit to show the full configuration.",
    )
    parser_show.set_defaults(func=main_config_show)

    parser_set = config_subparsers.add_parser("set")
    parser_set.add_argument("key", type=str, help="The key to modify.")
    parser_set.add_argument(
        "value", type=str, nargs="*", help="The value to set key to."
    )
    parser_set.add_argument(
        "-f", "--force", action="store_true", help="Override any validation warnings."
    )
    parser_set.set_defaults(func=main_config_set)

    parser_verify = config_subparsers.add_parser("verify")
    parser_verify.set_defaults(func=main_config_verify)

    parser_migrate = subparsers.add_parser(
        "migrate",
        description="Irreversibly migrate this project's schema version to the "
        "supported version.",
    )
    parser_migrate.add_argument(
        "-r",
        "--root-directory",
        type=str,
        default="",
        help="The path to the project.",
    )
    parser_migrate.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Do not ask for confirmation.",
    )
    parser_migrate.set_defaults(func=main_migrate)

    # This is a hack, as argparse itself does not
    # allow to parse only --version without any
    # of the other required arguments.
    if "--version" in sys.argv:
        print("signac", __version__)
        sys.exit(0)

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        log_level = (
            logging.DEBUG
            if args.debug
            else [
                logging.CRITICAL,
                logging.ERROR,
                logging.WARNING,
                logging.INFO,
                logging.MORE,
                logging.DEBUG,
            ][min(args.verbosity, 5)]
        )
        logging.basicConfig(level=log_level)

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(2)
    try:
        args.func(args)
    except KeyboardInterrupt:
        _print_err("Interrupted.")
        if args.debug:
            raise
        sys.exit(1)
    except RuntimeWarning as warning:
        _print_err(f"Warning: {warning}")
        if args.debug:
            raise
        sys.exit(1)
    except Exception as error:
        _print_err(f"Error: {error}")
        if args.debug:
            raise
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
