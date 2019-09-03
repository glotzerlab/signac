#!/usr/bin/env python
# Copyright 2018 The Regents of the University of Michigan
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import os
import six
import sys
import string
import random
import timeit
import argparse
import logging
import warnings
import base64
import json
import platform
from pprint import pprint
from cProfile import Profile
from collections import OrderedDict
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from tempfile import gettempdir
from multiprocessing import Pool

import signac
import git
import psutil
import pandas as pd
from tqdm import tqdm
from version import __version__
if six.PY2:
    from backports.tempfile import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


logger = logging.getLogger('signac-benchmark')


COMPLEXITY = {
    'iterate': 'N',
    'iterate_single_pass': 'N',
    'search_lean_filter': 'N',
    'search_rich_filter': 'N',
    'determine_len': 'N',
    'select_by_id': '1',
}


def size(fn):
    try:
        return os.path.getsize(fn)
    except (OSError, IOError):
        return 0


def calc_project_metadata_size(project):
    sp_size = []
    doc_size = []
    for job in tqdm(project, 'determine metadata size'):
        sp_size.append(size(job.fn(job.FN_MANIFEST)))
        doc_size.append(size(job.fn(job.FN_DOCUMENT)))
    return sp_size, doc_size


def get_partition(path):
    path = os.path.realpath(path)
    candidates = dict()
    for partition in psutil.disk_partitions(all=True):
        mp = os.path.realpath(partition.mountpoint)
        if path.startswith(mp):
            candidates[mp] = partition
    if candidates:
        return candidates[list(sorted(candidates, key=len))[-1]]
    else:
        raise LookupError(path)


def create_doc(args):
    tmpdir = gettempdir() if args.root is None else args.root
    if six.PY2:
        platform_doc = dict((k, v) for k, v in
                            zip(('system', 'node', 'release', 'version',
                                 'machine', 'processor'), platform.uname()))
    else:
        platform_doc = platform.uname()._asdict()
    return {'meta': {
        'tool': 'signac',
        'num_keys': args.num_keys,
        'num_doc_keys': args.num_doc_keys,
        'data_size': args.data_size,
        'seed': args.seed,
        'cached': args.cached,
        'categories': args.categories,
        'platform': platform_doc,
        'fstype': get_partition(tmpdir).fstype,
    }}


@contextmanager
def run_with_profile():
    profile = Profile()
    profile.enable()
    yield profile
    profile.disable()
    with NamedTemporaryFile() as statsfile:
        profile.dump_stats(statsfile.name)
        statsfile.flush()
        statsfile.seek(0)
        profile.stats = base64.b64encode(statsfile.read()).decode()


def _random_str(size):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(size))


def _make_doc(i, num_keys=1, data_size=0):
    assert num_keys >= 1
    assert data_size >= 0

    doc = {'b_{}'.format(j): _random_str(data_size) for j in range(num_keys - 1)}
    doc['a'] = '{}{}'.format(i, _random_str(max(0, data_size - len(str(i)))))
    return doc


def _make_job(project, num_keys, num_doc_keys, data_size, data_std, i):
    size = max(0, int(random.gauss(data_size, data_std)))
    job = project.open_job(_make_doc(i, num_keys, size))
    if num_doc_keys > 0:
        size = max(0, int(random.gauss(data_size, data_std)))
        job.document.update(_make_doc(i, num_doc_keys, size))
    else:
        job.init()


def generate_random_data(project, N_sp, num_keys=1, num_doc_keys=0,
                         data_size=0, data_std=0, parallel=True):
    assert len(project) == 0

    if six.PY2:
        if parallel:
            warnings.warn("Function 'generate_random_data()' not parallelized for Python 2.")
            parallel = False

    if parallel:
        with Pool() as pool:
            p = [(project, num_keys, num_doc_keys, data_size, data_std, i) for i in range(N_sp)]
            list(pool.starmap(_make_job, tqdm(p, desc='init random project data')))
    else:
        from functools import partial
        make = partial(_make_job, project, num_keys, num_doc_keys, data_size, data_std)
        list(map(make, tqdm(range(N_sp), desc='init random project data')))


@contextmanager
def setup_random_project(N, num_keys=1, num_doc_keys=0,
                         data_size=0, data_std=0, seed=0, root=None):
    random.seed(seed)
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    with TemporaryDirectory(dir=root) as tmp:
        project = signac.init_project('benchmark-N={}'.format(N), root=tmp)
        generate_random_data(project, N, num_keys, num_doc_keys, data_size, data_std)
        yield project


class Timer(timeit.Timer):

    def timeit(self, number=10):
        return number, timeit.Timer.timeit(self, number=number)

    def repeat(self, repeat=3, number=10):
        return timeit.Timer.repeat(self, repeat=repeat, number=number)


def noop(*args, **kwargs):
    return


def benchmark_project(project, keys=None):
    root = project.root_directory()
    setup = "import signac; project = signac.get_project(root='{}'); ".format(root)
    setup += "from itertools import islice, repeat; import random; "
    setup += "from benchmark import noop; "

    data = OrderedDict()

    def run(key, timer, repeat=3, number=10):
        if keys is None or key in keys:
            logger.info("Run '{}'...".format(key))
            data[key] = timer.repeat(repeat=repeat, number=number)

    run('determine_len', Timer('len(project)', setup=setup))

    run('select_by_id', Timer(
        stmt="project.open_job(id=jobid)",
        setup=setup + "jobid = random.choice(list(islice(project, 100))).get_id()"))

    run('iterate', Timer("list(project)", setup), 3, 10)

    run('iterate_single_pass', Timer("list(project)", setup), number=1)

    run('search_lean_filter', Timer(
        stmt="len(project.find_jobs(f))",
        setup=setup +
        "sp = project.open_job(id=random.choice(list(project"
        ".find_job_ids()))).statepoint(); k, v = sp.popitem(); f = {k: v}"))

    run('search_rich_filter', Timer(
        stmt="len(project.find_jobs(f))",
        setup=setup + "f = project.open_job(id=random.choice(list"
                      "(project.find_job_ids()))).statepoint()"))

    return data


def determine_project_size(project):
    sp_size, doc_size = calc_project_metadata_size(project)
    meta = {
        'N': len(project),
        'statepoint_metadata_size': sum(sp_size),
        'document_metadata_size': sum(doc_size),
        'total': sum(sp_size) + sum(doc_size),
    }
    return meta


def main_run(args):

    def check_skip(key):
        if args.overwrite or args.output == '-':
            return False
        else:
            with signac.Collection.open(args.output) as c:
                return len(c.find(key)) >= 1

    def store_result(key, doc):
        if args.output == '-':
            if args.json:
                print(json.dumps(doc, indent=2))
            else:
                pprint(doc)
        else:
            with signac.Collection.open(args.output) as c:
                c.replace_one(key, doc, upsert=True)

    repo = git.Repo(search_parent_directories=True)

    if not args.force and args.output != '-' and repo.is_dirty():
        raise RuntimeError(
            "The git stage is dirty and results might not be reproducible. "
            "Please either do not store results (`--output='-'`) or use the "
            "the `-f/--force` option to ignore this warning.")

    default_doc = create_doc(args)
    default_doc['meta']['versions'] = {
        'python': '.'.join(map(str, sys.version_info)),
        'signac': __version__,
        'git': {
            'sha1': str(repo.head.commit),
            'dirty': repo.is_dirty(),
        }
    }

    for N in args.N:
        doc = default_doc.copy()
        doc['meta']['N'] = N
        key = doc.copy()
        key['profile'] = {'$exists': args.profile}

        if check_skip(key):
            print("Skipping...")
            continue

        with setup_random_project(N, args.num_keys, args.num_doc_keys,
                                  data_size=args.data_size, data_std=args.data_std,
                                  seed=args.seed, root=args.root) as project:
            if args.cached:
                project.update_cache()

            doc['size'] = determine_project_size(project)
            if args.profile:
                with run_with_profile() as profile:
                    doc['data'] = benchmark_project(project, args.categories)
                doc['profile'] = profile.stats
            else:
                doc['data'] = benchmark_project(project, args.categories)

        store_result(key, doc)


def strip_complexity(cat):
    if len(cat) > 1 and cat[1] == '_':
        return COMPLEXITY[cat[2:]], cat[2:]
    else:
        return COMPLEXITY.get(cat), cat


def normalize(data, N):
    for cat, x in data.items():
        cplx, cat_ = strip_complexity(cat)
        x_mean = min([(y/n) for n, y in x])
        if cplx is not None:
            x_mean /= eval(cplx)
        yield cat, 1e3 * x_mean


def tr(s):
    cplx, cat = strip_complexity(s)
    t = {
        'select_by_id': "Select by ID",
        'determine_len': "Determine N",
        'iterate': "Iterate (multiple passes)",
        'iterate_single_pass': "Iterate (single pass)",
        'search_lean_filter': "Search w/ lean filter",
        'search_rich_filter': "Search w/ rich filter",
        'datreant.core': "datreant",
        'tool,N': "Tool, N",
    }.get(cat, cat)
    if cplx is not None:
        t += ' O({})'.format(cplx)
    return t


def read_benchmark(filename, filter=None, include_metadata=False):
    with signac.Collection.open(filename) as c:
        docs = list(c.find(filter))

    df_data = pd.DataFrame(
        {doc['_id']: dict(normalize(doc['data'], doc['meta']['N'])) for doc in docs}).T

    if include_metadata:
        df_meta = pd.DataFrame(
            {doc['_id']: doc['meta'] for doc in docs}).T
        return pd.concat([df_meta, df_data], axis=1)
    else:
        return df_data


def main_report(args):
    filter = json.loads(args.filter) if args.filter else None
    df = read_benchmark(args.filename, filter, include_metadata=True)
    print("All values in ms.")
    print(df.rename(columns=tr).groupby(['tool', 'N']).mean().round(2).T)


def main_compare(args):
    repo = git.Repo(search_parent_directories=True)
    rev_this = str(repo.commit(args.rev_this))
    doc_this = read_benchmark(args.filename, {'meta.versions.git.sha1': rev_this})
    assert len(doc_this), "Can't find results for '{}'.".format(args.rev_this)
    rev_other = repo.commit(args.rev_other)
    doc_other = read_benchmark(args.filename, {'meta.versions.git.sha1': str(rev_other)})
    assert len(doc_other), "Can't find results for '{}'.".format(args.rev_other)

    print("Showing runtime {} ({}) / {} ({}):".format(
        args.rev_this, str(rev_this)[:6],
        args.rev_other, str(rev_other)[:6]))
    print()
    print(doc_this.min() / doc_other.min())
    print()

    slow_down = (doc_this.min() / doc_other.min()).mean()
    speed_up = (doc_other.min() / doc_this.min()).mean()
    if round(speed_up, 1) > 1:
        average_change = "{:0.1f}x faster than".format(speed_up)
    elif round(speed_up, 1) < 1:
        average_change = "{:0.1f}x slower than".format(slow_down)
    else:
        average_change = "{} as fast as".format('exactly' if speed_up == 1 else 'about')

    slow_down = (doc_this.min() / doc_other.min()).max()
    speed_up = (doc_other.min() / doc_this.min()).max()
    if round(slow_down, 1) > round(speed_up, 1):
        maximal_change = "{:0.1f}x slower".format(slow_down)
    elif round(speed_up, 1) > round(slow_down, 1):
        maximal_change = "{:0.1f}x faster".format(speed_up)
    else:
        maximal_change = "{} as fast".format('exactly' if speed_up == 1 else 'about')

    print("Revision '{this}' is {average_change} '{other}' on average and {maximal_change} for "
          "the category with the biggest difference.".format(
              this=args.rev_this, other=args.rev_other,
              average_change=average_change, maximal_change=maximal_change))

    if args.fail_above and slow_down > args.fail_above:
        print("FAIL: Runtime difference for the worst category ({:0.1f}) "
              "is above threshold ({})!".format(slow_down, args.fail_above))
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        "Test the runtime performance of signac for basic database operations.")
    subparsers = parser.add_subparsers()

    parser_run = subparsers.add_parser(
        name='run',
        description="Execute performance tests in various categories for "
                    "specific data space sizes (N).")
    parser_run.add_argument(
        '-o', '--output', nargs='?', default='benchmark.txt',
        help="Specify which collection file to store results to or '-' for None, "
             "default='benchmark.txt'.")
    parser_run.add_argument(
        '--json', action='store_true',
        help="Use JSON formatting if the --output argument is '-'.")
    parser_run.add_argument(
        '-N', type=int, default=[100], nargs='+',
        help="The number of data/ state points within the benchmarked project. "
             "The default size is 100. Specify more than one value to test multiple "
             "different size sequentally.")
    parser_run.add_argument(
        '-k', '--num-keys', type=int, default=10,
        help="The numnber of primary metadata keys.")
    parser_run.add_argument(
        '--num-doc-keys', type=int, default=0,
        help="The number of secondary metadata keys (if applicable).")
    parser_run.add_argument(
        '-s', '--data-size', type=int, default=100,
        help="The mean data size")
    parser_run.add_argument(
        '--data-std', type=float, default=0,
        help="The standard deviation of the data size.")
    parser_run.add_argument(
        '-r', '--seed', type=int, default=0,
        help="The random seed to use.")
    parser_run.add_argument(
        '--cached', action='store_true',
        help="Use caching option if applicable.")
    parser_run.add_argument(
        '-p', '--profile', action='store_true',
        help="Activate profiling (Results should not be used for reporting.")
    parser_run.add_argument(
        '--overwrite', action='store_true',
        help="Overwrite existing result.")
    parser_run.add_argument(
        '-n', '--dry-run', action='store_true',
        help="Perform a dry run, do not actually benchmark.")
    parser_run.add_argument(
        '--root', type=str,
        help="Specify the root directory for all temporary directories. "
             "Defaults to the system default temp directory.")
    parser_run.add_argument(
        '-c', '--categories', nargs='+',
        help="Limit benchmark to given categories.")
    parser_run.add_argument(
        '-f', '--force',
        action='store_true',
        help="Ignore warnings and store results anyways.")
    parser_run.set_defaults(func=main_run)

    parser_report = subparsers.add_parser(
        name='report',
        description="Display results from previous runs.")
    parser_report.add_argument(
        'filename', default='benchmark.txt', nargs='?',
        help="The collection that contains the benchmark data (default='benchmark.txt').")
    parser_report.add_argument(
        '-f', '--filter', type=str,
        help="Select a subset of the data.")
    parser_report.set_defaults(func=main_report)

    parser_compare = subparsers.add_parser(
        name='compare',
        description="Compare performance between two git-revisions of this repository. "
             "For example, to compare the current revision (HEAD) with the "
             "'master' branch revision, execute `{} compare master HEAD`. In this specific "
             "case one could omit both arguments, since 'master' and 'HEAD' are the two "
             "default arguments.".format(sys.argv[0]))
    parser_compare.add_argument(
        'rev_other', default='master', nargs='?',
        help="The git revision to compare against. Valid arguments are  for example "
             "a branch name, a tag, a specific commit id, or 'HEAD', defaults to 'master'.")
    parser_compare.add_argument(
        'rev_this', default='HEAD', nargs='?',
        help="The git revision that is benchmarked. Valid arguments are  for example "
             "a branch name, a tag, a specific commit id, or 'HEAD', defaults to 'HEAD'.")
    parser_compare.add_argument(
        '--filename', default='benchmark.txt', nargs='?',
        help="The collection that contains the benchmark data (default='benchmark.txt').")
    parser_compare.add_argument(
        '-f', '--fail-above',
        type=float,
        help="Exit with error code in case that the runtime ratio of "
             "the worst tested category between this and the other revision "
             "is above this value.")
    parser_compare.set_defaults(func=main_compare)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(2)
    args.func(args)
