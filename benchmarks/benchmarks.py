# Copyright 2021 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Benchmarks for use with asv (airspeed velocity).

This script defines benchmarks of common signac operations, used to assess the
performance of the framework over time. The asv tools allow for profiling,
comparison, and visualization of benchmark results. This complements the file
``benchmark.py`` in the root directory of the repository, which is primarily
intended for CI tests.
"""

import os
import random
import string
import tempfile
from itertools import islice
from multiprocessing import Pool

from tqdm import tqdm

import signac


def _random_str(size):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(size))


def _make_json_data(i, num_keys=1, data_size=0):
    assert num_keys >= 1
    assert data_size >= 0

    data = {f"b_{j}": _random_str(data_size) for j in range(num_keys - 1)}
    data["a"] = f"{i}{_random_str(max(0, data_size - len(str(i))))}"
    return data


def _make_job(project, num_keys, num_doc_keys, data_size, data_std, i):
    size = max(0, int(random.gauss(data_size, data_std)))
    job = project.open_job(_make_json_data(i, num_keys, size))
    if num_doc_keys > 0:
        size = max(0, int(random.gauss(data_size, data_std)))
        job.document.update(_make_json_data(i, num_doc_keys, size))
    else:
        job.init()


def generate_random_data(
    project,
    N,
    num_keys=1,
    num_doc_keys=0,
    data_size_mean=0,
    data_size_std=0,
    parallel=True,
):
    assert len(project) == 0

    if parallel:
        with Pool() as pool:
            p = [
                (project, num_keys, num_doc_keys, data_size_mean, data_size_std, i)
                for i in range(N)
            ]
            list(pool.starmap(_make_job, tqdm(p, desc="init random project data")))
    else:
        from functools import partial

        make = partial(
            _make_job, project, num_keys, num_doc_keys, data_size_mean, data_size_std
        )
        list(map(make, tqdm(range(N), desc="init random project data")))


def setup_random_project(
    N, num_keys=1, num_doc_keys=0, data_size_mean=0, data_size_std=0, seed=0, root=None
):
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    # Compute a reproducible hash for this set of parameters, used to avoid
    # recreating random projects for every run. We cannot use asv's setup_cache
    # method because it is not parameterized. Instead, we create a temporary
    # directory of random project data that persists across benchmarks for a
    # given set of parameters.
    project_hash = signac.contrib.hashing.calc_id(
        {
            "N": N,
            "num_keys": num_keys,
            "num_doc_keys": num_doc_keys,
            "data_size_mean": data_size_mean,
            "data_size_std": data_size_std,
            "seed": seed,
        }
    )
    name = f"benchmark_N_{N}_{project_hash}"
    root = os.path.join(tempfile.gettempdir(), "signac_benchmarks", name)

    if os.path.isdir(root):
        project = signac.get_project(root=root)
        assert len(project) == N
    else:
        project = signac.init_project(name, root=root)
        random.seed(seed)
        generate_random_data(
            project, N, num_keys, num_doc_keys, data_size_mean, data_size_std
        )
    return project


PARAMETERS = {
    "N": [100, 1_000],
    "num_statepoint_keys": [10],
    "num_document_keys": [0],
    "data_size_mean": [100],
    "data_size_std": [0],
}


class _ProjectBenchBase:
    param_names = PARAMETERS.keys()
    params = PARAMETERS.values()
    rounds = 1
    repeat = (3, 3, 0)
    sample_time = 0.1
    min_run_count = 3

    def setup(self, *params):
        N, num_keys, num_doc_keys, data_size_mean, data_size_std = params
        self.project = setup_random_project(
            N,
            num_keys=num_keys,
            num_doc_keys=num_doc_keys,
            data_size_mean=data_size_mean,
            data_size_std=data_size_std,
        )


class ProjectBench(_ProjectBenchBase):
    def time_determine_len(self, *params):
        len(self.project)

    def time_iterate_single_pass(self, *params):
        list(self.project)

    def time_iterate(self, *params):
        for _ in range(10):
            list(self.project)

    def time_iterate_load_sp(self, *params):
        for _ in range(10):
            [job.sp() for job in self.project]


class ProjectRandomJobBench(_ProjectBenchBase):
    def setup(self, *params):
        super().setup(*params)
        self.random_job = random.choice(list(self.project))
        self.random_job_sp = self.random_job.statepoint()
        self.random_job_id = self.random_job.id
        self.lean_filter = {k: v for k, v in islice(self.random_job_sp.items(), 1)}

    def time_select_by_id(self, *params):
        self.project.open_job(id=self.random_job_id)

    def time_search_lean_filter(self, *params):
        len(self.project.find_jobs(self.lean_filter))

    def time_search_rich_filter(self, *params):
        len(self.project.find_jobs(self.random_job_sp))
