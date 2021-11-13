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

import random
import string
from itertools import islice
from multiprocessing import Pool
from tempfile import TemporaryDirectory

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
    random.seed(seed)
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    temp_dir = TemporaryDirectory()
    project = signac.init_project(f"benchmark-N={N}", root=temp_dir.name)
    generate_random_data(
        project, N, num_keys, num_doc_keys, data_size_mean, data_size_std
    )
    return project, temp_dir


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

    def setup(self, *params):
        N, num_keys, num_doc_keys, data_size_mean, data_size_std = params
        self.project, self.temp_dir = setup_random_project(
            N,
            num_keys=num_keys,
            num_doc_keys=num_doc_keys,
            data_size_mean=data_size_mean,
            data_size_std=data_size_std,
        )

    def teardown(self, *params):
        self.temp_dir.cleanup()


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
