# Copyright 2021 The Regents of the University of Michigan

import random
import string
from itertools import islice
from multiprocessing import Pool
from tempfile import TemporaryDirectory

from tqdm import tqdm

import signac


def _random_str(size):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(size))


def _make_doc(i, num_keys=1, data_size=0):
    assert num_keys >= 1
    assert data_size >= 0

    doc = {f"b_{j}": _random_str(data_size) for j in range(num_keys - 1)}
    doc["a"] = f"{i}{_random_str(max(0, data_size - len(str(i))))}"
    return doc


def _make_job(project, num_keys, num_doc_keys, data_size, data_std, i):
    size = max(0, int(random.gauss(data_size, data_std)))
    job = project.open_job(_make_doc(i, num_keys, size))
    if num_doc_keys > 0:
        size = max(0, int(random.gauss(data_size, data_std)))
        job.document.update(_make_doc(i, num_doc_keys, size))
    else:
        job.init()


def generate_random_data(
    project, N_sp, num_keys=1, num_doc_keys=0, data_size=0, data_std=0, parallel=True
):
    assert len(project) == 0

    if parallel:
        with Pool() as pool:
            p = [
                (project, num_keys, num_doc_keys, data_size, data_std, i)
                for i in range(N_sp)
            ]
            list(pool.starmap(_make_job, tqdm(p, desc="init random project data")))
    else:
        from functools import partial

        make = partial(_make_job, project, num_keys, num_doc_keys, data_size, data_std)
        list(map(make, tqdm(range(N_sp), desc="init random project data")))


def setup_random_project(
    N, num_keys=1, num_doc_keys=0, data_size=0, data_std=0, seed=0, root=None
):
    random.seed(seed)
    if not isinstance(N, int):
        raise TypeError("N must be an integer!")

    temp_dir = TemporaryDirectory()
    project = signac.init_project(f"benchmark-N={N}", root=temp_dir.name)
    generate_random_data(project, N, num_keys, num_doc_keys, data_size, data_std)
    return project, temp_dir


class ProjectBench:
    def setup(self):
        self.project, self.temp_dir = setup_random_project(100)

    def teardown(self):
        self.temp_dir.cleanup()

    def time_determine_len(self):
        len(self.project)

    def time_iterate_single_pass(self):
        list(self.project)

    def time_iterate(self):
        for _ in range(10):
            list(self.project)

    def time_iterate_load_sp(self):
        for _ in range(10):
            [job.sp() for job in self.project]


class ProjectRandomJobBench:
    def setup(self):
        self.project, self.temp_dir = setup_random_project(100)
        self.random_job = random.choice(list(self.project))
        self.random_job_sp = self.random_job.statepoint()
        self.random_job_id = self.random_job.id
        self.lean_filter = {k: v for k, v in islice(self.random_job_sp.items(), 1)}

    def teardown(self):
        self.temp_dir.cleanup()

    def time_select_by_id(self):
        self.project.open_job(id=self.random_job_id)

    def time_search_lean_filter(self):
        len(self.project.find_jobs(self.lean_filter))

    def time_search_rich_filter(self):
        len(self.project.find_jobs(self.random_job_sp))
