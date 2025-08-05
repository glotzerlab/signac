import pytest

from test_project import TestProject
from itertools import product

class TestNeighborList(TestProject):
    def test_neighbors(self):
        a_vals = [1, 2]
        b_vals = [3, 4, 5]
        for a,b in product(a_vals, b_vals):
            self.project.open_job({"a": a, "b": b}).init()

        neighbor_list = self.project.get_neighbors()

        for a,b in product(a_vals, b_vals):
            job = self.project.open_job({"a": a, "b": b})
            neighbors_job = job.get_neighbors()
            
            neighbors_project = neighbor_list[job.id]
            assert neighbors_project == neighbors_job

            this_neighbors = neighbors_project
            
            # for this_neighbors in [neighbors_project, neighbors_job]:
            # a neighbors
            if a == 1:
                assert this_neighbors["a"][2] == self.project.open_job({"a": 2, "b": b}).id
            elif a == 2:
                assert this_neighbors["a"][1] == self.project.open_job({"a": 1, "b": b}).id

            # b neighbors
            if b == 3:
                assert this_neighbors["b"][4] == self.project.open_job({"a": a, "b": 4}).id
            elif b == 4:
                assert this_neighbors["b"][3] == self.project.open_job({"a": a, "b": 3}).id
                assert this_neighbors["b"][5] == self.project.open_job({"a": a, "b": 5}).id
            elif b == 5:
                assert this_neighbors["b"][4] == self.project.open_job({"a": a, "b": 4}).id

    def test_neighbors_ignore(self):
        b_vals = [3, 4, 5]
        for b in b_vals:
            self.project.open_job({"b": b, "2b": 2 * b}).init()

        neighbor_list = self.project.get_neighbors(ignore = "2b")

        for b in b_vals:
            job = self.project.open_job({"b": b, "2b": 2 * b})
            this_neighbors = neighbor_list[job.id]

            if b == 3:
                assert this_neighbors["b"][4] == self.project.open_job({"b": 4, "2b": 8}).id
            elif b == 4:
                assert this_neighbors["b"][3] == self.project.open_job({"b": 3, "2b": 6}).id
                assert this_neighbors["b"][5] == self.project.open_job({"b": 5, "2b": 10}).id
            elif b == 5:
                assert this_neighbors["b"][4] == self.project.open_job({"b": 4, "2b": 8}).id

    def test_neighbors_nested(self):
        a_vals = [{"c": 2}, {"c": 3}, {"c": 4}]
        for a in a_vals:
            self.project.open_job({"a": a}).init()

        neighbor_list = self.project.get_neighbors()

        for a in a_vals:
            job = self.project.open_job({"a": a})
            this_neighbors = neighbor_list[job.id]
            # note how the inconsistency in neighborlist access syntax comes from schema
            if a == 2:
                assert this_neighbors["a.c"][3] == self.project.open_job({"a": {"c": 3}}).id
            elif a == 3:
                assert this_neighbors["a.c"][2] == self.project.open_job({"a": {"c": 2}}).id
                assert this_neighbors["a.c"][4] == self.project.open_job({"a": {"c": 4}}).id
            elif a == 4:
                assert this_neighbors["a.c"][3] == self.project.open_job({"a": {"c": 3}}).id

    def test_neighbors_varied_types(self):
        # in sort order
        # NoneType is first because it's capitalized
        a_vals = [None, False, True, 1.2, 1.3, 2, "1", "2", "x", "y", (3,4), (5,6)]

        job_ids = []
        for a in a_vals:
            job = self.project.open_job({"a": a}).init()
            job_ids.append(job.id)

        neighbor_list = self.project.get_neighbors()

        for i,a in enumerate(a_vals):
            jobid = job_ids[i]
            if i > 0:
                prev_val = a_vals[i-1]
                assert neighbor_list[jobid]["a"][prev_val] == job_ids[i-1]
            if i < len(a_vals) - 1:
                next_val = a_vals[i+1]
                assert neighbor_list[jobid]["a"][next_val] == job_ids[i+1]

    def test_neighbors_no(self):
        self.project.open_job({"a": 1}).init()
        self.project.open_job({"b": 1}).init()
        neighbor_list = self.project.get_neighbors()

        for job in self.project:
            for v in neighbor_list[job.id].values():
                assert len(v) == 0

    def test_neighbors_ignore_dups(self):
        a_vals = [1,2]
        b_vals = [3,4,5]
        for a,b in product(a_vals, b_vals):
            self.project.open_job({"a": a, "b": b}).init()
        with pytest.raises(ValueError):
            self.project.get_neighbors(ignore = "a")
        with pytest.raises(ValueError):
            self.project.get_neighbors(ignore = "b")

    
    

