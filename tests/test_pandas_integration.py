# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from test_project import TestProjectBase

try:
    import pandas  # noqa

    PANDAS = True
except ImportError:
    PANDAS = False


@pytest.mark.skipif(not PANDAS, reason="test requires the pandas package")
class TestPandasIntegration(TestProjectBase):
    def test_to_dataframe(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.to_dataframe()
        assert len(df) == len(self.project)
        assert "sp.a" in df.columns
        assert "doc.b" in df.columns

    def test_jobs_iterator_to_dataframe(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.find_jobs().to_dataframe()
        assert len(df) == len(self.project)
        assert "sp.a" in df.columns
        assert "doc.b" in df.columns
        jobs = self.project.find_jobs(dict({"a.$lte": 5}))
        df = jobs.to_dataframe()
        assert len(df) == len(jobs)
        assert "sp.a" in df.columns
        assert "doc.b" in df.columns

    def test_prefixes(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.to_dataframe(sp_prefix="", doc_prefix="")
        assert len(df) == len(self.project)
        assert "a" in df.columns
        assert "b" in df.columns

    def test_usecols(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i, b=i * 2))
            job.doc.c = float(i)
            job.doc.d = float(i * 3)

        # Including no keys should return an empty DataFrame
        df = self.project.to_dataframe(usecols=[])
        assert len(df.columns) == 0
        assert len(df) == 0

        # Excluding all keys should return an empty DataFrame
        def usecols(column):
            return column not in ["sp.a", "sp.b", "doc.c", "doc.d"]

        df = self.project.to_dataframe(usecols=usecols)
        assert len(df.columns) == 0
        assert len(df) == 0

        # Include one state point column
        df = self.project.to_dataframe(usecols=["sp.a"])
        assert "sp.a" in df.columns
        assert len(df.columns) == 1
        assert len(df) == len(self.project)

        # Exclude one state point column
        def usecols(column):
            return column != "sp.b"

        df = self.project.to_dataframe(usecols=usecols)
        assert "sp.a" in df.columns
        assert "sp.b" not in df.columns
        assert "doc.c" in df.columns
        assert "doc.d" in df.columns
        assert len(df.columns) == 3
        assert len(df) == len(self.project)

        # Include one document column
        df = self.project.to_dataframe(usecols=["doc.c"])
        assert "doc.c" in df.columns
        assert len(df.columns) == 1
        assert len(df) == len(self.project)

        # Exclude one document column
        def usecols(column):
            return column != "doc.d"

        df = self.project.to_dataframe(usecols=usecols)
        assert "sp.a" in df.columns
        assert "sp.b" in df.columns
        assert "doc.c" in df.columns
        assert "doc.d" not in df.columns
        assert len(df.columns) == 3
        assert len(df) == len(self.project)

    def test_flatten(self):
        for i in range(10):
            job = self.project.open_job(dict(a=dict(b=i * 2, c=i * 3), d=i))
            job.doc.e = dict(f=float(i))

        # Including no keys should return an empty DataFrame
        df = self.project.to_dataframe(usecols=[], flatten=True)
        assert len(df.columns) == 0
        assert len(df) == 0

        # Include one flattened state point column
        df = self.project.to_dataframe(usecols=["sp.a.b"], flatten=True)
        assert "sp.a.b" in df.columns
        assert len(df.columns) == 1
        assert len(df) == len(self.project)

        # Include one flattened document column
        df = self.project.to_dataframe(usecols=["doc.e.f"], flatten=True)
        assert "doc.e.f" in df.columns
        assert len(df.columns) == 1
        assert len(df) == len(self.project)
