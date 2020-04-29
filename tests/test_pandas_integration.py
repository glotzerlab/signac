# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from test_project import TestProjectBase
try:
    import pandas    # noqa
    PANDAS = True
except ImportError:
    PANDAS = False


@pytest.mark.skipif(not PANDAS, reason='test requires the pandas package')
class TestPandasIntegration(TestProjectBase):

    def test_to_dataframe(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.to_dataframe()
        assert len(df) == len(self.project)
        assert 'sp.a' in df.columns
        assert 'doc.b' in df.columns

    def test_jobs_iterator_to_dataframe(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.find_jobs().to_dataframe()
        assert len(df) == len(self.project)
        assert 'sp.a' in df.columns
        assert 'doc.b' in df.columns
        jobs = self.project.find_jobs(dict({'a.$lte': 5}))
        df = jobs.to_dataframe()
        assert len(df) == len(jobs)
        assert 'sp.a' in df.columns
        assert 'doc.b' in df.columns

    def test_prefixes(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.to_dataframe(sp_prefix='', doc_prefix='')
        assert len(df) == len(self.project)
        assert 'a' in df.columns
        assert 'b' in df.columns

    def test_includes_excludes(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i, b=i*2))
            job.doc.c = float(i)
            job.doc.d = float(i*3)

        # Including no keys should return an empty DataFrame
        df = self.project.to_dataframe(sp_includes=[], doc_includes=[])
        assert len(df) == 0

        # Excluding all keys should return an empty DataFrame
        df = self.project.to_dataframe(
            sp_excludes=['a', 'b'], doc_excludes=['c', 'd'])
        assert len(df) == 0

        # Include one state point column
        df = self.project.to_dataframe(sp_includes=['a'])
        assert 'sp.a' in df.columns
        assert 'sp.b' not in df.columns
        assert 'doc.c' in df.columns
        assert 'doc.d' in df.columns
        assert len(df) == len(self.project)

        # Exclude one state point column
        df = self.project.to_dataframe(sp_excludes=['b'])
        assert 'sp.a' in df.columns
        assert 'sp.b' not in df.columns
        assert 'doc.c' in df.columns
        assert 'doc.d' in df.columns
        assert len(df) == len(self.project)

        # Include one document column
        df = self.project.to_dataframe(doc_includes=['c'])
        assert 'sp.a' in df.columns
        assert 'sp.b' in df.columns
        assert 'doc.c' in df.columns
        assert 'doc.d' not in df.columns
        assert len(df) == len(self.project)

        # Exclude one document column
        df = self.project.to_dataframe(doc_excludes=['d'])
        assert 'sp.a' in df.columns
        assert 'sp.b' in df.columns
        assert 'doc.c' in df.columns
        assert 'doc.d' not in df.columns
        assert len(df) == len(self.project)
