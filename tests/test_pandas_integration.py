# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from test_project import TestBaseProject
try:
    import pandas    # noqa
    PANDAS = True
except ImportError:
    PANDAS = False


@pytest.mark.skipif(not PANDAS,reason= 'test requires the pandas package')
class TestPandasIntegration(TestBaseProject):

    def test_to_dataframe(self,setUp):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.to_dataframe()
        assert len(df) == len(self.project)
        assert 'sp.a' in df.columns
        assert 'doc.b' in df.columns

    def test_jobs_iterator_to_dataframe(self,setUp):
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

