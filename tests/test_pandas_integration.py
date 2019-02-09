# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
from test_project import BaseProjectTest
try:
    import pandas    # noqa
    PANDAS = True
except ImportError:
    PANDAS = False


@unittest.skipIf(not PANDAS, 'test requires the pandas package')
class PandasIntegrationTest(BaseProjectTest):

    def test_to_dataframe(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.to_dataframe()
        self.assertEqual(len(df), len(self.project))
        self.assertIn('sp.a', df.columns)
        self.assertIn('doc.b', df.columns)

    def test_jobs_iterator_to_dataframe(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.b = float(i)
        df = self.project.find_jobs().to_dataframe()
        self.assertEqual(len(df), len(self.project))
        self.assertIn('sp.a', df.columns)
        self.assertIn('doc.b', df.columns)
        jobs = self.project.find_jobs(dict({'a.$lte': 5}))
        df = jobs.to_dataframe()
        self.assertEqual(len(df), len(jobs))
        self.assertIn('sp.a', df.columns)
        self.assertIn('doc.b', df.columns)


if __name__ == '__main__':
    unittest.main()
