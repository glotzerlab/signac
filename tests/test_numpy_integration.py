# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest
from test_project import BaseProjectTest
try:
    import numpy    # noqa
    import numpy.testing
    NUMPY = True
except ImportError:
    NUMPY = False


@unittest.skipIf(not NUMPY, 'test requires the numpy package')
class NumpyIntegrationTest(BaseProjectTest):

    def test_store_number_in_sp_and_doc(self):
        for i in range(10):
            a = numpy.float32(i) if i % 2 else numpy.float64(i)
            b = numpy.float64(i) if i % 2 else numpy.float32(i)
            job = self.project.open_job(dict(a=a))
            job.doc.b = b
            numpy.testing.assert_equal(job.doc.b, b)
        for i, job in enumerate(sorted(self.project, key=lambda job: job.sp.a)):
            self.assertEqual(job.sp.a, i)
            self.assertEqual(job.doc.b, i)

    def test_store_array_in_sp(self):
        for i in range(10):
            self.project.open_job(dict(a=numpy.array([i]))).init()
        for i, job in enumerate(sorted(self.project, key=lambda job: job.sp.a)):
            self.assertEqual([i], job.sp.a)
            self.assertEqual(numpy.array([i]), job.sp.a)

    def test_store_array_in_doc(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            job.doc.array = numpy.ones(3) * i
            numpy.testing.assert_equal(job.doc.array, numpy.ones(3) * i)
        for i, job in enumerate(sorted(self.project, key=lambda job: job.sp.a)):
            self.assertEqual(i, job.sp.a)
            self.assertTrue(((numpy.array([i, i, i]) == job.doc.array).all()))
            self.assertEqual([i] * 3, job.doc.array)


if __name__ == '__main__':
    unittest.main()
