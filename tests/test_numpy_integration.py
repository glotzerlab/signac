# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
from synced_collections.numpy_utils import NumpyConversionWarning
from test_project import TestProjectBase

try:
    import numpy  # noqa
    import numpy.testing

    NUMPY = True
except ImportError:
    NUMPY = False


@pytest.mark.skipif(not NUMPY, reason="test requires the numpy package")
class TestNumpyIntegration(TestProjectBase):
    def test_store_number_in_sp_and_doc(self):
        for i in range(10):
            a = numpy.float32(i) if i % 2 else numpy.float64(i)
            b = numpy.float64(i) if i % 2 else numpy.float32(i)
            with pytest.warns(NumpyConversionWarning):
                job = self.project.open_job(dict(a=a))
                job.statepoint
            with pytest.warns(NumpyConversionWarning):
                job.doc.b = b
            numpy.testing.assert_equal(job.doc.b, b)
        for i, job in enumerate(sorted(self.project, key=lambda job: job.sp.a)):
            assert job.sp.a == i
            assert job.doc.b == i

    def test_store_array_in_sp(self):
        for i in range(10):
            with pytest.warns(NumpyConversionWarning):
                self.project.open_job(dict(a=numpy.array([i]))).init()
        for i, job in enumerate(sorted(self.project, key=lambda job: job.sp.a)):
            assert [i] == job.sp.a
            assert numpy.array([i]) == job.sp.a

    def test_store_array_in_doc(self):
        for i in range(10):
            job = self.project.open_job(dict(a=i))
            with pytest.warns(NumpyConversionWarning):
                job.doc.array = numpy.ones(3) * i
            numpy.testing.assert_equal(job.doc.array, numpy.ones(3) * i)
        for i, job in enumerate(sorted(self.project, key=lambda job: job.sp.a)):
            assert i == job.sp.a
            assert (numpy.array([i, i, i]) == job.doc.array).all()
            assert [i] * 3 == job.doc.array

    def test_store_zero_dim_array_in_sp(self):
        # Zero-dimensional arrays have size 1, and their tolist() method
        # returns a single value.
        value = 1.0
        with pytest.warns(NumpyConversionWarning):
            job = self.project.open_job(dict(a=numpy.array(value))).init()
        assert value == job.sp.a
        assert numpy.array(value) == job.sp.a

    def test_store_zero_dim_array_in_doc(self):
        # Zero-dimensional arrays have size 1, and their tolist() method
        # returns a single value.
        value = 1.0
        job = self.project.open_job(dict(a=1)).init()
        with pytest.warns(NumpyConversionWarning):
            job.doc.array = numpy.array(value)
        numpy.testing.assert_equal(job.doc.array, numpy.array(value))
        assert value == job.doc.array
        assert numpy.array(value) == job.doc.array
