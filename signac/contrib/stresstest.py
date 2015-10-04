import random
import ctypes
from six import with_metaclass
from abc import ABCMeta, abstractmethod
from threading import Timer

"""Increase robustness of jobs by adding stress tests."""

class BaseRandomFailure(with_metaclass(ABCMeta, object)):
    """
    This object will produce a random error
    to test robustness.
    """

    def __init__(self, p = None):
        if p is None:
            p = 0.1
        self._p = p

    def __str__(self):
        return str(type(self))

    @abstractmethod
    def _fail(self):
        return False

    @abstractmethod
    def test(self):
        return False

class RandomError(BaseRandomFailure):
    
    def _fail(self):
        msg = "{} failed, probability = {}."
        raise RuntimeError(msg.format(self, self._p))

class RandomSegfault(BaseRandomFailure):
    
    def _fail(self):
        print("RandomSegfault: Crashing!")
        ctypes.string_at(1)

class RandomFailureHere(BaseRandomFailure):
    
    def test(self):
        if random.random() <= self._p:
            self._fail()

    def __enter__(self):
        self.test()
        return self

    def __exit__(self, err_t, err_v, tb):
        return False

class RandomFailureFuture(RandomFailureHere):

    def __init__(self, p = None, interval = None):
        super(RandomFailureFuture, self).__init__(p = p)
        if interval is None:
            interval = 10
        self._interval = interval
        self._timer = None

    def __enter__(self):
        def test_periodic():
            self.test()
            self._timer = Timer(self._interval, test_periodic)
            self._timer.start()
        test_periodic()
        return self

    def __exit__(self, err_t, err_v, tb):
        self._timer.cancel()
        self._timer = None
        return False

class RandomErrorHere(RandomError, RandomFailureHere):
    pass

class RandomErrorFuture(RandomError, RandomFailureFuture):
    pass

class RandomSegfaultHere(RandomSegfault, RandomFailureHere):
    pass

class RandomSegfaultFuture(RandomSegfault, RandomFailureFuture):
    pass
