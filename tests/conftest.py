from contextlib import contextmanager

import pytest
from packaging import version

import signac


@contextmanager
def deprecated_in_version(version_string):
    if version.parse(version_string) <= version.parse(signac.__version__):
        with pytest.deprecated_call():
            yield
    else:
        yield
