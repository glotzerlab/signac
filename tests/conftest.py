import uuid
from contextlib import contextmanager

import pytest
from packaging import version

import signac


@pytest.fixture
def testdata():
    return str(uuid.uuid4())
