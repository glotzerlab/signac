import uuid

import pytest


@pytest.fixture
def testdata():
    return str(uuid.uuid4())
