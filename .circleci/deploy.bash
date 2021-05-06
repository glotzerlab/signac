#!/bin/bash

set -e
set -u

python -m pip install --progress-bar off --user -U -r requirements/requirements-test.txt
python -m pip install --progress-bar off --user -U -r requirements/requirements-test-optional.txt
python -m pip install --progress-bar off --user -U twine wheel setuptools

# PYPI_API_TOKEN - (Required, Secret) Token for the publisher's account on PyPI
# TEST_PYPI_API_TOKEN - (Required, Secret) Token for the publisher's account on TestPyPI

cat << EOF > ~/.pypirc
[distutils]
index-servers=
    pypi
    testpypi

[pypi]
username: __token__
password: ${PYPI_API_TOKEN}

[testpypi]
repository: https://test.pypi.org/legacy/
username: __token__
password: ${TEST_PYPI_API_TOKEN}
EOF

# Create wheels and source distribution
python setup.py bdist_wheel
python setup.py sdist

# Test generated wheel
python -m pip install signac --progress-bar off -U --force-reinstall -f dist/
python -m pytest tests/ -v

# Upload wheels
if [[ "$1" == "testpypi" || "$1" == "pypi" ]]; then
    python -m twine upload --skip-existing --repository $1 dist/*
else
    echo "A valid repository must be provided: pypi or testpypi."
    exit 1
fi
