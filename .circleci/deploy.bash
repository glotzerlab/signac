#!/bin/bash

set -e
set -u

python -m pip install --user -U twine wheel setuptools

# PYPI_USERNAME - (Required) Username for the publisher's account on PyPI
# PYPI_PASSWORD - (Required, Secret) Password for the publisher's account on PyPI

cat << EOF > ~/.pypirc
[distutils]
index-servers=
    pypi
    testpypi

[pypi]
username: ${PYPI_USERNAME}
password: ${PYPI_PASSWORD}

[testpypi]
repository: https://test.pypi.org/legacy/
username: ${PYPI_USERNAME}
password: ${PYPI_PASSWORD}
EOF

python setup.py bdist_wheel
if [[ "$1" == "testpypi" || "$1" == "pypi" ]]; then
    python -m twine upload --skip-existing --repository $1 dist/*
else
    echo "A valid repository must be provided: pypi or testpypi."
    exit 1
fi
