#!/bin/bash

set -e
set -u

# PYPI_USERNAME - (Required) Username for the publisher's account on PyPI
# PYPI_PASSWORD - (Required, Secret) Password for the publisher's account on PyPI

cat << EOF >> ~/.pypirc
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
python -m twine upload --skip-existing --repository testpypi dist/*
