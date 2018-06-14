#!/bin/bash

set -e
set -u

python -m pip install -U twine wheel setuptools

# PYPI_USERNAME - (Required) Username for the publisher's account on PyPI
# PYPI_PASSWORD - (Required, Secret) Password for the publisher's account on PyPI

cat <<'EOF' >> ~/.pypirc
[distutils]
index-servers=
    pypi
    pypitest

[pypi]
repository:https://pypi.python.org/pypi
username:$PYPI_USERNAME
password:$PYPI_PASSWORD

[pypitest]
repository:https://test.pypi.org/legacy/
username:$PYPI_USERNAME
password:$PYPI_PASSWORD
EOF

python setup.py bdist_wheel
python -m twine upload --repository testpypi dist/*
