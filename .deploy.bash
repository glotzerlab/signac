#!/bin/bash

set -e
set -u

# PYPI_USERNAME - (Required) Username for the publisher's account on PyPI
# PYPI_PASSWORD - (Required, Secret) Password for the publisher's account on PyPI

cat <<'EOF' >> .pypirc
[pypi]
repository=https://pypi.python.org/pypi
username=$PYPI_USERNAME
password=$PYPI_PASSWORD
EOF
python3.6 setup.py bdist_wheel
python3.6 -m twine upload dist/*
