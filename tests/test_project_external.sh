#!/usr/bin/env bash

set -e

signac init testing --template=example
signac check
python job.py
signac snapshot test.tar
signac restore test.tar
signac snapshot test.tar.gz
signac restore test.tar.gz
signac --yes clear
python job.py
signac --yes remove -j all
python job.py
signac view
rm -r view
signac view -w
rm -r view
signac view -s
signac view --flat
signac --yes remove --project
