#!/usr/bin/env bash

set -e

compdb init testing --template=example
compdb check
python job.py
compdb snapshot test.tar
compdb restore test.tar
compdb snapshot test.tar.gz
compdb restore test.tar.gz
compdb --yes clear
python job.py
compdb --yes remove -j all
python job.py
compdb --yes remove --project
