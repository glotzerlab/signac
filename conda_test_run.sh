#!/bin/sh

source activate signac-py27
python -m unittest discover tests ${@}
source activate signac-py33
python -m unittest discover tests ${@}
source activate signac-py34
python -m unittest discover tests ${@}
source activate signac-py35
python -m unittest discover tests ${@}
