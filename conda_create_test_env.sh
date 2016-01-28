#!/bin/sh

conda env remove --yes -n signac-py27
conda env remove --yes -n signac-py33
conda env remove --yes -n signac-py34
conda env remove --yes -n signac-py35

echo "Creating environment for python 2.7..."
conda create --yes -n signac-py27 python=2.7 pymongo networkx mpi4py
. activate signac-py27
python setup.py develop
. deactivate
echo "Done."

conda create --yes -n signac-py33 python=3.3 mpi4py
. activate signac-py33
pip install pymongo
pip install networkx
python setup.py develop
. deactivate

conda create --yes -n signac-py34 python=3.4 pymongo networkx mpi4py
. activate signac-py34
python setup.py develop
. deactivate

conda create --yes -n signac-py35 python=3.5 pymongo networkx mpi4py
. activate signac-py35
python setup.py develop
. deactivate
