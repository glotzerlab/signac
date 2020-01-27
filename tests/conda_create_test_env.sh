#!/bin/bash

conda env remove - -yes - n signac - py27
conda env remove - -yes - n signac - py33
conda env remove - -yes - n signac - py34
conda env remove - -yes - n signac - py35
conda env remove - -yes - n signac - py27 - minimal
conda env remove - -yes - n signac - py33 - minimal
conda env remove - -yes - n signac - py34 - minimal
conda env remove - -yes - n signac - py35 - minimal

echo "Creating environment for python 2.7."
conda create - -yes - n signac - py27 python = 2.7 pymongo mpi4py
if ["$?" != "0"]
then
conda create - -yes - n signac - py27 python = 2.7
fi
. activate signac - py27
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment for python 3.3."
conda create - -yes - n signac - py33 python = 3.3 mpi4py
if ["$?" != "0"]
then
conda create - -yes - n signac - py33 python = 3.3
fi
. activate signac - py33
pip install pymongo
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment for python 3.4."
conda create - -yes - n signac - py34 python = 3.4 pymongo mpi4py
if ["$?" != "0"]
then
conda create - -yes - n signac - py34 python = 3.4
fi
. activate signac - py34
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment for python 3.5."
conda create - -yes - n signac - py35 python = 3.5 pymongo mpi4py
if ["$?" != "0"]
then
conda create - -yes - n signac - py35 python = 3.5
fi
. activate signac - py35
python setup.py develop
. deactivate
echo "Done."

echo "Creating minimal environment for python 2.7."
conda create - -yes - n signac - py27 - minimal python = 2.7
. activate signac - py27 - minimal
python setup.py develop
. deactivate
echo "Done."

echo "Creating minimal environment for python 3.3."
conda create - -yes - n signac - py33 - minimal python = 3.3
. activate signac - py33 - minimal
python setup.py develop
. deactivate
echo "Done."

echo "Creating minimal environment for python 3.4."
conda create - -yes - n signac - py34 - minimal python = 3.4
. activate signac - py34 - minimal
python setup.py develop
. deactivate
echo "Done."

echo "Creating minimal environment for python 3.5."
conda create - -yes - n signac - py35 - minimal python = 3.5
. activate signac - py35 - minimal
python setup.py develop
. deactivate
echo "Done."
