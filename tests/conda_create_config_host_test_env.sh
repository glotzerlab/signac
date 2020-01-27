#!/bin/bash

conda env remove --yes -n signac-config-host-py27
conda env remove --yes -n signac-config-host-py34
conda env remove --yes -n signac-config-host-py35
conda env remove --yes -n signac-config-host-py27-passlib
conda env remove --yes -n signac-config-host-py34-passlib
conda env remove --yes -n signac-config-host-py35-passlib

echo "Creating environment signac-config-host-py27"
conda create --yes -n signac-config-host-py27 python=2.7 pymongo
. activate signac-config-host-py27
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment signac-config-host-py34."
conda create --yes -n signac-config-host-py34 python=3.4 pymongo
. activate signac-config-host-py34
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment signac-config-host-py35."
conda create --yes -n signac-config-host-py35 python=3.5 pymongo
. activate signac-config-host-py35
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment ${signac-config-host-py27-passlib}."
conda create --yes -n signac-config-host-py27-passlib python=2.7 pymongo passlib bcrypt
. activate signac-config-host-py27-passlib
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment signac-config-host-py34-passlib."
conda create --yes -n signac-config-host-py34-passlib python=3.4 pymongo passlib bcrypt
. activate signac-config-host-py34-passlib
python setup.py develop
. deactivate
echo "Done."

echo "Creating environment signac-config-host-py35-passlib."
conda create --yes -n signac-config-host-py35-passlib python=3.5 pymongo passlib bcrypt
. activate signac-config-host-py35-passlib
python setup.py develop
. deactivate
echo "Done."
