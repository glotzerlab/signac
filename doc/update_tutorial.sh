#!/bin/bash
set -e
set -u

if [ "$(basename $(pwd))" != "doc" ]
  then
    echo "Execute this script within the doc folder!"
    exit 1
fi
PWD=$(pwd)
export PYTHONPATH=$PWD/..

function cleanup {
  cd $PWD
  #rm -rf signac-examples
}
trap cleanup EXIT

git clone git@github.com:csadorf/signac-examples.git
cd signac-examples/
scons signac
cd ../
find signac-examples/notebooks/static/ -regex '.*\/signac_[a-zA-Z0-9\_]+\.ipynb' -exec cp {} ./ \;
