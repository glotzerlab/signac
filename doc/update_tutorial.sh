#!/bin/bash
set -e
set -u

if [ "$(basename $(pwd))" != "doc" ]
  then
    echo "Execute this script within the doc folder!"
    exit 1
fi
PWD=$(pwd)

function cleanup {
  cd $PWD
  rm -rf signac-examples
}
trap cleanup EXIT

git clone --reference ~/local/gitcaches/signac-examples.reference git@bitbucket.org:glotzer/signac-examples.git
cd signac-examples/
PYTHONPATH=.. scons signac
cd ../
find signac-examples/notebooks/static -regex '.*/signac\_[a-z0-9_]*\.ipynb' -exec cp {} ./ \;
