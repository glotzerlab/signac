#!/usr/bin/env bash

set -e

function run_tests {
  echo "-----------------------------------------------------------"
  echo "Run tests for environment '${1}'."
  source ~/.virtualenvs/$1/bin/activate
  python setup.py develop
  pip install nose coverage sqlitedict
  nosetests --with-coverage --cover-package=signac  ${@:2}
  ret_code=${?}
  echo "return code: ${ret_code}"
  return ${ret_code}
  echo "-----------------------------------------------------------"
}

run_tests signac-dev-pymongo-2.8 ${@}
run_tests signac-dev-pymongo-3.0.0 ${@}
run_tests signac-dev-pymongo-3.0.1 ${@}
run_tests signac-dev-pymongo-3.0.2 ${@}
run_tests signac-dev-pymongo-3.0.3 ${@}
exit $?
