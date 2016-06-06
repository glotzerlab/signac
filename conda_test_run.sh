#!/bin/bash

# Use the tests/conda_create_test_env.sh script to
# create the conda test environments.

sys_path_check() {
  n=$(python -c "import sys; print(':'.join(sys.path).count('site-packages/signac'))")
  if [ "$n" != "0" ]; then
    echo "Possible path misconfiguration!"
    python -c 'import sys; print(sys.path)'
  fi
  }

declare -a envs=(
  "signac-py27"
  "signac-py33"
  "signac-py34"
  "signac-py35"
  "signac-py27-minimal"
  "signac-py33-minimal"
  "signac-py34-minimal"
  "signac-py35-minimal")

for env in "${envs[@]}"; do
  source activate $env
  sys_path_check
  python -W once::DeprecationWarning -m unittest discover tests ${@}
done

source deactivate
