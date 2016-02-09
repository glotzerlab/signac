#!/bin/bash

sys_path_check() {
  n=$(python -c "import sys; print(':'.join(sys.path).count('site-packages/signac'))")
  if [ "$n" != "0" ]; then
    echo "Possible path misconfiguration!"
    python -c 'import sys; print(sys.path)'
  fi
  }

source activate signac-py27
sys_path_check
python -W once -m unittest discover tests ${@}

source activate signac-py33
sys_path_check
python -W once -m unittest discover tests ${@}

source activate signac-py34
sys_path_check
python -W once -m unittest discover tests ${@}

source activate signac-py35
sys_path_check
python -W once -m unittest discover tests ${@}
