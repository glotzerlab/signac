#!/bin/bash

declare - a envs = (
    "signac-config-host-py27"
    "signac-config-host-py34"
    "signac-config-host-py35"
    "signac-config-host-py27-passlib"
    "signac-config-host-py34-passlib"
    "signac-config-host-py35-passlib")

for env in "${envs[@]}"
do
. activate $env
. . / host_config_test.sh $@
. deactivate
done
