#!/bin/bash
# Run host configuration tests.

SIGNAC="signac $@"
HOSTNAME=testcfg

set -e

CURRENT_URI=`signac config show hosts.$HOSTNAME.url`
CURRENT_USERNAME=`signac config show hosts.$HOSTNAME.username`
CURRENT_PW=`signac config host $HOSTNAME --show-pw`

python ./connection_test.py
$SIGNAC --yes config host $HOSTNAME -r
$SIGNAC --yes config host $HOSTNAME $CURRENT_URI
$SIGNAC --yes config host $HOSTNAME -r
$SIGNAC --yes config host $HOSTNAME $CURRENT_URI -u $CURRENT_USERNAME
$SIGNAC --yes config host $HOSTNAME -r
$SIGNAC --yes config host $HOSTNAME $CURRENT_URI -u $CURRENT_USERNAME -p $CURRENT_PW
python ./connection_test.py
$SIGNAC --yes config host $HOSTNAME --update-pw None -p $CURRENT_PW
python ./connection_test.py
$SIGNAC --yes config host $HOSTNAME -r
$SIGNAC --yes config host $HOSTNAME $CURRENT_URI -u $CURRENT_USERNAME -p $CURRENT_PW
python ./connection_test.py
