#!/usr/bin/env bash

# return nonzero exit status of rightmost command, so that we
# get nonzero exit on test failure without halting subunit-trace
set -o pipefail


TESTRARGS=$1

python setup.py testr --slowest --testr-args="--subunit $TESTRARGS" | subunit-trace -f

