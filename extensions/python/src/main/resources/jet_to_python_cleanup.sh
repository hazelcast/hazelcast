#!/bin/bash
# shellcheck disable=SC1090

set -e -x

source jet_to_python_env/bin/activate
source jet_to_python_params.sh

./cleanup.sh
