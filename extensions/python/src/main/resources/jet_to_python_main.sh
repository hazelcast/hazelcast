#!/bin/bash
# shellcheck disable=SC1090

set -e -x

source jet_to_python_env/bin/activate
source jet_to_python_params.sh

exec python3 jet_to_python_grpc_server.py "$1" "$JET_TO_PYTHON_HANDLER_MODULE" "$JET_TO_PYTHON_HANDLER_FUNCTION"
