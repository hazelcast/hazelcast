#!/bin/bash
# shellcheck disable=SC1090

set -e -x

python3 -m venv jet_to_python_env --system-site-packages
source jet_to_python_env/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install protobuf==@protobuf.version@ grpcio==@grpc.version@
[[ -f requirements.txt ]] && python3 -m pip install -r requirements.txt
source ./jet_to_python_params.sh
if [[ -f init.sh ]]; then
    echo "Executing init.sh"
   ./init.sh
fi
