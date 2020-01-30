#!/bin/bash
# shellcheck disable=SC1090

set -e -x

python3 -m venv jet_to_python_env --system-site-packages
source jet_to_python_env/bin/activate
python3 -m pip install protobuf grpcio
[[ -f requirements.txt ]] && python3 -m pip install -r requirements.txt
source ./jet_to_python_params.sh
[[ -f init.sh ]] && ./init.sh
