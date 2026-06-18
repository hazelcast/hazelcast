#!/bin/bash
# shellcheck disable=SC1090

set -e -x

PYTHON_RUNTIME_COMPATIBILITY_CHECK_FAILED_EXIT_CODE=86

python3 -m venv jet_to_python_env --system-site-packages
source jet_to_python_env/bin/activate

flock --exclusive ~/.jet-pip.lock python3 -m pip install --upgrade pip
flock --exclusive ~/.jet-pip.lock python3 -m pip install "protobuf==@python.protobuf.version@; python_version < '3.14'" "protobuf==@python.protobuf.version.3.14@; python_version >= '3.14'" grpcio==@grpc.version@

[[ -f requirements.txt ]] && python3 -m pip install -r requirements.txt

if ! python3 - <<'PY'
import importlib.metadata
import sys

try:
    import grpc
    import google.protobuf
    import jet_to_python_pb2
    import jet_to_python_pb2_grpc
except Exception:
    print("Python runtime compatibility check failed")
    print("Python executable:", sys.executable)
    print("Python version:", sys.version)
    print("protobuf version:", importlib.metadata.version("protobuf"))
    print("grpcio version:", importlib.metadata.version("grpcio"))
    raise
PY
then
    exit $PYTHON_RUNTIME_COMPATIBILITY_CHECK_FAILED_EXIT_CODE
fi

source ./jet_to_python_params.sh
if [[ -f init.sh ]]; then
    echo "Executing init.sh"
   ./init.sh
fi
