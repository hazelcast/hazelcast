#!/bin/bash
# shellcheck disable=SC1090

set -e -x

python3 -m venv jet_to_python_env --system-site-packages
source jet_to_python_env/bin/activate

flock --exclusive ~/.jet-pip.lock python3 -m pip install --upgrade pip
# grpcio versions do not always align with Java releases, so ensure versions are broadly (rather than exactly) in sync
# https://github.com/hazelcast/hazelcast/pull/26097#issuecomment-1840378216
flock --exclusive ~/.jet-pip.lock python3 -m pip install 'protobuf==@python.protobuf.version@' 'grpcio~=@grpc.version@'

[[ -f requirements.txt ]] && python3 -m pip install -r requirements.txt
source ./jet_to_python_params.sh
if [[ -f init.sh ]]; then
    echo "Executing init.sh"
   ./init.sh
fi
