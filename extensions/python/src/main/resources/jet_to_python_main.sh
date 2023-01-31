#!/bin/bash
#
# Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# shellcheck disable=SC1090

set -e -x

source jet_to_python_env/bin/activate
source jet_to_python_params.sh

exec python3 jet_to_python_grpc_server.py "$1" "$JET_TO_PYTHON_HANDLER_MODULE" "$JET_TO_PYTHON_HANDLER_FUNCTION"
