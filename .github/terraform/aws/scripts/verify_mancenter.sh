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

set -ex

EXPECTED_SIZE=$1

verify_hazelcast_cluster_size() {
    EXPECTED_SIZE=$1
    for i in `seq 1 6`; do
        local MEMBER_COUNT=$(grep -cE " Started communication with (a new )?member" ~/logs/mancenter.stdout.log)

        if [ "$MEMBER_COUNT" == "$EXPECTED_SIZE" ] ; then
            echo "Hazelcast cluster size equal to ${EXPECTED_SIZE}"
            return 0
        else
            echo "Hazelcast cluster size NOT equal to ${EXPECTED_SIZE}!. Waiting.."
            sleep 10
        fi
    done
    return 1
}

echo "Verifying the Hazelcast cluster connected to Management Center"
verify_hazelcast_cluster_size $EXPECTED_SIZE