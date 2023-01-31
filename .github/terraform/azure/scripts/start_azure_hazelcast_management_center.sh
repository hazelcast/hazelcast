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

set -x

MANCENTER_VERSION=$1

mkdir -p ${HOME}/lib
mkdir -p ${HOME}/logs
mkdir -p ${HOME}/man

LOG_DIR=${HOME}/logs
MAN_CENTER_HOME=${HOME}/man

MANCENTER_JAR_URL=https://repository.hazelcast.com/download/management-center/hazelcast-management-center-${MANCENTER_VERSION}.zip

pushd ${HOME}/lib
    echo "Downloading JAR..."
    if wget -q "$MANCENTER_JAR_URL"; then
        echo "Hazelcast Management JAR downloaded successfully."
    else
        echo "Hazelcast Management JAR could NOT be downloaded!"
        exit 1;
    fi
    unzip hazelcast-management-center-${MANCENTER_VERSION}.zip
    cp -R hazelcast-management-center-*/* ./
popd
 
lib/bin/hz-mc conf cluster add --client-config=${HOME}/hazelcast-client.yaml \
                       >> $LOG_DIR/mancenter.conf.stdout.log 2>> $LOG_DIR/mancenter.conf.stderr.log

nohup lib/bin/hz-mc start >> $LOG_DIR/mancenter.stdout.log 2>> $LOG_DIR/mancenter.stderr.log &

sleep 5
