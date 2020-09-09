#!/bin/bash

set -x

LABEL_KEY=$1
LABEL_VALUE=$2

sed -i -e "s/LABEL_KEY/${LABEL_KEY}/g" ${HOME}/hazelcast.yaml
sed -i -e "s/LABEL_VALUE/${LABEL_VALUE}/g" ${HOME}/hazelcast.yaml

CLASSPATH="${HOME}/jars/hazelcast.jar:${HOME}/jars/hazelcast-gcp.jar:${HOME}/hazelcast.yaml"

nohup java -cp ${CLASSPATH} -server com.hazelcast.core.server.HazelcastMemberStarter &>> ${HOME}/logs/hazelcast.logs &

sleep 5
