#!/bin/bash

set -x

REGION=$1
TAG_KEY=$2
TAG_VALUE=$3


sed -i -e "s/REGION/${REGION}/g" ${HOME}/hazelcast.yaml
sed -i -e "s/TAG_KEY/${TAG_KEY}/g" ${HOME}/hazelcast.yaml
sed -i -e "s/TAG_VALUE/${TAG_VALUE}/g" ${HOME}/hazelcast.yaml

CLASSPATH="${HOME}/jars/hazelcast.jar:${HOME}/jars/hazelcast-aws.jar:${HOME}/hazelcast.yaml"

nohup java -cp ${CLASSPATH} -server com.hazelcast.core.server.HazelcastMemberStarter >> ${HOME}/logs/hazelcast.stderr.log 2>> ${HOME}/logs/hazelcast.stdout.log &

sleep 5
