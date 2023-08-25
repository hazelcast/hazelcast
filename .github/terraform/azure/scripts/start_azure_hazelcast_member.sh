#!/bin/bash

set -x

CLASSPATH="${HOME}/jars/hazelcast.jar:${HOME}/hazelcast.yaml"

nohup java -cp ${CLASSPATH} -server com.hazelcast.core.server.HazelcastMemberStarter >> ${HOME}/logs/hazelcast.stderr.log 2>> ${HOME}/logs/hazelcast.stdout.log &

sleep 5
