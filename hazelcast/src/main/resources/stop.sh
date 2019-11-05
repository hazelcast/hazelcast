#!/bin/sh

#### This script kills all the instances started with start.sh script.

PIDS=$(ps ax | grep com.hazelcast.core.server.HazelcastMemberStarter | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No Hazelcast IMDG member found to stop"
  exit 1
else
  kill -s TERM $PIDS
fi
