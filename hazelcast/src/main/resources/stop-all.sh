#!/bin/sh

#### This script kills all the instances started with start.sh script.

PIDS=$(pgrep -f HazelcastMemberStarter)

if [ -z "$PIDS" ]; then
  echo "No Hazelcast IMDG member found to stop"
  exit 1
else
  kill -s TERM $PIDS
  echo "Stopped Hazelcast instances with the following PIDs:"
  echo "$PIDS"
fi
