#!/bin/sh

PIDS=`ps ax | grep com.hazelcast.jet.server.StartServer | grep -v grep | awk '{print $1}'`

if [ -z "$PIDS" ]; then
  echo "No Hazelcast Jet server found to stop"
  exit 1
else
  kill -s TERM $PIDS
fi