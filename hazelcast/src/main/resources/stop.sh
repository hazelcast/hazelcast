#!/bin/sh
PRG="$0"
PRGDIR=`dirname "$PRG"`
HAZELCAST_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`
PID_FILE="${HAZELCAST_HOME}"/bin/hazelcast_instance.pid

if [ ! -f "${PID_FILE}" ]; then
    echo "No hazelcast instance is running."
    exit 0
fi

PID=$(cat "${PID_FILE}");
if [ -z "${PID}" ]; then
    echo "No hazelcast instance is running."
    exit 0
else
   kill -15 "${PID}"
   rm "${PID_FILE}"
   echo "Hazelcast Instance with PID ${PID} shutdown."
   exit 0
fi
