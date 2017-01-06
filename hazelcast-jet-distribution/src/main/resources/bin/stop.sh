#!/bin/sh
PRG="$0"
PRGDIR=`dirname "$PRG"`
JET_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`
PID_FILE="${JET_HOME}"/bin/jet_instance.pid

if [ ! -f "${PID_FILE}" ]; then
    echo "No Hazelcast Jet instance is running."
    exit 0
fi

PID=$(cat "${PID_FILE}");
if [ -z "${PID}" ]; then
    echo "No Hazelcast Jet instance is running."
    exit 0
else
   kill -15 "${PID}"
   rm "${PID_FILE}"
   echo "Hazelcast Jet Instance with PID ${PID} shutdown."
   exit 0
fi
