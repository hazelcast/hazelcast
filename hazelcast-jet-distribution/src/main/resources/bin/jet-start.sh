#!/bin/bash

SCRIPT_DIR=`dirname "$0"`
. $SCRIPT_DIR/common.sh

echo "########################################"
echo "# JAVA=$JAVA"
echo "# JAVA_OPTS=$JAVA_OPTS"
echo "# CLASSPATH=$CLASSPATH"
echo "# starting now...."
echo "########################################"

$JAVA -server "${JAVA_OPTS[@]}" -cp "$CLASSPATH" com.hazelcast.jet.server.StartServer
