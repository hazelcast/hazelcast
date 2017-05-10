#!/bin/sh

SCRIPT_DIR=`dirname "$0"`
. $SCRIPT_DIR/common.sh

JAR_FILE=$1
shift

echo "########################################"
echo "# JAVA=$JAVA"
echo "# JAVA_OPTS=$JAVA_OPTS"
echo "########################################"

$JAVA $JAVA_OPTS -cp $CLASSPATH:$JAR_FILE com.hazelcast.jet.server.JetBootstrap $JAR_FILE $@
