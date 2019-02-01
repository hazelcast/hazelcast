#!/bin/sh

SCRIPT_DIR=`dirname "$0"`
. $SCRIPT_DIR/common.sh

$JAVA $JAVA_OPTS -cp $CLASSPATH com.hazelcast.jet.server.JetCommandLine "$@"
