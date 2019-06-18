#!/bin/bash
SCRIPT_DIR="$(dirname "$0")"
JET_HOME="$(cd "$SCRIPT_DIR/.."; pwd)"
PID_FILE=$JET_HOME/bin/jet_instance.pid

if [ $JAVA_HOME ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    echo "JAVA_HOME environment variable not available."
    JAVA="$(which java 2>/dev/null)"
fi

if [ -z "$JAVA" ]; then
    echo "Java could not be found in your system."
    exit 1
fi

#### you can enable following variables by uncommenting them
#### minimum heap size
# MIN_HEAP_SIZE=1G

#### maximum heap size
# MAX_HEAP_SIZE=1G

#convert existing opts to an array, separated by space
JAVA_OPTS=($JAVA_OPTS)

if [ "x$MIN_HEAP_SIZE" != "x" ]; then
    JAVA_OPTS+=("-Xms${MIN_HEAP_SIZE}")
fi

if [ "x$MAX_HEAP_SIZE" != "x" ]; then
    JAVA_OPTS+=("-Xmx${MAX_HEAP_SIZE}")
fi

#### add classpath and java opts entries that come from docker image if defined
if [ "x$CLASSPATH_DEFAULT" != "x" ]; then
    CLASSPATH="${CLASSPATH_DEFAULT}:${CLASSPATH}"
fi

if [ "x$JAVA_OPTS_DEFAULT" != "x" ]; then
    JAVA_OPTS+=("${JAVA_OPTS_DEFAULT}")
fi

if [ "x$JET_LICENSE_KEY" != "x" ]; then
    JAVA_OPTS+=("-Dhazelcast.enterprise.license.key=${JET_LICENSE_KEY}")
fi

if [ "x$JET_LICENCE_KEY" != "x" ]; then
    JAVA_OPTS+=("-Dhazelcast.enterprise.license.key=${JET_LICENCE_KEY}")
fi

CLASSPATH="$JET_HOME/lib/${hazelcast.jet.artifact}-${project.version}.jar:$CLASSPATH"
JAVA_OPTS+=("-Dhazelcast.config=$JET_HOME/config/hazelcast.xml" \
"-Djet.home=$JET_HOME" \
"-Dhazelcast.client.config=$JET_HOME/config/hazelcast-client.xml" \
"-Dhazelcast.jet.config=$JET_HOME/config/hazelcast-jet.xml")
