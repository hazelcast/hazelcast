#!/bin/sh

PRG="$0"
PRGDIR=$(dirname "$PRG")
HAZELCAST_HOME=$(cd "$PRGDIR/.." >/dev/null; pwd)

if [ "$JAVA_HOME" ]
then
	echo "JAVA_HOME found at $JAVA_HOME"
	RUN_JAVA="$JAVA_HOME/bin/java"
else
	echo "JAVA_HOME environment variable not available."
    RUN_JAVA=$(command -v java 2>/dev/null)
fi

if [ -z "$RUN_JAVA" ]
then
    echo "JAVA could not be found in your system."
    echo "Please install Java 1.8 or higher!!!"
    exit 1
fi

	echo "Path to Java : $RUN_JAVA"

#### you can enable following variables by uncommenting them

#### minimum heap size
# MIN_HEAP_SIZE=1G

#### maximum heap size
# MAX_HEAP_SIZE=1G


if [ "x$MIN_HEAP_SIZE" != "x" ]; then
	JAVA_OPTS="$JAVA_OPTS -Xms${MIN_HEAP_SIZE}"
fi

if [ "x$MAX_HEAP_SIZE" != "x" ]; then
	JAVA_OPTS="$JAVA_OPTS -Xmx${MAX_HEAP_SIZE}"
fi

export CLASSPATH="$HAZELCAST_HOME/lib/hazelcast-all-${project.version}.jar:$HAZELCAST_HOME/user-lib:$HAZELCAST_HOME/user-lib/*"

echo "########################################"
echo "# RUN_JAVA=$RUN_JAVA"
echo "# JAVA_OPTS=$JAVA_OPTS"
echo "# starting now...."
echo "########################################"

"$RUN_JAVA" -server $JAVA_OPTS com.hazelcast.core.server.HazelcastMemberStarter
