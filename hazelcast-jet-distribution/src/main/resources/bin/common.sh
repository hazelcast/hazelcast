SCRIPT_DIR=`dirname "$0"`
JET_HOME=`cd "$SCRIPT_DIR/.." >/dev/null; pwd`
PID_FILE=$JET_HOME/bin/jet_instance.pid

if [ $JAVA_HOME ]; then
	JAVA=$JAVA_HOME/bin/java
else
	echo "JAVA_HOME environment variable not available."
    JAVA=`which java 2>/dev/null`
fi

if [ -z $JAVA ]; then
    echo "Java could not be found in your system."
    exit 1
fi

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

CLASSPATH="$JET_HOME/lib/hazelcast-jet-${project.version}.jar:$CLASSPATH"
JAVA_OPTS="$JAVA_OPTS -Dhazelcast.config=$JET_HOME/config/hazelcast.xml \
-Dhazelcast.client.config=$JET_HOME/config/hazelcast-client.xml \
-Dhazelcast.jet.config=$JET_HOME/config/hazelcast-jet.xml"