SCRIPT_DIR="$(dirname "$0")"
JET_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="$(which java 2>/dev/null)"
fi

if [ -z "$JAVA" ]; then
    echo "Cannot find a way to start the JVM: neither JAVA_HOME is set nor the java command is on the PATH"
    exit 1
fi

CLASSPATH="$JET_HOME/lib/*:$CLASSPATH:$CLASSPATH_DEFAULT"

JAVA_OPTS_ARRAY=(\
$JAVA_OPTS \
$JAVA_OPTS_DEFAULT \
"-Dhazelcast.logging.type=log4j" \
"-Dlog4j.configuration=file:$JET_HOME/config/log4j.properties" \
"-Djet.home=$JET_HOME" \
"-Dhazelcast.config=$JET_HOME/config/hazelcast.yaml" \
"-Dhazelcast.client.config=$JET_HOME/config/hazelcast-client.yaml" \
"-Dhazelcast.jet.config=$JET_HOME/config/hazelcast-jet.yaml" \
)

if [ "$JET_LICENSE_KEY" ]; then
  JAVA_OPTS_ARRAY+=("-Dhazelcast.enterprise.license.key=${JET_LICENSE_KEY}")
fi
