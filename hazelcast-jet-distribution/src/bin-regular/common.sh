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

# 1 -> Java 8 or earlier (1.8..)
# 9, 10, 11 -> JDK9, JDK10, JDK11 etc.
JAVA_VERSION=$(${JAVA} -version 2>&1 | sed -En 's/.* version "([0-9]+).*$/\1/p')
if [ "$JAVA_VERSION" -ge "9" ]; then
    JDK_OPTS="\
        --add-modules java.se \
        --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        --add-opens java.base/java.nio=ALL-UNNAMED \
        --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
        --add-opens java.management/sun.management=ALL-UNNAMED \
        --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
    "
fi

JAVA_OPTS_ARRAY=(\
$JDK_OPTS \
$JAVA_OPTS_DEFAULT \
"-Dhazelcast.logging.type=log4j" \
"-Dlog4j.configuration=file:$JET_HOME/config/log4j.properties" \
"-Djet.home=$JET_HOME" \
"-Dhazelcast.config=$JET_HOME/config/hazelcast.yaml" \
"-Dhazelcast.client.config=$JET_HOME/config/hazelcast-client.yaml" \
"-Dhazelcast.jet.config=$JET_HOME/config/hazelcast-jet.yaml" \
$JAVA_OPTS \
)


if [ "$JET_LICENSE_KEY" ]; then
  JAVA_OPTS_ARRAY+=("-Dhazelcast.enterprise.license.key=${JET_LICENSE_KEY}")
fi

CLASSPATH="$JET_HOME/lib/*:$CLASSPATH:$CLASSPATH_DEFAULT"
