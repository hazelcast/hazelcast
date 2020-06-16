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

IFS=',' read -ra MODULES <<< "$JET_MODULES"
for module in "${MODULES[@]}"; do
    # ${project.version} interpolated during build by maven assembly plugin
    if [ -z "$CLASSPATH" ]; then
        CLASSPATH="$JET_HOME/opt/hazelcast-jet-${module}-${project.version}.jar"
    else
        CLASSPATH="$JET_HOME/opt/hazelcast-jet-${module}-${project.version}.jar:$CLASSPATH"
    fi
done

CLASSPATH="$JET_HOME/lib:$JET_HOME/lib/*:$CLASSPATH"
