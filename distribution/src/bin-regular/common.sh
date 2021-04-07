SCRIPT_DIR="$(dirname "$0")"
HAZELCAST_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="$(command -v java 2>/dev/null)"
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

if [ -n "${CLASSPATH}" ]; then
  export CLASSPATH="${CLASSPATH_DEFAULT}:${CLASSPATH}"
else
  export CLASSPATH="${CLASSPATH_DEFAULT}"
fi

if [ -n "${JAVA_OPTS}" ]; then
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT} ${JAVA_OPTS}"
else
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT}"
fi

CLASSPATH="$HAZELCAST_HOME/lib:$HAZELCAST_HOME/lib/*:$CLASSPATH"

function readJvmOptionsFile {
    # Read jvm.options file
    while IFS= read -r line
    do
      # Ignore lines starting with # (does not support # in the middle of the line)
      if [[ "$line" =~ ^#.*$ ]]
      then
        continue;
      fi

      JVM_OPTIONS="$JVM_OPTIONS $line"
    done < $HAZELCAST_HOME/config/$1

    # Evaluate variables in the options, allowing to use e.g. HAZELCAST_HOME variable
    JVM_OPTIONS=$(eval echo $JVM_OPTIONS)
}
