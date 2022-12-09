if [ -z "$SCRIPT_DIR" ]; then
  echo "Variable SCRIPT_DIR is expected to be set by the calling script";
  exit 1;
fi;

export HAZELCAST_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="$(command -v java 2>/dev/null)"
fi

if [ -z "$JAVA" ]; then
    echo "Cannot find a way to start the JVM: neither JAVA_HOME is set nor the java command is on the PATH"
    exit 1
fi

# Bash on Windows may produce paths such as /c/path/to/lib and Java wants c:\path\to\lib
# and, in this case, the cygpath command *should* be available - and then we will use it
CYGPATH=""
if [ "$(command -v cygpath)" != "" ]; then
  CYGPATH=cygpath
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

# 1 -> Java 8 or earlier (1.8..)
# 9, 10, 11 -> JDK9, JDK10, JDK11 etc.
JAVA_VERSION=$(${JAVA} -version 2>&1 | sed -En 's/.* version "([0-9]+).*$/\1/p')
if [ "$JAVA_VERSION" -ge "9" ]; then
    JDK_OPTS="\
        --add-modules java.se \
        --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
        --add-opens java.management/sun.management=ALL-UNNAMED \
        --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
    "

    VM_NAME=$(${JAVA} -XshowSettings:properties -version 2>&1 | grep java.vm.name | cut -d "=" -f2)
    if [[ "$VM_NAME" =~ "OpenJ9" ]]; then
        JDK_OPTS="$JDK_OPTS --add-exports jdk.management/com.ibm.lang.management.internal=ALL-UNNAMED"
    fi
fi

# ensure CLASSPATH_DEFAULT is unix style + trimmed
if [ -n "${CLASSPATH_DEFAULT}" ]; then
  if [ -n "${CYGPATH}" ]; then
    CLASSPATH_DEFAULT=$(cygpath -u -p "$CLASSPATH_DEFAULT")
  fi
  CLASSPATH_DEFAULT="${CLASSPATH_DEFAULT##:}"
  CLASSPATH_DEFAULT="${CLASSPATH_DEFAULT%%:}"
fi

# ensure CLASSPATH is unix style + trimmed
if [ -n "${CLASSPATH}" ]; then
  if [ -n "${CYGPATH}" ]; then
    CLASSPATH=$(cygpath -u -p "$CLASSPATH")
  fi
  CLASSPATH="${CLASSPATH##:}"
  CLASSPATH="${CLASSPATH%%:}"
fi

# combine CLASSPATH and CLASSPATH_DEFAULT
if [ -n "${CLASSPATH}" ]; then
  if [ -n "${CLASSPATH_DEFAULT}" ]; then
    export CLASSPATH="${CLASSPATH_DEFAULT}:${CLASSPATH}"
  fi
else
  export CLASSPATH="${CLASSPATH_DEFAULT}"
fi

if [ -n "${JAVA_OPTS}" ]; then
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT} ${JAVA_OPTS}"
else
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT}"
fi

CLASSPATH="$CLASSPATH:$HAZELCAST_HOME/lib:$HAZELCAST_HOME/lib/*:$HAZELCAST_HOME/bin/user-lib:$HAZELCAST_HOME/bin/user-lib/*"

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

# trim CLASSPATH
CLASSPATH="${CLASSPATH##:}"
CLASSPATH="${CLASSPATH%%:}"

# ensure CLASSPATH is windows style on Windows
if [ -n "${CYGPATH}" ]; then
  CLASSPATH=$(cygpath -w -p "$CLASSPATH")
fi
