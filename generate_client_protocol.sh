#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"
JET_HOME="$(cd "$SCRIPT_DIR/"; pwd)"

PYTHON="$(which python3 2>/dev/null)"
PIP3="$(which pip3 2>/dev/null)"
GIT="$(which git 2>/dev/null)"

PROTOCOL_DIRECTORY=`mktemp -d 2>/dev/null || mktemp -d -t 'protocol'`

if [ -z "$PYTHON" ]; then
    echo "Python 3 could not be found in your system."
    exit 1
fi

if [ -z "$PIP3" ]; then
    echo "PIP 3 could not be found in your system."
    exit 1
fi

if [ -z "$GIT" ]; then
    echo "Git could not be found in your system."
    exit 1
fi

echo $SCRIPT_DIR
echo $JET_HOME
echo $PROTOCOL_DIRECTORY

$GIT clone --depth=1 https://github.com/hazelcast/hazelcast-client-protocol.git $PROTOCOL_DIRECTORY

cd $PROTOCOL_DIRECTORY

$PIP3 install -r requirements.txt

$PYTHON generator.py -r $JET_HOME -p $JET_HOME/hazelcast-jet-core/src/main/resources/client-protocol-definition \
-o hazelcast-jet-core/src/main/java/com/hazelcast/jet/impl/client/protocol/codec \
-n com.hazelcast.jet.impl.client.protocol.codec --no-binary --no-id-check

rm -rf $PROTOCOL_DIRECTORY