#!/bin/bash

set -ex

EXPECTED_SIZE=$1

verify_hazelcast_cluster_size() {
    EXPECTED_SIZE=$1
    for i in `seq 1 30`; do
        local MEMBER_COUNT=$(grep -cE " Started communication with (a new )?member" ~/logs/mancenter.stdout.log)

        if [ "$MEMBER_COUNT" == "$EXPECTED_SIZE" ] ; then
            echo "Hazelcast cluster size equal to ${EXPECTED_SIZE}"
            return 0
        else
            echo "Hazelcast cluster size NOT equal to ${EXPECTED_SIZE}!. Waiting.."
            sleep 10
        fi
    done
    return 1
}

echo "Verifying the Hazelcast cluster connected to Management Center"
verify_hazelcast_cluster_size $EXPECTED_SIZE
